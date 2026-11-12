//! Runner behavior against real provider processes: the `node-test-agent.mjs`
//! fixture speaking ACP over stdio. Skips when `node` is not on PATH.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;

use super::*;
use crate::executor::storage::Storage;

mod forge;
mod shared;

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
        one_shot: None,
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
        git_socket: None,
        pull_staging: storage.pull_staging_dir(),
        forge_git: None,
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
        checkout: None,
        mcp_proxies: Vec::new(),
        compact_tool_descriptions: false,
        checkout_of: None,
        forge: None,
        forge_fetch: None,
        git_identity: None,
        new_machine: None,
        resumed_from_disk: false,
        harness_version: None,
        labels: Box::default(),
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
    move |r| r.agent_id().is_some_and(|a| a.0 == id) && matches!(r.body, JournalBody::Exited { .. })
}

fn turn_ended<'a>(id: &'a str, turn: &'a str) -> impl Fn(&JournalRecord) -> bool + 'a {
    move |r| {
        r.agent_id().is_some_and(|a| a.0 == id)
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
        one_shot: None,
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
        git_socket: None,
        pull_staging: storage.pull_staging_dir(),
        forge_git: None,
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

/// A turn waiting for a provider that never starts, and a turn sent after
/// it exited, say why it exited instead of only "the agent has exited".
#[tokio::test]
async fn node_runner_turn_refusal_carries_the_exit_reason() {
    let Some(h) = harness(1, &["--auth-required-setup"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "waiting turn end", turn_ended("a1", "t1")).await;
    let all = if all.iter().any(exited("a1")) {
        all
    } else {
        wait_for(&h.runner, "exit", exited("a1")).await
    };
    let exit = exit_error(find(&all, exited("a1"))).unwrap().to_owned();
    // A provider without a Default configuration keeps the plain start code.
    assert!(exit.starts_with(codes::START), "{exit}");
    h.runner
        .prompt(&AgentId::from("a1"), "t2".into(), text("late"))
        .await;
    let all = wait_for(&h.runner, "late turn end", turn_ended("a1", "t2")).await;
    for turn in ["t1", "t2"] {
        let (stop_reason, error) = turn_end(find(&all, turn_ended("a1", turn)));
        let error = error.unwrap();
        assert_eq!(stop_reason, "refusal");
        assert!(error.starts_with(codes::NOT_RUNNING), "{turn}: {error}");
        assert!(
            error.contains("the provider did not start"),
            "{turn}: {error}"
        );
        assert!(error.contains(&exit), "{turn}: {error}");
    }
}

/// A provider that exits with a status says so in the late turn's refusal.
#[tokio::test]
async fn node_runner_turn_refusal_names_the_exit_status() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("exit"))
        .await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    assert_eq!(exit_error(find(&all, exited("a1"))), None);
    h.runner
        .prompt(&AgentId::from("a1"), "t2".into(), text("late"))
        .await;
    let all = wait_for(&h.runner, "late turn end", turn_ended("a1", "t2")).await;
    let (_, error) = turn_end(find(&all, turn_ended("a1", "t2")));
    assert_eq!(
        error,
        Some(format!("{}: the provider exited with status 0", codes::NOT_RUNNING).as_str())
    );
}

fn starts(log: &std::path::Path) -> usize {
    std::fs::read_to_string(log).map_or(0, |log| log.lines().count())
}

/// A provider that exits before it is ready is started once more, and the
/// agent then runs as if its first start had worked.
#[tokio::test]
async fn node_runner_starts_a_provider_once_more_when_its_first_start_fails() {
    let dir = tempfile::tempdir().unwrap();
    let (marker, log) = (dir.path().join("failed-once"), dir.path().join("starts"));
    let Some(h) = harness(
        1,
        &[
            "--exit-on-first-start",
            marker.to_str().unwrap(),
            "--log-start",
            log.to_str().unwrap(),
        ],
    ) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
    let (stop_reason, error) = turn_end(find(&all, turn_ended("a1", "t1")));
    assert_eq!((stop_reason, error), ("end_turn", None));
    assert_eq!(all.iter().filter(|r| started("a1")(r)).count(), 1);
    assert!(!all.iter().any(exited("a1")), "{all:#?}");
    assert_eq!(starts(&log), 2);
}

/// A provider that times out on both starts exits once, saying why, and a
/// turn sent after it is told the same reason.
#[tokio::test]
async fn node_runner_gives_up_after_the_second_start_and_says_why() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("starts");
    let Some(h) = harness(
        1,
        &[
            "--slow-initialize",
            "15000",
            "--log-start",
            log.to_str().unwrap(),
        ],
    ) else {
        return;
    };
    // Long enough for a loaded machine to boot node and log each start.
    h.runner
        .set_initialize_timeouts(Duration::from_secs(3), Duration::from_secs(60));
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    let exit = exit_error(find(&all, exited("a1"))).unwrap().to_owned();
    assert_eq!(
        exit,
        format!("{}: request `initialize` timed out", codes::START)
    );
    assert_eq!(all.iter().filter(|r| exited("a1")(r)).count(), 1);
    assert_eq!(starts(&log), 2);
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("late"))
        .await;
    let all = wait_for(&h.runner, "late turn end", turn_ended("a1", "t1")).await;
    let (_, error) = turn_end(find(&all, turn_ended("a1", "t1")));
    let error = error.unwrap();
    assert!(error.starts_with(codes::NOT_RUNNING), "{error}");
    assert!(error.contains(&exit), "{error}");
}

/// The second start after a bind gets the ordinary timeout, not the cold
/// one the first start used.
#[tokio::test]
async fn node_runner_retries_a_cold_start_with_the_ordinary_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let (marker, log) = (dir.path().join("failed-once"), dir.path().join("starts"));
    let Some(h) = harness(
        1,
        &[
            "--exit-on-first-start",
            marker.to_str().unwrap(),
            "--slow-initialize",
            "15000",
            "--log-start",
            log.to_str().unwrap(),
        ],
    ) else {
        return;
    };
    h.runner
        .set_initialize_timeouts(Duration::from_secs(3), Duration::from_secs(60));
    h.runner.adopt_head(Some("hs-1"), &[]);
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    let exit = exit_error(find(&all, exited("a1"))).unwrap();
    assert!(exit.contains("request `initialize` timed out"), "{exit}");
    assert_eq!(starts(&log), 2);
}

/// A sign-in failure at start is not retried: a second start would fail
/// the same way.
#[tokio::test]
async fn node_runner_does_not_retry_a_sign_in_failure() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("starts");
    let Some(h) = harness(
        1,
        &[
            "--auth-required-setup",
            "--log-start",
            log.to_str().unwrap(),
        ],
    ) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    let exit = exit_error(find(&all, exited("a1"))).unwrap();
    assert!(exit.starts_with(codes::START), "{exit}");
    assert_eq!(starts(&log), 1);
}

/// Only the first agent started after a link binds gets the longer cold
/// `initialize` timeout.
#[tokio::test]
async fn node_runner_allows_a_slow_start_only_for_the_first_launch_after_a_bind() {
    let Some(h) = harness(2, &["--slow-initialize", "1000"]) else {
        return;
    };
    h.runner
        .set_initialize_timeouts(Duration::from_millis(300), Duration::from_secs(20));
    h.runner.adopt_head(Some("hs-1"), &[]);
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "cold start", started("a1")).await;
    h.runner.launch(launch("a2")).await;
    let all = wait_for(&h.runner, "warm start timeout", exited("a2")).await;
    let exit = exit_error(find(&all, exited("a2"))).unwrap();
    assert!(exit.contains("request `initialize` timed out"), "{exit}");
    assert!(!all.iter().any(exited("a1")), "{all:#?}");
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
    move |r| {
        r.agent_id().is_some_and(|a| a.0 == id) && matches!(r.body, JournalBody::Started { .. })
    }
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

/// A saved wip head would not pin fails the launch with
/// `node-checkout-failed` before the agent can run and overwrite it.
#[tokio::test]
async fn node_runner_fails_the_launch_when_an_unapplied_wip_cannot_be_pinned() {
    let Some(h) = harness(1, &[]) else { return };
    h.checkouts
        .fail_pin
        .store(true, std::sync::atomic::Ordering::SeqCst);
    h.runner.launch(launch("a1")).await;
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    let error = exit_error(find(&all, exited("a1"))).unwrap();
    assert!(error.starts_with(codes::CHECKOUT), "{error}");
    assert!(error.contains("pin the unapplied wip"), "{error}");
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
            text(r#"wsapi return await host({ method: "secret.provide", args: {} });"#),
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
        let exit = exit_error(find(&all, exited("a1"))).expect("exit error");
        assert!(
            !exit.starts_with(codes::SIGNED_OUT),
            "{args:?}: only a Default launch of codex or claude-code is signed out: {exit}"
        );
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

/// Review of intentd#2910: a credential equal to an ACP enum word must not
/// break the typed parse on head. Enum values at their schema positions (an
/// annotation's `audience` roles, a config option's `type`) are kept as sent;
/// every free-form string around them is still masked.
#[test]
fn the_redactor_keeps_enum_schema_positions_so_the_update_still_parses() {
    let parses = |params: &serde_json::Value| {
        serde_json::from_value::<intent_acp::session::SessionUpdate>(params["update"].clone())
            .is_ok()
    };
    for secret in ["user", "assistant", "select"] {
        let profile = ProfileEnv::new(
            "codex@work",
            "f1",
            vec![("CODEX_ACCESS_TOKEN".into(), Some(secret.into()))],
        );
        let redactor = Redactor::new(Some(&profile));
        let annotations = json!({
            "audience": ["user", "assistant"],
            "priority": 0.5,
            "lastModified": format!("by {secret}"),
        });

        let mut chunk = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {
                    "type": "text",
                    "text": format!("the token is {secret}"),
                    "annotations": annotations.clone(),
                },
            },
        });
        redactor.notification(&mut chunk);
        let content = &chunk["update"]["content"];
        assert_eq!(
            content["annotations"]["audience"],
            json!(["user", "assistant"]),
            "{secret}"
        );
        assert_eq!(
            content["annotations"]["lastModified"], "by [redacted]",
            "{secret}"
        );
        assert_eq!(
            content["text"], "the token is [redacted]",
            "{secret}: a secret in free text is masked"
        );
        assert!(parses(&chunk), "{secret}: unparseable {chunk}");

        let mut tool = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": "call-1",
                "title": format!("read {secret}"),
                "kind": "read",
                "status": "completed",
                "content": [{
                    "type": "content",
                    "content": {
                        "type": "resource_link",
                        "uri": format!("file:///{secret}.md"),
                        "name": format!("{secret}.md"),
                        "annotations": annotations.clone(),
                    },
                }],
            },
        });
        redactor.notification(&mut tool);
        let link = &tool["update"]["content"][0]["content"];
        assert_eq!(
            link["annotations"]["audience"],
            json!(["user", "assistant"]),
            "{secret}"
        );
        assert_eq!(link["name"], "[redacted].md", "{secret}");
        assert_eq!(tool["update"]["title"], "read [redacted]", "{secret}");
        assert!(parses(&tool), "{secret}: unparseable {tool}");

        let mut config = json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "config_option_update",
                "configOptions": [{
                    "id": "model",
                    "name": format!("Model for {secret}"),
                    "type": "select",
                    "currentValue": "fast",
                    "options": [{ "value": "fast", "name": format!("Fast {secret}") }],
                }],
            },
        });
        redactor.notification(&mut config);
        let option = &config["update"]["configOptions"][0];
        assert_eq!(option["type"], "select", "{secret}");
        assert_eq!(option["name"], "Model for [redacted]", "{secret}");
        assert_eq!(option["options"][0]["name"], "Fast [redacted]", "{secret}");
        assert!(parses(&config), "{secret}: unparseable {config}");
    }
}

/// Only an enum's own words pass unmasked at those positions: a secret a
/// provider places there is masked like any other string.
#[test]
fn the_redactor_masks_a_secret_placed_in_an_enum_position() {
    let secret = "zq7w-not-a-role";
    let profile = ProfileEnv::new(
        "codex@work",
        "f1",
        vec![("CODEX_ACCESS_TOKEN".into(), Some(secret.into()))],
    );
    let redactor = Redactor::new(Some(&profile));
    let mut chunk = json!({
        "sessionId": "s1",
        "update": {
            "sessionUpdate": "agent_message_chunk",
            "content": {
                "type": "text",
                "text": "hi",
                "annotations": { "audience": ["user", secret] },
            },
        },
    });
    redactor.notification(&mut chunk);
    assert_eq!(
        chunk["update"]["content"]["annotations"]["audience"],
        json!(["user", "[redacted]"])
    );
    let mut config = json!({
        "sessionId": "s1",
        "update": {
            "sessionUpdate": "config_option_update",
            "configOptions": [{ "id": "model", "name": "Model", "type": secret, "currentValue": "x" }],
        },
    });
    redactor.notification(&mut config);
    assert_eq!(config["update"]["configOptions"][0]["type"], "[redacted]");
}

/// Remote parity S6: each remote agent gets a private, owner-only temp
/// folder beside its checkout as `TMPDIR` / `TMP` / `TEMP`, over the inherited
/// value, and the folder is removed with the checkout.
#[tokio::test]
async fn node_runner_gives_each_agent_a_private_tmpdir_removed_with_its_checkout() {
    use std::os::unix::fs::PermissionsExt as _;
    let Some(h) = harness(2, &["--dump-env"]) else {
        return;
    };
    h.runner.launch(launch("a1")).await;
    h.runner.launch(launch("a2")).await;
    let mut tmps = Vec::new();
    for id in ["a1", "a2"] {
        let env = agent_env(&h.storage, id).await;
        let checkout = h.storage.agent_dir(&AgentId::from(id)).unwrap();
        let expected = checkout.parent().unwrap().join("tmp");
        for key in ["TMPDIR", "TMP", "TEMP"] {
            assert_eq!(
                env.get(key).and_then(Value::as_str),
                Some(expected.to_string_lossy().as_ref()),
                "{id} {key}"
            );
        }
        let mode = std::fs::metadata(&expected).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700, "{id}: {mode:o}");
        tmps.push(expected);
    }
    assert_ne!(tmps[0], tmps[1]);
    std::fs::write(tmps[0].join("scratch.txt"), "x").unwrap();

    let id = AgentId::from("a1");
    wait_for(&h.runner, "started", started("a1")).await;
    tokio::time::timeout(wait(), h.runner.final_stop(&id))
        .await
        .unwrap();
    assert!(!tmps[0].exists(), "the temp folder goes with the checkout");
    assert!(tmps[1].is_dir(), "another agent's folder stays");
    h.runner.stop(&AgentId::from("a2"));
    wait_for(&h.runner, "exit", exited("a2")).await;
}

/// A final stop (nodeProtocol 9) stops the agent, makes its exit push the
/// final one (bounded by the final-push timeout) and only then removes its
/// checkout; its acknowledgement reports the push.
#[tokio::test]
async fn node_runner_final_stop_pushes_within_its_bound_then_removes_the_checkout() {
    use crate::executor::checkout::FINAL_PUSH_TIMEOUT;
    use intent_core::node_link::{FinalPush, StopOutcome};
    let Some(h) = harness(2, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    let checkout = h.storage.existing_agent_dir(&id).expect("a checkout");
    std::fs::write(checkout.join("work.txt"), "unsaved").unwrap();

    let acked = tokio::time::timeout(wait(), h.runner.final_stop(&id))
        .await
        .unwrap();
    assert_eq!(acked, Some((StopOutcome::Stopped, FinalPush::Pushed)));
    assert_eq!(
        h.checkouts.push_timeouts.lock().unwrap().last(),
        Some(&FINAL_PUSH_TIMEOUT)
    );
    assert!(records(&h.runner).await.iter().any(exited("a1")));
    assert!(!checkout.exists(), "the checkout is removed after the push");
    assert!(h.storage.agent_ids().is_empty());

    // A repeated final stop finds nothing to stop or push.
    assert_eq!(
        h.runner.final_stop(&id).await,
        Some((StopOutcome::NotRunning, FinalPush::Skipped))
    );
}

/// A final stop of an agent that already exited pushes its checkout once
/// more when the machine knows its workspace; a failed push is reported and
/// the checkout still goes.
#[tokio::test]
async fn node_runner_final_stop_of_an_exited_agent_pushes_and_reports_a_failed_push() {
    use intent_core::node_link::{FinalPush, StopOutcome};
    let Some(h) = harness(2, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    h.runner.stop(&id).wait().await.unwrap();
    assert!(h.storage.existing_agent_dir(&id).is_some());
    h.checkouts
        .fail_pushes
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let pushes = h.checkouts.pushes.lock().unwrap().len();
    assert_eq!(
        h.runner.final_stop(&id).await,
        Some((StopOutcome::NotRunning, FinalPush::Failed))
    );
    assert_eq!(h.checkouts.pushes.lock().unwrap().len(), pushes + 1);
    assert!(h.storage.existing_agent_dir(&id).is_none());
}

/// A final stop delivered again while the first is still pushing joins it:
/// one push, one removal, the same answer for both, and a launch of the
/// agent in the meantime waits for the cleanup instead of losing its new
/// checkout to it.
#[tokio::test]
async fn node_runner_repeated_final_stops_share_one_cleanup_and_fence_launches() {
    use intent_core::node_link::{FinalPush, StopOutcome};
    let Some(h) = harness(2, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    h.runner.stop(&id).wait().await.unwrap();
    assert!(h.storage.existing_agent_dir(&id).is_some());
    let pushes = h.checkouts.pushes.lock().unwrap().len();

    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    let first = tokio::spawn(h.runner.final_stop(&id));
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();
    let second = tokio::spawn(h.runner.final_stop(&id));
    let runner = h.runner.clone();
    let relaunch = tokio::spawn(async move { runner.launch(launch("a1")).await });
    // timing-guard: a short window in which nothing may finish while the push is held
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !second.is_finished(),
        "the repeat finished before the cleanup"
    );
    assert_eq!(h.checkouts.pushes.lock().unwrap().len(), pushes + 1);
    assert!(h.storage.existing_agent_dir(&id).is_some());

    release.send(()).unwrap();
    let expected = Some((StopOutcome::NotRunning, FinalPush::Pushed));
    assert_eq!(
        tokio::time::timeout(wait(), first).await.unwrap().unwrap(),
        expected
    );
    assert_eq!(
        tokio::time::timeout(wait(), second).await.unwrap().unwrap(),
        expected
    );
    assert_eq!(
        h.checkouts.pushes.lock().unwrap().len(),
        pushes + 1,
        "the repeat pushed again"
    );
    // The relaunch ran after the cleanup: its checkout is there and stays.
    tokio::time::timeout(wait(), relaunch)
        .await
        .unwrap()
        .unwrap();
    let deadline = tokio::time::Instant::now() + wait();
    while records(&h.runner)
        .await
        .iter()
        .filter(|r| started("a1")(r))
        .count()
        < 2
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the relaunch never started"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        h.storage.existing_agent_dir(&id).is_some(),
        "the cleanup removed the relaunch's checkout"
    );
    h.runner.stop(&id).wait().await.unwrap();
}

/// A cleanup that panics leaves nothing behind: its stop and a repeat
/// waiting on it fail (so the backend resends rather than getting an ack),
/// a launch of the agent goes ahead instead of waiting forever, and the
/// next final stop runs the cleanup again.
#[tokio::test]
async fn node_runner_a_panicked_final_stop_cleanup_fails_cleanly() {
    use intent_core::node_link::{FinalPush, StopOutcome};
    let Some(h) = harness(2, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    h.runner.stop(&id).wait().await.unwrap();

    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    h.checkouts
        .panic_push
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first = tokio::spawn(h.runner.final_stop(&id));
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();
    let repeat = tokio::spawn(h.runner.final_stop(&id));
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), first).await.unwrap().unwrap(),
        None
    );
    assert_eq!(
        tokio::time::timeout(wait(), repeat).await.unwrap().unwrap(),
        None
    );
    assert!(
        h.storage.existing_agent_dir(&id).is_some(),
        "nothing was removed"
    );

    // A launch is not left waiting on the dead cleanup.
    tokio::time::timeout(wait(), h.runner.launch(launch("a1")))
        .await
        .expect("the launch waited on a panicked cleanup");
    let deadline = tokio::time::Instant::now() + wait();
    while records(&h.runner)
        .await
        .iter()
        .filter(|r| started("a1")(r))
        .count()
        < 2
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the relaunch never started"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The next final stop runs the cleanup again, to the end.
    assert_eq!(
        tokio::time::timeout(wait(), h.runner.final_stop(&id))
            .await
            .unwrap(),
        Some((StopOutcome::Stopped, FinalPush::Pushed))
    );
    assert!(h.storage.existing_agent_dir(&id).is_none());
}

/// A final stop whose checkout cannot be removed is not acknowledged, so
/// the backend resends it.
#[tokio::test]
async fn node_runner_final_stop_is_not_acknowledged_while_the_checkout_stays() {
    let Some(h) = harness(2, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    h.checkouts
        .fail_removals
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(h.runner.final_stop(&id).await, None);
    assert!(h.storage.existing_agent_dir(&id).is_some());
    h.checkouts
        .fail_removals
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(h.runner.final_stop(&id).await.is_some());
    assert!(h.storage.existing_agent_dir(&id).is_none());
}

/// The bind sweep removes the checkouts of agents the backend no longer
/// records, and keeps those it lists and those still running.
#[tokio::test]
async fn node_runner_sweep_removes_only_orphaned_checkouts() {
    let Some(h) = harness(2, &[]) else { return };
    h.runner.launch(launch("running")).await;
    wait_for(&h.runner, "started", started("running")).await;
    for orphan in ["kept", "orphan"] {
        h.storage.agent_dir(&AgentId::from(orphan)).unwrap();
    }
    let swept = h.runner.sweep(&[AgentId::from("kept")]).await;
    assert_eq!(swept, vec![AgentId::from("orphan")]);
    assert_eq!(
        h.storage.agent_ids(),
        vec![AgentId::from("kept"), AgentId::from("running")]
    );
}

/// A launch that arrives while the bind sweep is removing its agent's
/// checkout waits for the removal, then starts on a fresh checkout.
#[tokio::test]
async fn node_runner_a_launch_during_the_sweep_waits_for_the_removal() {
    let Some(h) = harness(2, &[]) else { return };
    let id = AgentId::from("a1");
    h.storage.agent_dir(&id).unwrap();
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *h.checkouts.hold_sweep.lock().unwrap() = Some((entered_tx, release_rx));
    let sweeping = {
        let runner = h.runner.clone();
        tokio::spawn(async move { runner.sweep(&[]).await })
    };
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();

    let launching = {
        let runner = h.runner.clone();
        tokio::spawn(async move { runner.launch(launch("a1")).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !records(&h.runner).await.iter().any(started("a1")),
        "the launch waits for the removal"
    );
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(wait(), sweeping)
            .await
            .unwrap()
            .unwrap(),
        vec![id.clone()]
    );
    tokio::time::timeout(wait(), launching)
        .await
        .unwrap()
        .unwrap();
    wait_for(&h.runner, "started", started("a1")).await;
    assert!(h.storage.existing_agent_dir(&id).is_some());
}

/// The sweep takes its list before the new link's frames are handled; an
/// agent launched after that keeps its checkout.
#[tokio::test]
async fn node_runner_the_sweep_leaves_an_agent_launched_after_its_list() {
    let Some(h) = harness(2, &[]) else { return };
    let id = AgentId::from("a1");
    h.storage.agent_dir(&id).unwrap();
    let candidates = h.runner.sweep_candidates(&[]);
    assert_eq!(candidates, vec![id.clone()]);
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    h.runner.stop(&id).wait().await;
    assert!(h.runner.sweep_checkouts(candidates).await.is_empty());
    assert!(h.storage.existing_agent_dir(&id).is_some());
}

/// A checkout whose saved wip work could not be restored tells the agent on
/// its first turn, and only then.
#[tokio::test]
async fn node_runner_prepends_the_checkout_notice_to_the_first_turn_only() {
    let Some(h) = harness(1, &[]) else { return };
    *h.checkouts.notice.lock().unwrap() = Some("wip-not-restored.".into());
    h.runner.launch(launch("a1")).await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "first turn end", turn_ended("a1", "t1")).await;
    let echoed = |all: &[JournalRecord], turn: &str, expected: &str| {
        all.iter().any(|r| {
            r.turn_id.as_deref() == Some(turn)
                && matches!(&r.body, JournalBody::Notification { notification }
                    if notification.to_string().contains(expected))
        })
    };
    assert!(
        echoed(&all, "t1", "echo: Note from Intent: wip-not-restored.hello"),
        "{all:#?}"
    );
    h.runner
        .prompt(&AgentId::from("a1"), "t2".into(), text("again"))
        .await;
    let all = wait_for(&h.runner, "second turn end", turn_ended("a1", "t2")).await;
    assert!(echoed(&all, "t2", "echo: again"), "{all:#?}");
    assert!(!echoed(&all, "t2", "wip-not-restored"), "{all:#?}");
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
}

/// A launch after a disk-only resume (Northflank) tells the agent's first
/// turn, in the same notice as an unrestored wip, what the machine kept and
/// lost; later turns and plain launches say nothing about it.
#[tokio::test]
async fn node_runner_tells_the_first_turn_after_a_disk_resume() {
    let Some(h) = harness(2, &[]) else { return };
    *h.checkouts.notice.lock().unwrap() = Some("wip-not-restored.".into());
    let mut resumed = launch("a1");
    resumed.resumed_from_disk = true;
    h.runner.launch(resumed).await;
    h.runner.launch(launch("a2")).await;
    let echoed = |all: &[JournalRecord], turn: &str, expected: &str| {
        all.iter().any(|r| {
            r.turn_id.as_deref() == Some(turn)
                && matches!(&r.body, JournalBody::Notification { notification }
                    if notification.to_string().contains(expected))
        })
    };
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "first turn end", turn_ended("a1", "t1")).await;
    assert!(
        echoed(&all, "t1", "stopped and started again from its disk"),
        "{all:#?}"
    );
    assert!(echoed(&all, "t1", "wip-not-restored."), "{all:#?}");
    h.runner
        .prompt(&AgentId::from("a1"), "t2".into(), text("again"))
        .await;
    let all = wait_for(&h.runner, "second turn end", turn_ended("a1", "t2")).await;
    assert!(!echoed(&all, "t2", "from its disk"), "{all:#?}");
    h.runner
        .prompt(&AgentId::from("a2"), "t3".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "other agent's turn end", turn_ended("a2", "t3")).await;
    assert!(!echoed(&all, "t3", "from its disk"), "{all:#?}");
    for id in ["a1", "a2"] {
        h.runner.stop(&AgentId::from(id));
        wait_for(&h.runner, "exit", exited(id)).await;
    }
}

/// Every remote agent's first turn on the machine opens with the
/// long-commands hint, before any checkout notice; later turns do not.
#[tokio::test]
async fn node_runner_opens_the_first_turn_with_the_long_commands_hint() {
    let Some(h) = harness(1, &[]) else { return };
    *h.checkouts.notice.lock().unwrap() = Some("wip-not-restored.".into());
    h.runner.launch(launch("a1")).await;
    let raw = |all: &[JournalRecord], turn: &str| -> String {
        all.iter()
            .filter(|r| r.turn_id.as_deref() == Some(turn))
            .filter_map(|r| match &r.body {
                JournalBody::Notification { notification } => notification["update"]["content"]
                    ["text"]
                    .as_str()
                    .map(str::to_owned),
                _ => None,
            })
            .collect()
    };
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("echo-raw"))
        .await;
    let all = wait_for(&h.runner, "first turn end", turn_ended("a1", "t1")).await;
    let first = raw(&all, "t1");
    let sent: Value = serde_json::from_str(first.strip_prefix("raw: ").unwrap()).unwrap();
    let first = sent["raw"].as_str().unwrap();
    let hint = format!(
        "<system>{}</system>\n\nNote from Intent: wip-not-restored.",
        intent_core::machine_selection::REMOTE_LONG_COMMANDS_HINT
    );
    assert!(first.starts_with(&hint), "{first}");
    h.runner
        .prompt(&AgentId::from("a1"), "t2".into(), text("echo-raw"))
        .await;
    let all = wait_for(&h.runner, "second turn end", turn_ended("a1", "t2")).await;
    let second = raw(&all, "t2");
    assert!(second.starts_with("raw: "), "{second}");
    assert!(!second.contains("ws.script.start"), "{second}");
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
}

/// A push while the daemon is stopping is the agent's last one, bounded like
/// a final stop's (intent-hq/intent#8437).
#[tokio::test]
async fn node_runner_shutdown_pushes_within_the_final_bound() {
    use crate::executor::checkout::FINAL_PUSH_TIMEOUT;
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    h.runner.shutdown().await;
    assert_eq!(
        h.checkouts.pushes.lock().unwrap().as_slice(),
        &[AgentId::from("a1")]
    );
    assert_eq!(
        h.checkouts.push_timeouts.lock().unwrap().last(),
        Some(&FINAL_PUSH_TIMEOUT)
    );
}

/// A final stop's cleanup already under way when the daemon stops is joined
/// within the shutdown drain: its push and the checkout's removal finish
/// while the backend's links are still up (intent-hq/intent#8437).
#[tokio::test]
async fn node_runner_shutdown_joins_a_final_stop_cleanup_in_flight() {
    use intent_core::node_link::{FinalPush, StopOutcome};
    let Some(h) = harness(1, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    h.runner.stop(&id).wait().await.unwrap();
    assert!(h.storage.existing_agent_dir(&id).is_some());

    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    let cleanup = tokio::spawn(h.runner.final_stop(&id));
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();
    let runner = h.runner.clone();
    let shutdown = tokio::spawn(async move { runner.shutdown().await });
    // timing-guard: a short window in which shutdown may not return while the cleanup's push is held
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown returned with a final stop's cleanup still running"
    );

    release.send(()).unwrap();
    tokio::time::timeout(wait(), shutdown)
        .await
        .unwrap()
        .unwrap();
    assert!(
        h.storage.existing_agent_dir(&id).is_none(),
        "the cleanup removed the checkout before shutdown returned"
    );
    assert_eq!(
        tokio::time::timeout(wait(), cleanup)
            .await
            .unwrap()
            .unwrap(),
        Some((StopOutcome::NotRunning, FinalPush::Pushed))
    );
}

/// An ordinary turn-end push still under way when the daemon stops is
/// cancelled, so the agent's last push (bounded by `FINAL_PUSH_TIMEOUT`)
/// starts within the shutdown budget instead of queueing behind it
/// (intent-hq/intent#8437).
#[tokio::test]
async fn node_runner_shutdown_cancels_an_ordinary_push_in_flight() {
    use crate::executor::checkout::{FINAL_PUSH_TIMEOUT, WIP_PUSH_TIMEOUT};
    let Some(h) = harness(1, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    // Never released: only the shutdown can end this push.
    let (_release, release_rx) = tokio::sync::oneshot::channel::<()>();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    h.runner.prompt(&id, "t1".into(), text("hello")).await;
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();

    tokio::time::timeout(wait(), h.runner.shutdown())
        .await
        .expect("shutdown waited on the ordinary push in flight");
    assert_eq!(
        h.checkouts.pushes.lock().unwrap().as_slice(),
        &[id.clone(), id],
        "the agent's last push ran after the ordinary one was cancelled"
    );
    assert_eq!(
        h.checkouts.push_timeouts.lock().unwrap().as_slice(),
        &[WIP_PUSH_TIMEOUT, FINAL_PUSH_TIMEOUT]
    );
}

/// Outside a shutdown, a final stop still waits for an ordinary push in
/// flight, then pushes once more: nothing is cancelled.
#[tokio::test]
async fn node_runner_final_stop_waits_for_an_ordinary_push_in_flight() {
    use crate::executor::checkout::{FINAL_PUSH_TIMEOUT, WIP_PUSH_TIMEOUT};
    use intent_core::node_link::FinalPush;
    let Some(h) = harness(1, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    h.runner.prompt(&id, "t1".into(), text("hello")).await;
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();

    let stop = tokio::spawn(h.runner.final_stop(&id));
    // timing-guard: a short window in which the final stop may not finish while the ordinary push is held
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !stop.is_finished(),
        "the final stop finished while an ordinary push was held"
    );
    assert_eq!(h.checkouts.pushes.lock().unwrap().len(), 1);

    release.send(()).unwrap();
    let (_, push) = tokio::time::timeout(wait(), stop)
        .await
        .unwrap()
        .unwrap()
        .expect("the final stop is acknowledged");
    assert_eq!(push, FinalPush::Pushed);
    assert_eq!(
        h.checkouts.push_timeouts.lock().unwrap().as_slice(),
        &[WIP_PUSH_TIMEOUT, FINAL_PUSH_TIMEOUT]
    );
}

/// Turn-end pushes queued behind the one in flight when the daemon stops
/// are skipped, not run with the final bound one by one: the agent's exit
/// push is the only bounded push left, so it starts within the shutdown
/// budget and sends everything they would have (intent-hq/intent#8437).
#[tokio::test]
async fn node_runner_shutdown_skips_queued_turn_end_pushes() {
    use crate::executor::checkout::{FINAL_PUSH_TIMEOUT, WIP_PUSH_TIMEOUT};
    let Some(h) = harness(1, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    // Never released: only the shutdown can end this push.
    let (_release, release_rx) = tokio::sync::oneshot::channel::<()>();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    h.runner.prompt(&id, "t1".into(), text("one")).await;
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();
    // More turns end while the first push is held; their pushes queue.
    for turn in ["t2", "t3", "t4"] {
        h.runner.prompt(&id, turn.into(), text("more")).await;
        wait_for(&h.runner, "turn end", turn_ended("a1", turn)).await;
    }

    tokio::time::timeout(wait(), h.runner.shutdown())
        .await
        .expect("shutdown waited on queued turn-end pushes");
    assert_eq!(
        h.checkouts.push_timeouts.lock().unwrap().as_slice(),
        &[WIP_PUSH_TIMEOUT, FINAL_PUSH_TIMEOUT],
        "only the held push and the agent's exit push ran"
    );
}

/// An exit push already under way with the ordinary bound when the daemon
/// stops is cancelled and sent again with the final bound, so it ends
/// within the shutdown budget.
#[tokio::test]
async fn node_runner_shutdown_rebounds_an_exit_push_in_flight() {
    use crate::executor::checkout::{FINAL_PUSH_TIMEOUT, WIP_PUSH_TIMEOUT};
    let Some(h) = harness(1, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    let (_release, release_rx) = tokio::sync::oneshot::channel::<()>();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    let _stopping = h.runner.stop(&id);
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();

    tokio::time::timeout(wait(), h.runner.shutdown())
        .await
        .expect("shutdown waited on the ordinary exit push");
    assert_eq!(
        h.checkouts.push_timeouts.lock().unwrap().as_slice(),
        &[WIP_PUSH_TIMEOUT, FINAL_PUSH_TIMEOUT]
    );
}

/// A turn-end push that starts while a final stop is still stopping the
/// agent's scripts (its flag set, the agent still running) gives way when
/// the daemon then stops too: the exit push starts at once instead of
/// queueing behind a bounded turn-end push (intent-hq/intent#8437).
#[tokio::test]
async fn node_runner_shutdown_cancels_a_turn_end_push_during_a_final_stop() {
    use crate::executor::checkout::FINAL_PUSH_TIMEOUT;
    let Some(h) = harness(1, &[]) else { return };
    let id = AgentId::from("a1");
    h.runner.launch(launch("a1")).await;
    wait_for(&h.runner, "started", started("a1")).await;
    // The final stop's script-stop phase: its flag is set and the agent
    // still runs.
    // (No reference to the agent is kept: its journal pump ends when the
    // last one is dropped.)
    lock(&h.runner.inner.state)
        .agents
        .get(&id)
        .unwrap()
        .final_stop
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (entered_tx, entered) = tokio::sync::oneshot::channel();
    // Never released: only the shutdown can end this push.
    let (_release, release_rx) = tokio::sync::oneshot::channel::<()>();
    *h.checkouts.hold_push.lock().unwrap() = Some((entered_tx, release_rx));
    h.runner.prompt(&id, "t1".into(), text("hello")).await;
    tokio::time::timeout(wait(), entered)
        .await
        .unwrap()
        .unwrap();

    tokio::time::timeout(wait(), h.runner.shutdown())
        .await
        .expect("shutdown waited on the turn-end push of a final stop");
    assert_eq!(
        h.checkouts.push_timeouts.lock().unwrap().as_slice(),
        &[FINAL_PUSH_TIMEOUT, FINAL_PUSH_TIMEOUT],
        "the turn-end push gave way and the exit push ran"
    );
}

fn setup_finished(id: &str) -> impl Fn(&JournalRecord) -> bool + '_ {
    move |r| {
        r.agent_id().is_some_and(|a| a.0 == id)
            && matches!(r.body, JournalBody::SetupFinished { .. })
    }
}

/// Wait until `id` has journaled `count` exits.
async fn wait_exits(runner: &Runner, id: &str, count: usize) {
    let deadline = tokio::time::Instant::now() + wait();
    while records(runner)
        .await
        .iter()
        .filter(|r| exited(id)(r))
        .count()
        < count
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{id} never exited {count} times"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn setup_started(id: &str) -> impl Fn(&JournalRecord) -> bool + '_ {
    move |r| {
        r.agent_id().is_some_and(|a| a.0 == id)
            && matches!(r.body, JournalBody::SetupStarted { .. })
    }
}

/// A launch of `id` with a prepared checkout and `script` as its setup.
fn launch_with_setup(id: &str, script: Option<&str>, timeout_secs: Option<u64>) -> LaunchCommand {
    use intent_core::node_link::{PreparedCheckout, SetupCommand};
    let mut command = launch(id);
    command.checkout = Some(Box::new(PreparedCheckout {
        submodules: Vec::new(),
        setup: script.map(|script| SetupCommand {
            script: script.into(),
            branch_name: "feat/x".into(),
            source_branch: "main".into(),
            timeout_secs,
        }),
    }));
    command
}

/// `id`'s `setupFinished` state, exit code and error.
fn finished(
    all: &[JournalRecord],
    id: &str,
) -> (
    intent_core::node_link::SetupState,
    Option<i32>,
    Option<String>,
) {
    match &find(all, setup_finished(id)).body {
        JournalBody::SetupFinished {
            state,
            exit_code,
            error,
            ..
        } => (*state, *exit_code, error.clone()),
        _ => unreachable!(),
    }
}

/// Whether `id`'s turn `turn` was told setup failed (the fixture echoes its
/// prompt).
fn told_setup_failed(all: &[JournalRecord], id: &str, turn: &str, needle: &str) -> bool {
    all.iter().any(|r| {
        r.agent_id().is_some_and(|a| a.0 == id)
            && r.turn_id.as_deref() == Some(turn)
            && matches!(&r.body, JournalBody::Notification { notification }
                if notification.to_string().contains("workspace setup failed")
                    && notification.to_string().contains(needle))
    })
}

/// Test 8 (intent-hq/intent#8594): a fresh prepared checkout runs its
/// setup script before the agent starts, journaling `setupStarted` and
/// `setupFinished` before `started`, with the documented environment (and
/// the agent's private `TMPDIR`) and never the profile's values or the agent's git wiring; without a script
/// it records `skipped`. A failing or timed-out script still starts the
/// agent, whose first turn (only) carries the failure notice. A launch
/// without `checkout` records nothing, a relaunch that keeps the checkout
/// does not run setup again, and one without a checkout does.
#[tokio::test]
async fn node_runner_runs_setup_before_the_agent_starts() {
    use intent_core::node_link::SetupState;
    let Some(h) = harness(5, &[]) else { return };
    let env_script =
        "printf '%s|%s|%s|%s|%s|%s|%s|%s\\n' \"$(pwd -P)\" \"$WORKTREE_PATH\" \"$MAIN_CHECKOUT\" \
         \"$BRANCH_NAME\" \"$SOURCE_BRANCH\" \"$INTENT_REMOTE_MACHINE\" \
         \"${CODEX_ACCESS_TOKEN-none}${INTENT_NODE_GIT_SOCKET-}\" \"$TMPDIR\" > setup-env.txt\n\
         echo ran >> setup-runs.txt\n";
    let mut with_profile = launch_with_setup("a1", Some(env_script), None);
    with_profile.profile_env = launch_with_profile(
        "a1",
        &json!([{ "key": "CODEX_ACCESS_TOKEN", "value": CANARY }]),
    )
    .profile_env;
    h.runner.launch(with_profile).await;
    h.runner
        .launch(launch_with_setup(
            "a2",
            Some("echo boom-from-setup\nexit 3\n"),
            None,
        ))
        .await;
    h.runner
        .launch(launch_with_setup("a3", Some("sleep 30\n"), Some(1)))
        .await;
    h.runner.launch(launch_with_setup("a4", None, None)).await;
    h.runner.launch(launch("a5")).await;
    for id in ["a1", "a2", "a3", "a4", "a5"] {
        wait_for(&h.runner, "started", started(id)).await;
    }
    let all = records(&h.runner).await;
    for id in ["a1", "a2", "a3"] {
        let begun = all.iter().position(setup_started(id)).unwrap();
        let ended = all.iter().position(setup_finished(id)).unwrap();
        let start = all.iter().position(started(id)).unwrap();
        assert!(begun < ended && ended < start, "{all:#?}");
    }
    assert_eq!(finished(&all, "a1"), (SetupState::Completed, Some(0), None));
    assert_eq!(finished(&all, "a2"), (SetupState::Failed, Some(3), None));
    let (state, code, error) = finished(&all, "a3");
    assert_eq!((state, code), (SetupState::Failed, None));
    assert!(error.unwrap().starts_with("node-setup-timeout: "));
    assert_eq!(finished(&all, "a4"), (SetupState::Skipped, None, None));
    assert!(!all.iter().any(setup_started("a4")), "{all:#?}");
    assert!(!all.iter().any(setup_finished("a5")), "{all:#?}");

    let worktree = h.storage.existing_agent_dir(&AgentId::from("a1")).unwrap();
    let env = std::fs::read_to_string(worktree.join("setup-env.txt")).unwrap();
    let fields: Vec<&str> = env.trim_end().split('|').collect();
    let here = worktree.canonicalize().unwrap();
    assert_eq!(std::path::Path::new(fields[0]), here, "{env}");
    assert_eq!(std::path::Path::new(fields[1]), worktree, "{env}");
    assert!(fields[2].ends_with("/repos/repo/main"), "{env}");
    assert!(std::path::Path::new(fields[2]).is_dir(), "{env}");
    assert_eq!(fields[3..7], ["feat/x", "main", "1", "none"], "{env}");
    let agent_dir = worktree.parent().unwrap();
    // The script gets the agent's private temp folder (remote parity S6).
    assert_eq!(
        std::path::Path::new(fields[7]),
        agent_dir.join("tmp"),
        "{env}"
    );
    assert!(
        !std::fs::read_dir(agent_dir)
            .unwrap()
            .chain(std::fs::read_dir(&worktree).unwrap())
            .any(|entry| entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "sh")),
        "the script file is written outside the checkout and removed"
    );

    for id in ["a1", "a2", "a3"] {
        h.runner
            .prompt(&AgentId::from(id), "t1".into(), text("hello"))
            .await;
        wait_for(&h.runner, "turn end", turn_ended(id, "t1")).await;
        h.runner
            .prompt(&AgentId::from(id), "t2".into(), text("again"))
            .await;
    }
    for id in ["a1", "a2", "a3"] {
        wait_for(&h.runner, "second turn end", turn_ended(id, "t2")).await;
    }
    let all = records(&h.runner).await;
    assert!(!told_setup_failed(&all, "a1", "t1", ""), "{all:#?}");
    assert!(told_setup_failed(&all, "a2", "t1", "exited with code 3"));
    assert!(told_setup_failed(&all, "a2", "t1", "boom-from-setup"));
    assert!(told_setup_failed(&all, "a3", "t1", "timed out"));
    for id in ["a2", "a3"] {
        assert!(!told_setup_failed(&all, id, "t2", ""), "the notice is once");
    }

    // A relaunch that keeps the checkout does not run setup again.
    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "exit", exited("a1")).await;
    h.runner
        .launch(launch_with_setup("a1", Some(env_script), None))
        .await;
    h.runner
        .prompt(&AgentId::from("a1"), "t3".into(), text("kept"))
        .await;
    let all = wait_for(&h.runner, "kept turn end", turn_ended("a1", "t3")).await;
    assert_eq!(all.iter().filter(|r| setup_started("a1")(r)).count(), 1);
    assert_eq!(all.iter().filter(|r| setup_finished("a1")(r)).count(), 1);
    // A relaunch without its checkout runs it again.
    h.runner.stop(&AgentId::from("a1"));
    wait_exits(&h.runner, "a1", 2).await;
    h.storage.remove_agent(&AgentId::from("a1")).unwrap();
    h.runner
        .launch(launch_with_setup("a1", Some(env_script), None))
        .await;
    h.runner
        .prompt(&AgentId::from("a1"), "t4".into(), text("fresh"))
        .await;
    let all = wait_for(&h.runner, "fresh turn end", turn_ended("a1", "t4")).await;
    assert_eq!(all.iter().filter(|r| setup_started("a1")(r)).count(), 2);
    for id in ["a1", "a2", "a3", "a4", "a5"] {
        h.runner.stop(&AgentId::from(id));
    }
}

/// A stop during setup kills the script's process group and journals the
/// exit with neither `setupFinished` nor `started`; the next launch keeps
/// that checkout, records `failed` with `node-setup-interrupted` without
/// running the script again, and tells the agent.
#[tokio::test]
async fn node_runner_stop_during_setup_kills_it_and_records_it_interrupted() {
    use intent_core::node_link::SetupState;
    let Some(h) = harness(1, &[]) else { return };
    let script = "echo ran >> setup-runs.txt\nsleep 60 &\necho $! > child.pid\nwait\n";
    h.runner
        .launch(launch_with_setup("a1", Some(script), None))
        .await;
    wait_for(&h.runner, "setup started", setup_started("a1")).await;
    let worktree = h.storage.existing_agent_dir(&AgentId::from("a1")).unwrap();
    let pid_file = worktree.join("child.pid");
    let deadline = tokio::time::Instant::now() + wait();
    let child: i32 = loop {
        if let Some(pid) = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|pid| pid.trim().parse().ok())
        {
            break pid;
        }
        assert!(tokio::time::Instant::now() < deadline, "setup never ran");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    h.runner.stop(&AgentId::from("a1"));
    let all = wait_for(&h.runner, "exit", exited("a1")).await;
    assert!(!all.iter().any(setup_finished("a1")), "{all:#?}");
    assert!(!all.iter().any(started("a1")), "{all:#?}");
    let deadline = tokio::time::Instant::now() + wait();
    while nix::sys::signal::kill(nix::unistd::Pid::from_raw(child), None).is_ok() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the script's child outlived the stop"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    h.runner
        .launch(launch_with_setup("a1", Some(script), None))
        .await;
    h.runner
        .prompt(&AgentId::from("a1"), "t1".into(), text("hello"))
        .await;
    let all = wait_for(&h.runner, "turn end", turn_ended("a1", "t1")).await;
    let (state, code, error) = finished(&all, "a1");
    assert_eq!((state, code), (SetupState::Failed, None));
    assert!(error.unwrap().starts_with("node-setup-interrupted: "));
    assert_eq!(all.iter().filter(|r| setup_started("a1")(r)).count(), 1);
    assert_eq!(
        std::fs::read_to_string(worktree.join("setup-runs.txt")).unwrap(),
        "ran\n",
        "no re-run"
    );
    assert!(told_setup_failed(
        &all,
        "a1",
        "t1",
        "stopped before it finished"
    ));
    h.runner.stop(&AgentId::from("a1"));
}

/// A kept checkout with no recorded setup (its first launch was stopped
/// before setup, or ran on a machine without it) records one result, so
/// the backend never waits on a setup that will not come: `failed` without
/// running a script it carries, `skipped` without one.
#[tokio::test]
async fn node_runner_records_setup_for_a_kept_checkout_without_one() {
    use intent_core::node_link::SetupState;
    let Some(h) = harness(2, &[]) else { return };
    for id in ["a1", "a2"] {
        h.storage.agent_dir(&AgentId::from(id)).unwrap();
    }
    h.runner
        .launch(launch_with_setup("a1", Some("echo ran > ran.txt\n"), None))
        .await;
    h.runner.launch(launch_with_setup("a2", None, None)).await;
    for id in ["a1", "a2"] {
        wait_for(&h.runner, "started", started(id)).await;
    }
    let all = records(&h.runner).await;
    let (state, code, error) = finished(&all, "a1");
    assert_eq!((state, code), (SetupState::Failed, None));
    assert!(error.unwrap().starts_with("node-setup-interrupted: "));
    assert!(!all.iter().any(setup_started("a1")), "{all:#?}");
    let worktree = h.storage.existing_agent_dir(&AgentId::from("a1")).unwrap();
    assert!(
        !worktree.join("ran.txt").exists(),
        "no run on a kept checkout"
    );
    assert_eq!(finished(&all, "a2"), (SetupState::Skipped, None, None));
    for id in ["a1", "a2"] {
        h.runner.stop(&AgentId::from(id));
    }
}

/// A Default launch of codex or claude-code refused at setup reports
/// `node-provider-signed-out`, naming the provider and the fix and never
/// the provider's own error text.
#[test]
fn signed_out_applies_only_to_providers_with_a_default_configuration() {
    assert!(signs_in_on_the_machine("claude-code"));
    assert!(signs_in_on_the_machine("codex"));
    assert!(!signs_in_on_the_machine(PROVIDER));
    assert!(!signs_in_on_the_machine("unsloth"));
    let error = signed_out_error("claude-code");
    assert!(error.starts_with(codes::SIGNED_OUT), "{error}");
    assert!(
        error.contains("Claude Code isn't signed in on this machine"),
        "{error}"
    );
    assert!(error.contains("marked for remote machines"), "{error}");
    assert!(exit_detail(None, Some(&error)).starts_with("the provider did not start"));
}

/// A quick action that never finishes, for the cancel tests.
struct Hangs;

impl super::MachineOneShot for Hangs {
    fn run(
        &self,
        _frame: intent_core::node_link::OneShot,
        _profile: Option<intent_acp::ProfileEnv>,
    ) -> intent_core::BoxFuture<'static, intent_core::node_link::OneShotReply> {
        Box::pin(std::future::pending())
    }
}

fn quick_action_runner(dir: &std::path::Path) -> Runner {
    let storage = Arc::new(Storage::open(&dir.join("node"), None).unwrap());
    Runner::new(RunnerConfig {
        one_shot: Some(Arc::new(Hangs)),
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
        git_socket: None,
        pull_staging: storage.pull_staging_dir(),
        forge_git: None,
        home: None,
        auth_failed: Arc::new(|_: &str| {}),
    })
}

fn quick_action(id: u128) -> intent_core::node_link::OneShot {
    use intent_core::node_link::{OneShotCompleteInput, OneShotInput, OneShotOperation};
    intent_core::node_link::OneShot {
        request_id: uuid::Uuid::from_u128(id),
        operation: OneShotOperation::Complete,
        input: OneShotInput::Complete(OneShotCompleteInput {
            provider_id: "codex".into(),
            prompt: "hi".into(),
            system_prompt: None,
            model: None,
            effort: None,
        }),
        profile_env: None,
        timeout_ms: 1_000,
    }
}

/// `oneShotCancel` ends its quick action as `cancelled`; the end of the
/// link ends every one still running.
#[tokio::test]
async fn quick_actions_end_on_cancel_and_on_link_loss() {
    use intent_core::node_link::OneShotOutcome;
    let dir = tempfile::tempdir().unwrap();
    let runner = quick_action_runner(dir.path());
    let first = tokio::spawn({
        let runner = runner.clone();
        let cancelled = runner.begin_one_shot(uuid::Uuid::from_u128(1), NodeCounter(1));
        async move { runner.one_shot(quick_action(1), cancelled).await }
    });
    let second = tokio::spawn({
        let runner = runner.clone();
        let cancelled = runner.begin_one_shot(uuid::Uuid::from_u128(2), NodeCounter(1));
        async move { runner.one_shot(quick_action(2), cancelled).await }
    });
    let third = tokio::spawn({
        let runner = runner.clone();
        let cancelled = runner.begin_one_shot(uuid::Uuid::from_u128(3), NodeCounter(2));
        async move { runner.one_shot(quick_action(3), cancelled).await }
    });
    let deadline = tokio::time::Instant::now() + wait();
    while lock(&runner.inner.one_shots).len() < 3 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the runs never started"
        );
        tokio::task::yield_now().await;
    }
    runner.cancel_one_shot(uuid::Uuid::from_u128(1));
    let reply = first.await.unwrap();
    assert_eq!(reply.outcome, OneShotOutcome::Cancelled);
    assert_eq!(reply.request_id, uuid::Uuid::from_u128(1));
    runner.cancel_one_shot(uuid::Uuid::from_u128(9));
    // The end of generation 1's link ends its runs only; generation 2's
    // (a newer link of the same backend) keeps running.
    runner.cancel_binding_one_shots(NodeCounter(1));
    assert_eq!(second.await.unwrap().outcome, OneShotOutcome::Cancelled);
    assert!(
        !third.is_finished(),
        "a newer link's quick action was cancelled"
    );
    assert_eq!(lock(&runner.inner.one_shots).len(), 1);
    runner.cancel_binding_one_shots(NodeCounter(2));
    assert_eq!(third.await.unwrap().outcome, OneShotOutcome::Cancelled);
    assert!(lock(&runner.inner.one_shots).is_empty());
}
