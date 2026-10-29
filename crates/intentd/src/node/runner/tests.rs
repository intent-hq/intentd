//! Runner behavior against real provider processes: the `node-test-agent.mjs`
//! fixture speaking ACP over stdio. Skips when `node` is not on PATH.

use std::path::Path;
use std::time::Duration;

use serde_json::json;

use super::*;
use crate::node::storage::Storage;

const PROVIDER: &str = "mock";

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(20))
}

struct Harness {
    runner: Runner,
    storage: Arc<Storage>,
    checkouts: Arc<crate::node::checkout::PlainCheckouts>,
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
    let storage = Arc::new(Storage::open(&dir.path().join("node")).unwrap());
    let journal = Journal::open(&storage.journal_dir()).unwrap();
    let checkouts = crate::node::checkout::PlainCheckouts::new(storage.clone());
    let runner = Runner::new(RunnerConfig {
        max_agents,
        policy: PermissionPolicy::AllowAll,
        providers: BTreeMap::from([(
            PROVIDER.to_owned(),
            ProviderLaunch {
                config,
                binary: node,
            },
        )]),
        checkouts: checkouts.clone(),
        journal,
        mcp_bridge_exe: std::env::current_exe().unwrap(),
    });
    Some(Harness {
        runner,
        storage,
        checkouts,
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

    h.runner.stop(&AgentId::from("a1"));
    wait_for(&h.runner, "first exit", exited("a1")).await;
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
    assert!(error.contains("configured: mock"), "{error}");
    assert!(h.runner.running().is_empty());
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
        JournalBody::Exited { exit_code, error } => assert!(
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
        JournalBody::Exited { exit_code, error } => {
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

/// The text of the turn's `wsapi` line (the fixture's workspace_api result).
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
