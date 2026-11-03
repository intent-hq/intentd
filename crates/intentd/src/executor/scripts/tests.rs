//! Machine runs of saved scripts: each `scriptStart` is one process in the
//! agent's checkout, answered by `scriptStarted`, its output and one
//! `scriptExited`; every `scriptStop` is answered by a `scriptExited`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_core::node_link::{
    LinkFrame, ScriptEnvVar, ScriptExited, ScriptProcessRef, ScriptStartCommand,
};
use intent_core::nodes::NodeCounter;
use intent_core::{AgentId, ScriptMode};
use intent_transport::node_link::{DialedLink, LinkEvent};

use super::*;

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(20))
}

fn start(agent: &AgentId, spawn: &str, command: &str) -> ScriptStartCommand {
    ScriptStartCommand {
        agent_id: agent.clone(),
        script_id: "s1".into(),
        spawn_id: spawn.into(),
        command: command.into(),
        mode: ScriptMode::Command,
        cwd: None,
        env: vec![],
    }
}

fn process(agent: &AgentId, spawn: &str) -> ScriptProcessRef {
    ScriptProcessRef {
        agent_id: agent.clone(),
        script_id: "s1".into(),
        spawn_id: spawn.into(),
    }
}

/// What the backend saw for one process until its `scriptExited`.
#[derive(Debug, Default)]
struct Seen {
    pid: Option<u32>,
    output: String,
    seqs: Vec<u64>,
    exited: Option<ScriptExited>,
}

async fn until_exited(head: &mut DialedLink, spawn: &str) -> Seen {
    let mut seen = Seen::default();
    loop {
        let event = tokio::time::timeout(wait(), head.events.recv())
            .await
            .expect("timed out waiting for scriptExited")
            .expect("link closed");
        match event {
            LinkEvent::Frame(LinkFrame::ScriptStarted(s)) if s.spawn_id == spawn => {
                seen.pid = Some(s.pid);
            }
            LinkEvent::Frame(LinkFrame::ScriptOutput(o)) if o.spawn_id == spawn => {
                seen.seqs.push(o.seq);
                seen.output.push_str(&o.data);
            }
            LinkEvent::Frame(LinkFrame::ScriptExited(e)) if e.spawn_id == spawn => {
                seen.exited = Some(e);
                return seen;
            }
            _ => {}
        }
    }
}

struct Fixture {
    scripts: ScriptProcesses,
    link: intent_transport::node_link::NodeLink,
    head: DialedLink,
    checkout: PathBuf,
    _served: crate::executor::test_link::Served,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let checkout = dir.path().join("checkout");
    std::fs::create_dir_all(checkout.join("sub")).unwrap();
    let (link, head, served) = crate::executor::test_link::bound_link(dir.path()).await;
    Fixture {
        scripts: ScriptProcesses::new(std::sync::Arc::new(intent_pty::PtyHost::new())),
        link,
        head,
        checkout: checkout.canonicalize().unwrap(),
        _served: served,
        _dir: dir,
    }
}

const GEN: NodeCounter = NodeCounter(1);

fn checkout(f: &Fixture) -> &Path {
    f.checkout.as_path()
}

#[tokio::test]
async fn a_start_runs_in_the_agent_checkout_with_the_saved_env_and_reports_its_exit() {
    let mut f = fixture().await;
    let agent = AgentId::new();
    let mut cmd = start(&agent, "p1", "pwd; echo \"v=$GREETING\"; exit 3");
    cmd.cwd = Some("sub".into());
    cmd.env = vec![ScriptEnvVar {
        key: "GREETING".into(),
        value: "hello".into(),
    }];
    f.scripts.start(&f.link, GEN, Some(checkout(&f)), cmd).await;
    let seen = until_exited(&mut f.head, "p1").await;
    assert!(seen.pid.is_some(), "no scriptStarted: {seen:?}");
    assert!(
        seen.output
            .contains(&f.checkout.join("sub").display().to_string()),
        "{seen:?}"
    );
    assert!(seen.output.contains("v=hello"), "{seen:?}");
    let expected: Vec<u64> = (0..seen.seqs.len() as u64).collect();
    assert_eq!(seen.seqs, expected, "seq counts from 0 without gaps");
    let exited = seen.exited.unwrap();
    assert_eq!(exited.exit_code, Some(3));
    assert_eq!(exited.error, None);
    assert_eq!(exited.agent_id, agent);
    assert_eq!(exited.script_id, "s1");
}

#[tokio::test]
async fn a_start_without_a_checkout_is_refused_as_agent_unknown() {
    let mut f = fixture().await;
    let agent = AgentId::new();
    f.scripts
        .start(&f.link, GEN, None, start(&agent, "p1", "echo hi"))
        .await;
    let seen = until_exited(&mut f.head, "p1").await;
    assert_eq!(seen.pid, None);
    let error = seen.exited.unwrap().error.unwrap();
    assert!(error.starts_with("node-script-agent-unknown: "), "{error}");
}

#[tokio::test]
async fn a_cwd_outside_the_checkout_is_refused() {
    for cwd in ["../elsewhere", "/tmp", "sub/../../x"] {
        let mut f = fixture().await;
        let agent = AgentId::new();
        let mut cmd = start(&agent, "p1", "echo hi");
        cmd.cwd = Some(cwd.into());
        f.scripts.start(&f.link, GEN, Some(checkout(&f)), cmd).await;
        let seen = until_exited(&mut f.head, "p1").await;
        assert_eq!(seen.pid, None, "{cwd}");
        let error = seen.exited.unwrap().error.unwrap();
        assert!(
            error.starts_with("node-script-invalid-cwd: "),
            "{cwd}: {error}"
        );
    }
}

#[tokio::test]
async fn a_stop_kills_the_process_and_is_acknowledged_by_its_exit() {
    let mut f = fixture().await;
    let agent = AgentId::new();
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(&agent, "p1", "echo up; sleep 60"),
        )
        .await;
    loop {
        match tokio::time::timeout(wait(), f.head.events.recv())
            .await
            .unwrap()
        {
            Some(LinkEvent::Frame(LinkFrame::ScriptOutput(o))) if o.data.contains("up") => break,
            Some(_) => {}
            None => panic!("link closed"),
        }
    }
    f.scripts.stop(&f.link, &process(&agent, "p1"));
    let seen = until_exited(&mut f.head, "p1").await;
    let exited = seen.exited.unwrap();
    assert!(exited.exit_code.is_some(), "{exited:?}");
    assert!(
        !exited
            .error
            .as_deref()
            .unwrap_or_default()
            .starts_with("node-script-not-running"),
        "{exited:?}"
    );
    // A second stop of the same process names nothing running.
    f.scripts.stop(&f.link, &process(&agent, "p1"));
    let seen = until_exited(&mut f.head, "p1").await;
    let exited = seen.exited.unwrap();
    assert_eq!(exited.exit_code, Some(-1));
    assert!(exited
        .error
        .unwrap()
        .starts_with("node-script-not-running: "));
}

#[tokio::test]
async fn a_stop_for_an_unknown_process_is_answered_not_running() {
    let mut f = fixture().await;
    f.scripts.stop(&f.link, &process(&AgentId::new(), "never"));
    let seen = until_exited(&mut f.head, "never").await;
    let exited = seen.exited.unwrap();
    assert_eq!(exited.exit_code, Some(-1));
    assert!(exited
        .error
        .unwrap()
        .starts_with("node-script-not-running: "));
}

fn pid_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
        || std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .is_ok_and(|s| s.success())
}

async fn started_pid(head: &mut DialedLink, spawn: &str) -> u32 {
    loop {
        match tokio::time::timeout(wait(), head.events.recv())
            .await
            .unwrap()
        {
            Some(LinkEvent::Frame(LinkFrame::ScriptStarted(s))) if s.spawn_id == spawn => {
                return s.pid
            }
            Some(_) => {}
            None => panic!("link closed"),
        }
    }
}

async fn gone(pid: u32) {
    tokio::time::timeout(wait(), async {
        while pid_alive(pid) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("process {pid} still running"));
}

#[tokio::test]
async fn ending_a_binding_or_an_agent_kills_only_its_scripts() {
    let mut f = fixture().await;
    let a = AgentId::new();
    let b = AgentId::new();
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(&a, "pa", "sleep 60"),
        )
        .await;
    let pa = started_pid(&mut f.head, "pa").await;
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(&b, "pb", "sleep 60"),
        )
        .await;
    let pb = started_pid(&mut f.head, "pb").await;
    f.scripts
        .start(
            &f.link,
            NodeCounter(2),
            Some(checkout(&f)),
            start(&b, "pc", "sleep 60"),
        )
        .await;
    let pc = started_pid(&mut f.head, "pc").await;

    f.scripts.stop_agent(&a).await;
    gone(pa).await;
    assert!(pid_alive(pb) && pid_alive(pc));

    f.scripts.stop_binding(GEN).await;
    gone(pb).await;
    assert!(pid_alive(pc), "another binding's script was killed");
    f.scripts.stop_binding(NodeCounter(2)).await;
    gone(pc).await;
}

/// When output outpaces the link the machine drops it, and the drop is
/// always reported before the process's `scriptExited`.
#[tokio::test]
async fn dropped_output_is_reported_before_the_exit() {
    let mut f = fixture().await;
    let agent = AgentId::new();
    // Far more than the 4 MiB queue, while the backend reads nothing.
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(
                &agent,
                "p1",
                "head -c 24000000 /dev/zero | tr '\\0' a; printf 'tail\\303'",
            ),
        )
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let seen = until_exited(&mut f.head, "p1").await;
    let marker = seen
        .output
        .rfind("bytes of output dropped]")
        .unwrap_or_else(|| panic!("no dropped-output marker in {} bytes", seen.output.len()));
    assert!(marker > 0);
    assert_eq!(seen.exited.unwrap().exit_code, Some(0));
    let expected: Vec<u64> = (0..seen.seqs.len() as u64).collect();
    assert_eq!(seen.seqs, expected, "seq continues without a gap");
}

#[tokio::test]
async fn an_unfinished_trailing_character_is_sent_as_a_replacement() {
    let mut f = fixture().await;
    let agent = AgentId::new();
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(&agent, "p1", "printf 'ok\\303'"),
        )
        .await;
    let seen = until_exited(&mut f.head, "p1").await;
    assert!(seen.output.ends_with("ok\u{fffd}"), "{:?}", seen.output);
}

/// Review of intentd#3004: a descendant that outlives the direct child and
/// keeps the terminal open neither delays the exit report nor survives it.
#[tokio::test]
async fn a_descendant_holding_the_terminal_does_not_hold_up_the_exit() {
    let mut f = fixture().await;
    let agent = AgentId::new();
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(
                &agent,
                "p1",
                "(trap '' HUP; exec sleep 120) & echo \"child=$!\"; exit 3",
            ),
        )
        .await;
    let seen = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        until_exited(&mut f.head, "p1"),
    )
    .await
    .expect("the exit is reported while the descendant still holds the terminal");
    assert_eq!(seen.exited.unwrap().exit_code, Some(3));
    let child: u32 = seen
        .output
        .split("child=")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|pid| pid.parse().ok())
        .unwrap_or_else(|| panic!("no child pid in {:?}", seen.output));
    gone(child).await;
}

/// A process an ordinary `scriptStop` is already tearing down still holds
/// up `stop_agent` (what a final stop runs before it pushes and removes the
/// checkout): the second kill finds the terminal already taken, so
/// `stop_agent` must wait for the process's own reaper to record its exit,
/// not return at once.
#[tokio::test]
async fn stop_agent_waits_for_a_teardown_a_script_stop_already_started() {
    let mut f = fixture().await;
    let a = AgentId::from("a-stop");
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(&a, "p1", "sleep 60"),
        )
        .await;
    let pid = started_pid(&mut f.head, "p1").await;
    f.scripts.stop(&f.link, &process(&a, "p1"));
    // Let the ordinary stop's kill start (and take the terminal) first.
    tokio::task::yield_now().await;
    tokio::time::timeout(wait(), f.scripts.stop_agent(&a))
        .await
        .unwrap();
    assert!(
        !f.scripts.is_running(&process(&a, "p1")),
        "stop_agent returned before the process's exit was recorded"
    );
    assert!(
        !pid_alive(pid),
        "stop_agent returned while the script still ran"
    );
    let seen = until_exited(&mut f.head, "p1").await;
    assert!(seen.exited.is_some());
}

/// A process whose reaper never finishes (a child stuck in the kernel, whose
/// `wait` never returns) does not hold `stop_agent` or `stop_all` past the
/// reap bound: they log and return, and `stop_agent` reports it.
#[tokio::test]
async fn a_stuck_process_does_not_hold_up_stop_agent_or_stop_all() {
    let mut f = fixture().await;
    let a = AgentId::from("a-stuck-reap");
    hold_reaps_for_test(&a);
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(&a, "p1", "sleep 60"),
        )
        .await;
    started_pid(&mut f.head, "p1").await;
    let bound = reap_wait() + Duration::from_secs(3);
    let t0 = std::time::Instant::now();
    let reaped = tokio::time::timeout(bound, f.scripts.stop_agent(&a))
        .await
        .expect("stop_agent waited past the reap bound");
    assert!(
        !reaped,
        "a process that was never reaped was reported reaped"
    );
    assert!(t0.elapsed() >= reap_wait(), "gave up before the bound");
    tokio::time::timeout(bound, f.scripts.stop_all())
        .await
        .expect("stop_all waited past the reap bound");
}

/// The whole process group, not only the shell, is gone when `stop_agent`
/// returns after an ordinary `scriptStop` started the kill: the shell exits
/// on SIGTERM at once while a descendant that ignores it lives until the
/// original kill's SIGKILL, and `stop_agent` must wait for that kill.
#[tokio::test]
async fn stop_agent_waits_for_a_script_stops_whole_group_teardown() {
    let mut f = fixture().await;
    let a = AgentId::from("a-stop-group");
    f.scripts
        .start(
            &f.link,
            GEN,
            Some(checkout(&f)),
            start(
                &a,
                "p1",
                r#"exec /bin/sh -c 'sh -c "trap \"\" TERM HUP; echo trapped-\$\$; while :; do sleep 1; done" & sleep 300'"#,
            ),
        )
        .await;
    started_pid(&mut f.head, "p1").await;
    let mut output = String::new();
    let descendant: u32 = loop {
        match tokio::time::timeout(wait(), f.head.events.recv())
            .await
            .expect("the descendant never printed its pid")
            .expect("link closed")
        {
            LinkEvent::Frame(LinkFrame::ScriptOutput(o)) if o.spawn_id == "p1" => {
                output.push_str(&o.data);
                if let Some(pid) = output
                    .split_whitespace()
                    .find_map(|t| t.strip_prefix("trapped-").and_then(|p| p.parse().ok()))
                {
                    break pid;
                }
            }
            LinkEvent::Frame(LinkFrame::ScriptExited(e)) if e.spawn_id == "p1" => {
                panic!("exited early: {e:?} after {output:?}")
            }
            _ => {}
        }
    };
    assert!(pid_alive(descendant));
    f.scripts.stop(&f.link, &process(&a, "p1"));
    tokio::task::yield_now().await;
    tokio::time::timeout(wait(), f.scripts.stop_agent(&a))
        .await
        .unwrap();
    assert!(
        !pid_alive(descendant),
        "stop_agent returned while the script's trapped descendant still ran"
    );
}
