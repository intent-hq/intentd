//! Machine runs: a remote agent's run of a saved script on its machine,
//! driven over the executor link's script frames.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::node_link::{
    LinkFrame, ScriptExited, ScriptOutput, ScriptStartCommand, ScriptStarted,
};
use intent_core::{AgentId, Script, ScriptMode, ScriptPurpose, ScriptStatus, WorkspaceId};
use intent_services::feature_api::feature_ports::MachineScriptLink;

use super::{Hooks, MachineRuns, RunPlace};

#[derive(Default)]
struct Recorded {
    frames: Mutex<Vec<LinkFrame>>,
    changed: Mutex<Vec<String>>,
    wakes: Mutex<Vec<(AgentId, String)>>,
    unreachable: std::sync::atomic::AtomicBool,
    protocol: std::sync::atomic::AtomicU32,
}

fn hooks(rec: &Arc<Recorded>) -> Hooks {
    let links = rec.clone();
    let changed = rec.clone();
    let wakes = rec.clone();
    Hooks {
        link: Arc::new(move |_agent| {
            let rec = links.clone();
            Box::pin(async move {
                // A real link lookup awaits the connector: let racing
                // callers interleave here.
                tokio::task::yield_now().await;
                if rec.unreachable.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(intent_core::Error::Internal("machine-unreachable".into()));
                }
                let sender = rec.clone();
                let protocol = match rec.protocol.load(std::sync::atomic::Ordering::SeqCst) {
                    0 => 8,
                    p => p,
                };
                Ok(MachineScriptLink {
                    node_id: "mac".into(),
                    protocol,
                    machine_name: "Mac".into(),
                    machine_version: "0.11.0".into(),
                    send: Arc::new(move |frame| {
                        sender.frames.lock().unwrap().push(frame);
                        Box::pin(async { Ok(()) })
                    }),
                })
            })
        }),
        changed: Arc::new(move |_ws, script| {
            changed.changed.lock().unwrap().push(script);
            Box::pin(async {})
        }),
        wake: Arc::new(move |_ws, agent, content| {
            wakes.wakes.lock().unwrap().push((agent, content));
            Box::pin(async {})
        }),
        restart_backoff: Duration::from_millis(20),
        stop_wait: Duration::from_millis(500),
    }
}

fn ws() -> WorkspaceId {
    WorkspaceId::from("ws-1")
}

fn script(mode: ScriptMode, command: &str) -> Script {
    Script {
        purpose: ScriptPurpose::Saved,
        archived_at: None,
        last_run: None,
        id: "s1".into(),
        workspace_id: "ws-1".into(),
        name: "build".into(),
        command: command.into(),
        cwd: Some("web".into()),
        env: Some([("A".to_string(), "1".to_string())].into()),
        mode,
        category: None,
        source: "user".into(),
        auto_start: None,
        created_at: "t".into(),
        updated_at: None,
        remote_author: None,
    }
}

fn place(agent: &AgentId) -> RunPlace {
    RunPlace {
        workspace_id: ws(),
        script_id: "s1".into(),
        agent_id: agent.clone(),
    }
}

fn last_start(rec: &Recorded) -> ScriptStartCommand {
    rec.frames
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find_map(|f| match f {
            LinkFrame::ScriptStart(s) => Some(s.clone()),
            _ => None,
        })
        .expect("a scriptStart was sent")
}

async fn frame(runs: &MachineRuns, frame: LinkFrame) {
    runs.on_frame("mac", frame).await;
}

fn started(start: &ScriptStartCommand) -> LinkFrame {
    LinkFrame::ScriptStarted(ScriptStarted {
        agent_id: start.agent_id.clone(),
        script_id: start.script_id.clone(),
        spawn_id: start.spawn_id.clone(),
        pid: 42,
    })
}

fn output(start: &ScriptStartCommand, seq: u64, data: &str) -> LinkFrame {
    LinkFrame::ScriptOutput(ScriptOutput {
        agent_id: start.agent_id.clone(),
        script_id: start.script_id.clone(),
        spawn_id: start.spawn_id.clone(),
        seq,
        data: data.into(),
    })
}

fn exited(start: &ScriptStartCommand, code: Option<i32>, error: Option<&str>) -> LinkFrame {
    LinkFrame::ScriptExited(ScriptExited {
        agent_id: start.agent_id.clone(),
        script_id: start.script_id.clone(),
        spawn_id: start.spawn_id.clone(),
        exit_code: code,
        error: error.map(str::to_owned),
    })
}

#[tokio::test]
async fn a_command_run_starts_on_the_machine_and_settles_from_its_frames() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    let state = runs
        .start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    assert_eq!(state.status, ScriptStatus::Starting);
    let start = last_start(&rec);
    assert_eq!(start.agent_id, agent);
    assert_eq!(start.command, "make");
    assert_eq!(start.cwd.as_deref(), Some("web"));
    assert_eq!(start.env.len(), 1);
    assert_ne!(
        Some(start.spawn_id.clone()),
        state.run_id,
        "spawnId is not the runId"
    );

    frame(&runs, started(&start)).await;
    assert_eq!(
        runs.state(&place(&agent)).unwrap().status,
        ScriptStatus::Running
    );
    assert_eq!(runs.state(&place(&agent)).unwrap().pid, Some(42));
    frame(&runs, output(&start, 0, "hello\n")).await;
    frame(&runs, exited(&start, Some(2), None)).await;
    let state = runs.state(&place(&agent)).unwrap();
    assert_eq!(state.status, ScriptStatus::Exited);
    assert_eq!(state.exit_code, Some(2));
    let entries = runs.entries(&ws(), "s1");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["remoteAgentId"], agent.as_str());
    assert_eq!(entries[0]["nodeId"], "mac");
    assert_eq!(entries[0]["runtime"]["status"], "exited");
    assert_eq!(entries[0]["lastRun"]["outcome"], "failed");
    assert!(runs.output(&place(&agent)).unwrap().contains("hello"));
    assert!(
        rec.changed.lock().unwrap().len() >= 3,
        "each transition is published"
    );
}

#[tokio::test]
async fn frames_of_another_spawn_or_machine_are_ignored() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    let start = last_start(&rec);
    let mut stale = start.clone();
    stale.spawn_id = "old".into();
    frame(&runs, exited(&stale, Some(0), None)).await;
    runs.on_frame("other-machine", exited(&start, Some(0), None))
        .await;
    assert_eq!(
        runs.state(&place(&agent)).unwrap().status,
        ScriptStatus::Starting
    );
}

#[tokio::test]
async fn a_service_that_exits_is_restarted_with_a_new_spawn_and_the_same_run() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    let first = runs
        .start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, started(&start)).await;
    frame(&runs, exited(&start, Some(1), None)).await;
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        async {
            while last_start(&rec).spawn_id == start.spawn_id {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("restarted");
    let state = runs.state(&place(&agent)).unwrap();
    assert_eq!(state.run_id, first.run_id, "the runId spans restarts");
    assert_eq!(state.restart_count, 1);
}

#[tokio::test]
async fn stop_waits_for_the_ack_and_a_second_stop_dismisses_the_run() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, started(&start)).await;
    let acker = {
        let runs = runs.clone();
        let rec = rec.clone();
        let start = start.clone();
        tokio::spawn(async move {
            loop {
                if rec
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|f| matches!(f, LinkFrame::ScriptStop(_)))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            runs.on_frame("mac", exited(&start, Some(143), None)).await;
        })
    };
    let stopped = runs.stop(&place(&agent)).await.unwrap();
    acker.await.unwrap();
    assert_eq!(stopped.status, ScriptStatus::Idle);
    assert_eq!(
        runs.entries(&ws(), "s1").len(),
        1,
        "the stopped run is still listed"
    );
    runs.stop(&place(&agent)).await.unwrap();
    assert!(
        runs.entries(&ws(), "s1").is_empty(),
        "a stop of a stopped run dismisses it"
    );
}

#[tokio::test]
async fn a_lost_link_settles_live_runs_and_monitors_wake_their_agent() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, started(&start)).await;
    let monitor = runs
        .monitor(&place(&agent), &serde_json::json!({ "ttlMs": 60000 }))
        .unwrap();
    assert_eq!(monitor["monitor"]["remoteAgentId"], agent.as_str());
    assert_eq!(monitor["monitor"]["nodeId"], "mac");
    assert!(runs.live_on("mac"));
    runs.link_lost("mac", "remote-script-lost: the machine disconnected")
        .await;
    assert!(!runs.live_on("mac"));
    let state = runs.state(&place(&agent)).unwrap();
    assert_eq!(state.status, ScriptStatus::Exited);
    assert_eq!(state.exit_code, Some(-1));
    assert_eq!(
        state.error.as_deref(),
        Some("remote-script-lost: the machine disconnected")
    );
    assert_eq!(
        runs.entries(&ws(), "s1")[0]["lastRun"]["outcome"],
        "interrupted"
    );
    let wakes = rec.wakes.lock().unwrap();
    assert_eq!(wakes.len(), 1);
    assert_eq!(wakes[0].0, agent);
    assert!(wakes[0].1.contains("remote-script-lost"), "{}", wakes[0].1);
}

#[tokio::test]
async fn an_output_pattern_monitor_wakes_once_on_a_matching_line() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    runs.monitor(
        &place(&agent),
        &serde_json::json!({ "ttlMs": 60000, "outputPattern": "ready on \\d+" }),
    )
    .unwrap();
    frame(&runs, output(&start, 0, "booting\nready on 3000\n")).await;
    frame(&runs, output(&start, 1, "ready on 3001\n")).await;
    let wakes = rec.wakes.lock().unwrap();
    assert_eq!(wakes.len(), 1, "{wakes:?}");
    assert!(wakes[0].1.contains("ready on 3000"), "{}", wakes[0].1);
}

#[tokio::test]
async fn ending_an_agent_stops_its_runs_and_reports_unacknowledged_stops() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    runs.monitor(&place(&agent), &serde_json::json!({ "ttlMs": 60000 }))
        .unwrap();
    let deadline = tokio::time::Instant::now()
        + intentd_test_support::test_timeout(Duration::from_millis(100));
    let acked = runs
        .end_agent(
            &agent,
            deadline,
            "remote-script-lost: the machine was removed",
        )
        .await;
    assert!(!acked, "nobody acknowledged the stop");
    // Nobody saw the run end: it is settled as lost with the lease end's
    // reason (an ordinary stop would end it idle and wake nobody).
    {
        let wakes = rec.wakes.lock().unwrap();
        assert_eq!(wakes.len(), 1, "{wakes:?}");
        assert!(
            wakes[0]
                .1
                .contains("exited with code -1: remote-script-lost: the machine was removed"),
            "{}",
            wakes[0].1
        );
    }
    assert!(rec
        .frames
        .lock()
        .unwrap()
        .iter()
        .any(|f| matches!(f, LinkFrame::ScriptStop(_))));
    assert!(
        runs.entries(&ws(), "s1").is_empty(),
        "its entries are dropped"
    );
    // No live run: nothing to wait for.
    assert!(runs.end_agent(&agent, deadline, "x").await);
}

#[tokio::test]
async fn changing_the_definition_cancels_and_drops_every_machine_run() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let a = AgentId::new();
    let b = AgentId::new();
    runs.start(&place(&a), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    runs.start(&place(&b), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    assert_eq!(runs.entries(&ws(), "s1").len(), 2);
    runs.cancel_script(&ws(), "s1", "script replaced").await;
    assert!(runs.entries(&ws(), "s1").is_empty());
    let stops = rec
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|f| matches!(f, LinkFrame::ScriptStop(_)))
        .count();
    assert_eq!(stops, 2);
}

#[tokio::test]
async fn start_fails_when_the_machine_is_unreachable_or_too_old_for_a_restart() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    rec.unreachable
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let error = runs
        .start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("machine-unreachable"), "{error}");
    assert!(runs.entries(&ws(), "s1").is_empty());

    rec.unreachable
        .store(false, std::sync::atomic::Ordering::SeqCst);
    runs.start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, exited(&start, Some(0), None)).await;
    rec.protocol.store(7, std::sync::atomic::Ordering::SeqCst);
    let error = runs
        .restart(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("machine-update-required"),
        "{error}"
    );
    // A protocol 9 link still runs scripts.
    rec.protocol.store(9, std::sync::atomic::Ordering::SeqCst);
    runs.restart(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
}

/// Review of intentd#3005: two starts of the same place racing across the
/// link lookup send one `scriptStart` and track one process.
#[tokio::test]
async fn concurrent_starts_of_one_place_send_one_script_start() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    let def = script(ScriptMode::Service, "serve");
    let here = place(&agent);
    let (a, b) = tokio::join!(runs.start(&here, &def), runs.start(&here, &def));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.run_id, b.run_id, "both callers see the same run");
    let starts = rec
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|f| matches!(f, LinkFrame::ScriptStart(_)))
        .count();
    assert_eq!(starts, 1);
}

/// Review of intentd#3005: a lease end for several agents sends every
/// script stop first and waits once, so an agent that never acknowledges
/// does not cost another agent its acknowledgement.
#[tokio::test]
async fn ending_several_agents_waits_once_for_all_their_stops() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let silent = AgentId::new();
    let prompt = AgentId::new();
    runs.start(&place(&silent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    runs.start(&place(&prompt), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    // The machine acknowledges `prompt`'s stop as soon as it is sent.
    let acker = {
        let runs = runs.clone();
        let rec = rec.clone();
        let prompt = prompt.clone();
        tokio::spawn(async move {
            loop {
                let stop = rec.frames.lock().unwrap().iter().find_map(|f| match f {
                    LinkFrame::ScriptStop(s) if s.agent_id == prompt => Some(s.clone()),
                    _ => None,
                });
                if let Some(stop) = stop {
                    runs.on_frame(
                        "mac",
                        LinkFrame::ScriptExited(ScriptExited {
                            agent_id: stop.agent_id,
                            script_id: stop.script_id,
                            spawn_id: stop.spawn_id,
                            exit_code: Some(143),
                            error: None,
                        }),
                    )
                    .await;
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let deadline = tokio::time::Instant::now()
        + intentd_test_support::test_timeout(Duration::from_millis(300));
    let unacked = runs
        .end_agents(
            &[silent.clone(), prompt.clone()],
            deadline,
            "remote-script-lost: the machine was removed",
        )
        .await;
    acker.await.unwrap();
    assert_eq!(unacked, vec![silent]);
    assert!(runs.entries(&ws(), "s1").is_empty());
}

/// The bytes `value` takes on the wire.
fn wire_len(value: &serde_json::Value) -> usize {
    serde_json::to_string(value).unwrap().len()
}

/// Review of intentd#3005: a machine run's output replies follow the
/// "Bounded output reads" contract like the home run's: `script.output`
/// fits 480,000 JSON-escaped bytes under a `[showing last …]` header, and
/// `script.run` keeps the newest bytes with `outputTruncated: true`.
#[tokio::test]
async fn machine_run_output_replies_are_bounded() {
    // ASCII, and output that doubles when JSON-escaped.
    for (label, line) in [("ascii", "x".repeat(99)), ("escaped", "\"".repeat(99))] {
        let rec = Arc::new(Recorded::default());
        let runs = MachineRuns::new(hooks(&rec));
        let agent = AgentId::new();
        let def = script(ScriptMode::Command, "make");
        let here = place(&agent);
        let waiter = {
            let runs = runs.clone();
            let here = here.clone();
            let def = def.clone();
            tokio::spawn(async move {
                runs.run(
                    &here,
                    &def,
                    intentd_test_support::test_timeout(Duration::from_secs(10)),
                    10_000,
                )
                .await
            })
        };
        let start = loop {
            if let Some(start) = rec.frames.lock().unwrap().iter().find_map(|f| match f {
                LinkFrame::ScriptStart(s) => Some(s.clone()),
                _ => None,
            }) {
                break start;
            }
            tokio::task::yield_now().await;
        };
        // 5,000 lines of 100 bytes: within the 512 KiB the run keeps, over
        // the 480,000-byte reply budget (twice over once escaped).
        let chunk = format!("{line}\n").repeat(1000);
        for seq in 0..5 {
            frame(&runs, output(&start, seq, &chunk)).await;
        }
        frame(&runs, exited(&start, Some(0), None)).await;

        let reply = waiter.await.unwrap().unwrap();
        // The budget covers the output text; the reply's other members and
        // `script.output`'s header line ride on top, as for the home run.
        assert!(
            wire_len(&reply) <= 480_000 + 256,
            "{label}: script.run reply is {} bytes",
            wire_len(&reply)
        );
        assert_eq!(reply["outputTruncated"], true, "{label}");

        let text = runs.output_reply(&here, 10_000).unwrap();
        assert!(
            wire_len(&text) <= 480_000 + 256,
            "{label}: script.output is {} bytes",
            wire_len(&text)
        );
        let text = text.as_str().unwrap();
        assert!(
            text.starts_with("[showing last "),
            "{label}: {}",
            &text[..80]
        );
        assert!(text.contains("older output omitted"), "{label}");
    }
}

/// Review of intentd#3005: output without a newline (a progress bar, `yes |
/// tr -d '\n'`) keeps the run's output bounded instead of growing an
/// unfinished line without limit.
#[tokio::test]
async fn output_without_newlines_stays_bounded() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    let chunk = "y".repeat(64 * 1024);
    for seq in 0..20 {
        frame(&runs, output(&start, seq, &chunk)).await;
    }
    let kept = runs.output(&place(&agent)).unwrap();
    assert!(
        kept.len() <= 512 * 1024 + 64 * 1024,
        "kept {} bytes of 1.25 MiB without a newline",
        kept.len()
    );
}

/// Review of intentd#3005: monitors match lines as the home run's do (one
/// line kept up to the shared decoder's bound, an oversized line counts but
/// cannot match), so a wake never carries a huge line.
#[tokio::test]
async fn a_monitor_wake_never_carries_a_huge_line() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    runs.monitor(
        &place(&agent),
        &serde_json::json!({ "ttlMs": 60000, "outputPattern": "needle" }),
    )
    .unwrap();
    let huge = format!("{}needle{}\n", "a".repeat(200_000), "b".repeat(200_000));
    frame(&runs, output(&start, 0, &huge)).await;
    frame(&runs, output(&start, 1, "short needle\n")).await;
    let wakes = rec.wakes.lock().unwrap();
    assert_eq!(wakes.len(), 1, "one wake");
    assert!(
        wakes[0].1.len() <= 4096 + 512,
        "wake text is {} bytes",
        wakes[0].1.len()
    );
    assert!(
        wakes[0].1.contains("short needle"),
        "{}",
        &wakes[0].1[..200.min(wakes[0].1.len())]
    );
}

/// Review of intentd#3005: like a home-run monitor, a machine run's monitor
/// checks the final line when the run exits without a trailing newline, so
/// `printf READY; exit` wakes with the match, not the exit.
#[tokio::test]
async fn a_final_line_without_a_newline_matches_before_the_exit() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    let start = last_start(&rec);
    runs.monitor(
        &place(&agent),
        &serde_json::json!({ "ttlMs": 60000, "outputPattern": "READY" }),
    )
    .unwrap();
    frame(&runs, output(&start, 0, "READY")).await;
    frame(&runs, exited(&start, Some(0), None)).await;
    let wakes = rec.wakes.lock().unwrap();
    assert_eq!(wakes.len(), 1, "{wakes:?}");
    assert!(wakes[0].1.contains("output matched"), "{}", wakes[0].1);
    assert!(wakes[0].1.contains("READY"), "{}", wakes[0].1);
}

/// Review of intentd#3005: the final-line check runs only on the process's
/// real exit. A line cut off by a lost link is a stream gap, not EOF: the
/// monitor wakes with `remote-script-lost`, never a match on the fragment.
#[tokio::test]
async fn a_line_cut_off_by_a_lost_link_does_not_match() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    runs.monitor(
        &place(&agent),
        &serde_json::json!({ "ttlMs": 60000, "outputPattern": "Listening" }),
    )
    .unwrap();
    frame(&runs, output(&start, 0, "Listening on port 30")).await;
    runs.link_lost("mac", "remote-script-lost: the machine disconnected")
        .await;
    let wakes = rec.wakes.lock().unwrap();
    assert_eq!(wakes.len(), 1, "{wakes:?}");
    assert!(wakes[0].1.contains("remote-script-lost"), "{}", wakes[0].1);
    assert!(!wakes[0].1.contains("output matched"), "{}", wakes[0].1);
}

fn script_starts(rec: &Recorded) -> usize {
    rec.frames
        .lock()
        .unwrap()
        .iter()
        .filter(|f| matches!(f, LinkFrame::ScriptStart(_)))
        .count()
}

/// Docs K (final stop): the machine stops an agent's scripts itself during
/// a final stop (a retire sends no lease-end `scriptStop`), so once a final
/// stop is under way a service's exit settles it instead of restarting it.
#[tokio::test]
async fn a_service_ended_by_a_final_stop_is_not_restarted() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, started(&start)).await;
    runs.final_stop_begun(&agent);
    frame(&runs, exited(&start, Some(143), None)).await;
    // timing-guard: longer than the 20 ms restart backoff, so a restart would have been sent
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(script_starts(&rec), 1, "no restart after a final stop");
    assert_ne!(
        runs.state(&place(&agent)).map(|s| s.status),
        Some(ScriptStatus::Restarting)
    );
}

/// Docs K (final stop): once the machine acknowledges an agent's final stop,
/// the backend settles that agent's machine runs it never saw end, as the
/// lease end settles them, without sending any frame.
#[tokio::test]
async fn a_final_stop_ack_settles_the_runs_never_seen_ending() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, started(&start)).await;
    runs.monitor(&place(&agent), &serde_json::json!({ "ttlMs": 60000 }))
        .unwrap();
    let frames_before = rec.frames.lock().unwrap().len();
    runs.final_stop_acked(&agent, "remote-script-lost: the agent was stopped for good")
        .await;
    assert_eq!(
        rec.frames.lock().unwrap().len(),
        frames_before,
        "no frame sent"
    );
    assert!(
        runs.entries(&ws(), "s1").is_empty(),
        "its entries are dropped"
    );
    let wakes = rec.wakes.lock().unwrap();
    assert_eq!(wakes.len(), 1, "{wakes:?}");
    assert!(wakes[0].1.contains("remote-script-lost"), "{}", wakes[0].1);
}

/// Review of intentd#3005: a final stop that begins while a service waits
/// out its restart backoff fences that respawn: no new process starts after
/// the machine's cleanup began.
#[tokio::test]
async fn a_final_stop_fences_a_pending_service_respawn() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Service, "serve"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, started(&start)).await;
    // The service exits on its own: a respawn is scheduled after the backoff.
    frame(&runs, exited(&start, Some(1), None)).await;
    assert_eq!(
        runs.state(&place(&agent)).map(|s| s.status),
        Some(ScriptStatus::Restarting)
    );
    runs.final_stop_begun(&agent);
    // timing-guard: longer than the 20 ms restart backoff, so the respawn has had its chance
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        script_starts(&rec),
        1,
        "no respawn after a final stop began"
    );
    assert_ne!(
        runs.state(&place(&agent)).map(|s| s.status),
        Some(ScriptStatus::Restarting)
    );
}

/// Review of intentd#3005: like the home run's, a monitor registered after
/// the run already finished is refused `run-finished` instead of returning
/// an active watch that could never wake.
#[tokio::test]
async fn a_monitor_on_a_finished_run_is_refused() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    let start = last_start(&rec);
    frame(&runs, exited(&start, Some(0), None)).await;
    let reply = runs
        .monitor(&place(&agent), &serde_json::json!({ "ttlMs": 60000 }))
        .unwrap();
    assert_eq!(reply["ok"], false, "{reply}");
    assert_eq!(reply["refused"], true, "{reply}");
    assert_eq!(reply["reason"], "run-finished", "{reply}");
    assert!(runs.monitors_of(&agent).is_empty());
}

/// Review of intentd#3005: stopping a run retires its monitor, and a
/// monitor only ever matches its own run's output, so a watch on run A
/// never wakes on run B after a restart.
#[tokio::test]
async fn a_stopped_runs_monitor_never_wakes_on_the_next_run() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    let def = script(ScriptMode::Service, "serve");
    runs.start(&place(&agent), &def).await.unwrap();
    let a = last_start(&rec);
    frame(&runs, started(&a)).await;
    let reply = runs
        .monitor(
            &place(&agent),
            &serde_json::json!({ "ttlMs": 60000, "outputPattern": "ready" }),
        )
        .unwrap();
    assert_eq!(reply["ok"], true, "{reply}");
    // An acknowledged stop of run A.
    let acker = {
        let runs = runs.clone();
        let rec = rec.clone();
        let a = a.clone();
        tokio::spawn(async move {
            while !rec
                .frames
                .lock()
                .unwrap()
                .iter()
                .any(|f| matches!(f, LinkFrame::ScriptStop(_)))
            {
                tokio::task::yield_now().await;
            }
            runs.on_frame("mac", exited(&a, Some(143), None)).await;
        })
    };
    runs.stop(&place(&agent)).await.unwrap();
    acker.await.unwrap();
    assert!(
        runs.monitors_of(&agent).is_empty(),
        "run A's monitor is retired"
    );
    // Run B prints what run A's monitor waited for.
    runs.start(&place(&agent), &def).await.unwrap();
    let b = last_start(&rec);
    assert_ne!(a.spawn_id, b.spawn_id);
    frame(&runs, started(&b)).await;
    frame(&runs, output(&b, 0, "ready\n")).await;
    // Run A's monitor settled cancelled at its stop; it never matches run B.
    let wakes = rec.wakes.lock().unwrap();
    assert_eq!(wakes.len(), 1, "{wakes:?}");
    assert!(
        wakes[0].1.contains("was cancelled"),
        "run A's watch woke on run B: {wakes:?}"
    );
}

/// Stop `place`'s live run, acknowledging the machine's stop at once.
async fn stop_acked(runs: &MachineRuns, rec: &Arc<Recorded>, here: &RunPlace) {
    let acker = {
        let runs = runs.clone();
        let rec = rec.clone();
        tokio::spawn(async move {
            loop {
                let stop = rec
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .rev()
                    .find_map(|f| match f {
                        LinkFrame::ScriptStop(s) => Some(s.clone()),
                        _ => None,
                    });
                if let Some(stop) = stop {
                    runs.on_frame(
                        "mac",
                        LinkFrame::ScriptExited(ScriptExited {
                            agent_id: stop.agent_id,
                            script_id: stop.script_id,
                            spawn_id: stop.spawn_id,
                            exit_code: Some(143),
                            error: None,
                        }),
                    )
                    .await;
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
    };
    runs.stop(here).await.unwrap();
    acker.await.unwrap();
}

/// Review of intentd#3005: as the home run's, a monitored run that is
/// stopped settles its monitor as cancelled and wakes its owner exactly
/// once; a manual restart does the same for the old run's monitor.
#[tokio::test]
async fn stopping_or_restarting_a_monitored_run_wakes_once_cancelled() {
    for restart in [false, true] {
        let rec = Arc::new(Recorded::default());
        let runs = MachineRuns::new(hooks(&rec));
        let agent = AgentId::new();
        let def = script(ScriptMode::Service, "serve");
        let here = place(&agent);
        runs.start(&here, &def).await.unwrap();
        frame(&runs, started(&last_start(&rec))).await;
        runs.monitor(&here, &serde_json::json!({ "ttlMs": 60000 }))
            .unwrap();
        if restart {
            let acker = {
                let runs = runs.clone();
                let rec = rec.clone();
                tokio::spawn(async move {
                    loop {
                        let stop = rec
                            .frames
                            .lock()
                            .unwrap()
                            .iter()
                            .rev()
                            .find_map(|f| match f {
                                LinkFrame::ScriptStop(s) => Some(s.clone()),
                                _ => None,
                            });
                        if let Some(stop) = stop {
                            runs.on_frame(
                                "mac",
                                LinkFrame::ScriptExited(ScriptExited {
                                    agent_id: stop.agent_id,
                                    script_id: stop.script_id,
                                    spawn_id: stop.spawn_id,
                                    exit_code: Some(143),
                                    error: None,
                                }),
                            )
                            .await;
                            return;
                        }
                        tokio::task::yield_now().await;
                    }
                })
            };
            runs.restart(&here, &def).await.unwrap();
            acker.await.unwrap();
        } else {
            stop_acked(&runs, &rec, &here).await;
        }
        let wakes = rec.wakes.lock().unwrap();
        assert_eq!(wakes.len(), 1, "restart={restart}: {wakes:?}");
        assert!(
            wakes[0].1.contains("cancelled"),
            "restart={restart}: {}",
            wakes[0].1
        );
    }
}

/// Review of intentd#3005: as on the home path, a `runId` that is not the
/// run's own is rejected `-32602` before the finished check.
#[tokio::test]
async fn a_stale_run_id_is_invalid_even_on_a_finished_run() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    runs.start(&place(&agent), &script(ScriptMode::Command, "make"))
        .await
        .unwrap();
    frame(&runs, exited(&last_start(&rec), Some(0), None)).await;
    let error = runs
        .monitor(
            &place(&agent),
            &serde_json::json!({ "ttlMs": 60000, "runId": "not-this-run" }),
        )
        .unwrap_err();
    assert!(
        matches!(error, intent_core::Error::InvalidParams(_)),
        "{error:?}"
    );
}

/// Review of intentd#3005: while an agent's final stop is under way no
/// start or restart of its machine runs sends a new process to the machine.
#[tokio::test]
async fn no_start_or_restart_while_a_final_stop_is_under_way() {
    let rec = Arc::new(Recorded::default());
    let runs = MachineRuns::new(hooks(&rec));
    let agent = AgentId::new();
    let def = script(ScriptMode::Service, "serve");
    runs.start(&place(&agent), &def).await.unwrap();
    frame(&runs, started(&last_start(&rec))).await;
    runs.final_stop_begun(&agent);
    for result in [
        runs.restart(&place(&agent), &def).await,
        runs.start(&place(&agent), &def).await,
    ] {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("remote-script-agent-ending"),
            "{error}"
        );
    }
    assert_eq!(script_starts(&rec), 1);
    runs.final_stop_acked(&agent, "remote-script-lost: the agent was stopped for good")
        .await;
    // The final stop is over: a later start (a reused id) is not refused.
    runs.start(&place(&agent), &def).await.unwrap();
    assert_eq!(script_starts(&rec), 2);
}
