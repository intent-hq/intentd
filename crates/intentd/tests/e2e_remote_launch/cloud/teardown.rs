//! The fake E2B leaves no sandbox daemon behind, however its test ends.
//!
//! A sandbox's bootstrap starts `intentd serve` detached (`setsid nohup ...
//! &`), so nothing in the test parents it: 119 of them once outlived their
//! tests on one host, for days, because the fake's teardown ran `intentd
//! stop` against a scratch dir that was already gone (or renamed to
//! `failed-*`), and never ran at all when nextest killed the test. Each
//! scenario runs [`fake_e2b_child`] as a separate test process that starts
//! two sandbox daemons (one paused, as E2B pauses a sandbox) and then
//! passes, fails or hangs until it is killed (`SIGKILL`); afterwards neither daemon
//! may still run.

use std::process::Stdio;
use std::time::Instant;

use intentd_test_support::is_running;
use nix::sys::signal::Signal;

use super::*;

/// `<mode>:<dir>` for [`fake_e2b_child`]; unset, it does nothing.
const CHILD_ENV: &str = "INTENTD_TEST_FAKE_E2B_CHILD";
const CHILD_TEST: &str = "e2e_remote_launch::cloud::teardown::fake_e2b_child";
/// The daemon pids the child reports, space separated.
const PIDS_FILE: &str = "pids";
/// Written once the scenario has seen the daemons run: the child ends then.
const GO_FILE: &str = "go";

/// How the bootstrap's `START_SCRIPT` starts the sandbox daemon
/// (`intent-svc-remote` `cloud/bootstrap.rs`): detached, in a new session
/// when `setsid` exists.
const LAUNCH: &str = r#"set -eu
export INTENTD_DATA_DIR="$HOME/.intent"
dir="$INTENTD_DATA_DIR"
if command -v setsid >/dev/null 2>&1; then
  setsid nohup intentd serve >"$dir/serve.log" 2>&1 </dev/null &
else
  nohup intentd serve >"$dir/serve.log" 2>&1 </dev/null &
fi
"#;

fn have_node_and_git() -> bool {
    intent_providers::resolve_on_path("node").is_some()
        && intent_providers::resolve_on_path("git").is_some()
}

/// Create a sandbox on `fake` and start its daemon the way the bootstrap
/// does; the sandbox id and the daemon's pid once it runs.
async fn start_sandbox_daemon(fake: &FakeE2b) -> (String, u32) {
    let keyed = HashMap::from([("x-api-key".to_owned(), E2B_KEY.to_owned())]);
    let create = json!({ "templateID": "intent/intent-node-4x8:0", "metadata": {} });
    let (status, _, reply) = fake
        .route(
            "POST",
            "/v2/sandboxes",
            &keyed,
            create.to_string().as_bytes(),
        )
        .await;
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&reply));
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    let id = reply["sandboxID"].as_str().unwrap().to_owned();
    let envd = HashMap::from([
        ("e2b-sandbox-id".to_owned(), id.clone()),
        (
            "x-access-token".to_owned(),
            reply["envdAccessToken"].as_str().unwrap().to_owned(),
        ),
    ]);
    let mut body = vec![0_u8; 5];
    body.extend(
        json!({ "process": { "cmd": "/bin/sh", "args": ["-c", LAUNCH] } })
            .to_string()
            .into_bytes(),
    );
    let (status, _, _) = fake.process(&envd, &body).await;
    assert_eq!(status, 200);
    let dir = fake.state().sandboxes[&id].dir.clone();
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let pid = std::fs::read_to_string(dir.join("intentd.pid"))
            .ok()
            .and_then(|text| text.split_whitespace().next()?.parse::<u32>().ok());
        if let Some(pid) = pid.filter(|pid| is_running(*pid)) {
            return (id, pid);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "sandbox {id}'s daemon never started: {}",
            std::fs::read_to_string(dir.join("serve.log")).unwrap_or_default()
        );
        // timing-guard: poll interval until the detached daemon writes its pid file
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The test process the scenarios below run, end and inspect: it starts two
/// sandbox daemons, pauses one, reports their pids, then ends as `<mode>`
/// says. Outside a scenario (`CHILD_ENV` unset) it does nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "the child process of the fake E2B teardown tests, which run it"]
async fn fake_e2b_child() {
    let Ok(spec) = std::env::var(CHILD_ENV) else {
        return;
    };
    let (mode, dir) = spec.split_once(':').unwrap();
    let dir = Path::new(dir);
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in(dir.to_str().unwrap(), "c-");
    let fake = FakeE2b::start(scratch, &fixture).await;
    let (_, running) = start_sandbox_daemon(&fake).await;
    let (paused_id, paused) = start_sandbox_daemon(&fake).await;
    fake.signal(&paused_id, "-STOP");
    let report = dir.join(format!("{PIDS_FILE}.tmp"));
    std::fs::write(&report, format!("{running} {paused}")).unwrap();
    std::fs::rename(&report, dir.join(PIDS_FILE)).unwrap();
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !dir.join(GO_FILE).exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the scenario never said go"
        );
        // timing-guard: poll interval until the scenario has checked the daemons
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    match mode {
        "pass" => {
            drop(fake);
            assert!(
                !is_running(running) && !is_running(paused),
                "dropping the fake leaves no sandbox daemon"
            );
        }
        "panic" => panic!("the fake E2B child fails on purpose"),
        "hang" => {
            // timing-guard: parked until the scenario SIGKILLs this process
            tokio::time::sleep(common::test_timeout(Duration::from_secs(300))).await;
            panic!("the scenario never killed this process");
        }
        other => panic!("unknown mode {other}"),
    }
}

/// Run [`fake_e2b_child`] in `mode` until it reports its daemons; the child,
/// its log and the pids.
fn spawn_child(mode: &str, dir: &Path) -> (common::DaemonChild, std::path::PathBuf, Vec<u32>) {
    let log_path = dir.join("child.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([CHILD_TEST, "--exact", "--ignored", "--nocapture"])
        .env(CHILD_ENV, format!("{mode}:{}", dir.display()))
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    let mut child = common::spawn_guarded(&mut cmd).unwrap();
    let deadline = Instant::now() + common::test_timeout(Duration::from_secs(120));
    loop {
        if let Ok(text) = std::fs::read_to_string(dir.join(PIDS_FILE)) {
            let pids = text
                .split_whitespace()
                .map(|pid| pid.parse().unwrap())
                .collect();
            return (child, log_path, pids);
        }
        let exited = child.try_wait().unwrap();
        assert!(
            exited.is_none() && Instant::now() < deadline,
            "the child never reported its daemons ({exited:?}): {}",
            std::fs::read_to_string(&log_path).unwrap_or_default()
        );
        // timing-guard: poll interval until the child reports its daemon pids
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Run the child in `mode` and prove both its daemons are gone soon after it
/// ends; `kill` SIGKILLs it first, as nextest does on a timeout.
fn no_daemon_outlives(mode: &str, kill: bool) {
    if !have_node_and_git() {
        eprintln!("skipping fake E2B teardown e2e: node or git not on PATH");
        return;
    }
    let dir = common::test_tempdir_in("/tmp", "itd-e2bt-");
    let (mut child, log, pids) = spawn_child(mode, dir.path());
    assert!(pids.iter().all(|pid| is_running(*pid)), "{pids:?} run");
    std::fs::write(dir.path().join(GO_FILE), "").unwrap();
    if kill {
        child.signal(Signal::SIGKILL).unwrap();
    }
    let status = child
        .wait_with_timeout(common::test_timeout(Duration::from_secs(60)))
        .unwrap()
        .expect("the child exits");
    let log = std::fs::read_to_string(&log).unwrap_or_default();
    if !kill {
        assert_eq!(status.success(), mode == "pass", "{status:?}: {log}");
    }
    let deadline = Instant::now() + common::test_timeout(Duration::from_secs(5));
    while pids.iter().any(|pid| is_running(*pid)) {
        assert!(
            Instant::now() < deadline,
            "sandbox daemons {pids:?} outlived the {mode} child: {log}"
        );
        // timing-guard: poll interval until the orphaned daemons are gone
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn no_sandbox_daemon_outlives_a_passing_fake_e2b_test() {
    no_daemon_outlives("pass", false);
}

#[test]
fn no_sandbox_daemon_outlives_a_failing_fake_e2b_test() {
    no_daemon_outlives("panic", false);
}

#[test]
fn no_sandbox_daemon_outlives_a_killed_fake_e2b_test() {
    no_daemon_outlives("hang", true);
}
