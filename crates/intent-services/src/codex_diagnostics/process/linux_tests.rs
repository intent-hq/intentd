use super::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

struct FixtureCleanup {
    task: Option<tokio::task::AbortHandle>,
    observation: PathBuf,
}

impl Drop for FixtureCleanup {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Ok(text) = std::fs::read_to_string(&self.observation) {
            for pid in text.lines().filter_map(|line| line.parse::<i32>().ok()) {
                if pid > 1 {
                    let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
                }
            }
        }
    }
}

#[tokio::test]
async fn cleanup_owns_descendant_adopted_after_child_snapshot() {
    let fixture = crate::test_support::test_tempdir("codex-adoption-fixture");
    let home = crate::test_support::test_tempdir("codex-adoption-home");
    let home_path = home.path().to_path_buf();
    let observation = fixture.path().join("owned-pids");
    let mut cleanup = FixtureCleanup {
        task: None,
        observation: observation.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut command = Command::new(intent_providers::find_node().expect("test requires Node"));
    command
        .env_clear()
        .env("HOME", &home_path)
        .env("DD_TRACE_ENABLED", "false")
        .env("DD_TRACE_STARTUP_LOGS", "false")
        .current_dir(&home_path)
        .args(["-e", r"
const fs = require('node:fs');
fs.writeFileSync(process.argv[2], process.pid + '\n');
const socket = require('node:net').connect(Number(process.argv[1]), '127.0.0.1');
socket.on('connect', () => socket.write(JSON.stringify({leader: process.pid}) + '\n'));
socket.once('data', () => {
  const child = require('node:child_process').spawn(process.execPath,
    ['-e', `process.send('ready'); process.disconnect(); setInterval(() => {}, 1000);`],
    {detached: true, stdio: ['ignore', 'ignore', 'ignore', 'ipc']});
  fs.appendFileSync(process.argv[2], child.pid + '\n');
  child.unref();
  child.once('message', () => socket.end(JSON.stringify({child: child.pid}) + '\n', () => process.exit(0)));
});
"])
        .arg(port.to_string())
        .arg(&observation);
    let mut probe = super::super::ProbeProcess::spawn(command, home)
        .await
        .unwrap();
    let (socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut socket = BufReader::new(socket);
    let leader = receive_pid(&mut socket, "leader").await;
    let (snapshot_tx, snapshot_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    probe.ownership.as_mut().unwrap().snapshot_barrier = Some((snapshot_tx, resume_rx));
    let task = tokio::spawn(async move { probe.cleanup().await });
    cleanup.task = Some(task.abort_handle());
    let snapshot = tokio::time::timeout(Duration::from_secs(5), snapshot_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(snapshot.contains(&leader));
    socket.get_mut().write_all(b"fork and exit").await.unwrap();
    let child = receive_pid(&mut socket, "child").await;
    assert!(!snapshot.contains(&child));
    assert_eq!(
        nix::unistd::getpgid(Some(Pid::from_raw(child))).unwrap(),
        Pid::from_raw(child),
        "fixture must escape the supervisor group"
    );
    // Observe the real leader exit before resuming cleanup's stale snapshot.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !terminated(leader).unwrap() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(home_path.is_dir());
    resume_tx.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(8), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, Ok(()));
    assert_eq!(
        kill(Pid::from_raw(child), None),
        Err(nix::errno::Errno::ESRCH),
        "cleanup returned with an adopted descendant alive; HOME exists: {}",
        home_path.exists()
    );
    assert!(!home_path.exists());
}

async fn receive_pid(socket: &mut BufReader<tokio::net::TcpStream>, field: &str) -> i32 {
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), socket.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let message: serde_json::Value = serde_json::from_str(&line).unwrap();
    i32::try_from(message[field].as_i64().unwrap()).unwrap()
}

#[tokio::test]
async fn cleanup_retains_home_when_control_child_is_lost() {
    let home = crate::test_support::test_tempdir("codex-unconfirmed-home");
    let home_path = home.path().to_path_buf();
    let mut command = Command::new(intent_providers::find_node().expect("test requires Node"));
    command
        .env_clear()
        .env("HOME", &home_path)
        .env("DD_TRACE_ENABLED", "false")
        .current_dir(&home_path)
        .args(["-e", "setInterval(() => {}, 1000)"]);
    let mut probe = super::super::ProbeProcess::spawn(command, home)
        .await
        .unwrap();
    let owner = probe.ownership.as_mut().unwrap();
    let control = owner.control_pid().await.unwrap();
    let owner_pid = owner.pid;
    let children = owned_children(owner_pid).unwrap();
    kill(Pid::from_raw(control), Signal::SIGKILL).unwrap();
    // Wait for actual removal, not just delivery of the terminating signal.
    tokio::time::timeout(Duration::from_secs(5), async {
        while owned_children(owner_pid).unwrap().contains(&control) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        probe.cleanup().await,
        Err(super::super::UnknownReason::CleanupFailed)
    );
    assert!(home_path.is_dir(), "unconfirmed cleanup must retain HOME");
    // The failed owner's drop kills its group. Confirm the fixture is gone
    // before explicitly removing the intentionally retained test directory.
    tokio::time::timeout(Duration::from_secs(5), async {
        while children.iter().any(|pid| !terminated(*pid).unwrap()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    std::fs::remove_dir_all(home_path).unwrap();
}
