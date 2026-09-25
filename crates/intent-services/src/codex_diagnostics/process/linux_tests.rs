use super::*;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn private_descriptors_do_not_reach_provider_or_descendant() {
    // Force multi-digit original descriptors without changing the daemon's
    // descriptor table: these ordinary owned files live only in this test.
    let _reserved: Vec<_> = (0..16)
        .map(|_| std::fs::File::open("/dev/null").unwrap())
        .collect();
    let home = crate::test_support::test_tempdir("codex-private-descriptors");
    let home_path = home.path().to_path_buf();
    let startup = home_path.join("bash-env");
    std::fs::write(&startup, "printf 'unexpected shell startup\\n'\n").unwrap();
    let mut command = Command::new("/bin/sh");
    command
        .env_clear()
        .env("BASH_ENV", &startup)
        .env("SHELLOPTS", "errexit")
        .current_dir(&home_path)
        .args([
            "-c",
            r#"
printf '%s\n' "$$"
/bin/sh -c '
    printf "%s\n" "$$"
    IFS= read -r request
    printf "%s\n" "$request"
    printf "provider stderr\n" >&2
    exit 7
'
status=$?
exit "$status"
"#,
        ]);
    let mut probe = super::super::ProbeProcess::spawn(command, home)
        .await
        .unwrap();
    let mut stdout = BufReader::new(probe.stdout.take().unwrap());
    let mut stderr = probe.stderr.take().unwrap();
    let pids = tokio::time::timeout(Duration::from_secs(5), async {
        let mut pids = Vec::new();
        for _ in 0..2 {
            let mut line = String::new();
            stdout.read_line(&mut line).await.unwrap();
            pids.push(line.trim().parse::<u32>().unwrap());
        }
        pids
    })
    .await
    .unwrap();
    let owner = probe.ownership.as_mut().unwrap();
    let control_pid = tokio::time::timeout(Duration::from_secs(5), owner.control_pid())
        .await
        .unwrap()
        .unwrap();
    let channels = [
        std::fs::read_link(format!(
            "/proc/self/fd/{}",
            owner.control.as_ref().unwrap().as_raw_fd()
        ))
        .unwrap(),
        std::fs::read_link(format!("/proc/self/fd/{}", owner.status.as_raw_fd())).unwrap(),
    ];
    let provider_fds = private_descriptors(pids[0], &channels);
    let descendant_fds = private_descriptors(pids[1], &channels);
    let supervisor_fds = private_descriptors(owner.pid, &channels);
    let control_fds = private_descriptors(u32::try_from(control_pid).unwrap(), &channels);

    // Complete ordinary IO and cleanup before asserting the regression so
    // even the expected pre-fix failure leaves no test processes or home.
    let (out, err, status) = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stdin = probe.stdin.take().unwrap();
        stdin.write_all(b"provider input\n").await.unwrap();
        drop(stdin);
        let mut out = String::new();
        let mut err = String::new();
        tokio::try_join!(
            stdout.read_to_string(&mut out),
            stderr.read_to_string(&mut err)
        )
        .unwrap();
        (out, err, probe.wait().await.unwrap())
    })
    .await
    .unwrap();
    assert!(home_path.is_dir());
    probe.cleanup().await.unwrap();
    assert!(!home_path.exists());
    assert_eq!(out, "provider input\n");
    assert_eq!(err, "provider stderr\n");
    assert_eq!(status.code(), Some(7));
    assert!(
        provider_fds.is_empty() && descendant_fds.is_empty(),
        "private channels {channels:?} reached provider {provider_fds:?} and descendant {descendant_fds:?}"
    );
    assert_eq!(
        supervisor_fds,
        [(3, channels[0].clone()), (4, channels[1].clone())]
    );
    // read temporarily duplicates the control descriptor onto stdin.
    assert!(control_fds.contains(&(3, channels[0].clone())));
    assert!(
        control_fds
            .iter()
            .all(|(fd, target)| [0, 3].contains(fd) && target == &channels[0]),
        "unexpected private descriptors in stable control child: {control_fds:?}"
    );
}

fn private_descriptors(pid: u32, channels: &[PathBuf; 2]) -> Vec<(i32, PathBuf)> {
    let mut descriptors: Vec<_> = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_str().unwrap().parse::<i32>().unwrap(),
                std::fs::read_link(entry.path()).unwrap(),
            )
        })
        .filter(|(_, target)| channels.contains(target))
        .collect();
    descriptors.sort();
    descriptors
}

#[tokio::test]
async fn closing_original_descriptors_preserves_reused_stdin() {
    for (control, status) in [(5, 6), (6, 5), (57, 58), (58, 57)] {
        // Set exact descriptor numbers only in an already-execed fixture shell,
        // never over the test runtime's descriptors or Rust's exec-error pipe.
        // Channel isolation is checked by pipe identity above: provider startup
        // (including host instrumentation) may reuse the closed fd numbers.
        let setup = format!(
            r#"exec /bin/bash --noprofile --norc -p -c "$1" intentd-codex-diagnostic "$2" "$3" /bin/sh -c "$4" {control}</dev/null {status}>&1"#
        );
        let mut child = Command::new("/bin/bash")
            .args([
                "--noprofile",
                "--norc",
                "-p",
                "-c",
                &setup,
                "descriptor-fixture",
            ])
            .arg(include_str!("supervise.sh"))
            .arg(control.to_string())
            .arg(status.to_string())
            .arg(
                r#"
IFS= read -r request
printf '%s\n' "$request"
printf 'provider stderr\n' >&2
exit 7
"#,
            )
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(b"provider input\n").await.unwrap();
        drop(stdin);
        let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(output.status.success(), "{control}/{status}: {output:?}");
        assert_eq!(output.stderr, b"provider stderr\n");
        let stdout = String::from_utf8(output.stdout).unwrap();
        let lines: Vec<_> = stdout.lines().collect();
        assert_eq!(lines.len(), 3, "{control}/{status}: {stdout}");
        assert!(lines.contains(&"provider input"));
        assert!(lines.contains(&"007"));
        assert!(lines.iter().any(|line| {
            line.len() == 10
                && line.bytes().all(|byte| byte.is_ascii_digit())
                && line.parse::<u32>().is_ok_and(|pid| pid > 1)
        }));
    }
}

struct FixtureCleanup {
    task: Option<tokio::task::AbortHandle>,
    observation: PathBuf,
}

#[tokio::test]
async fn proc_stat_reaped_after_open_counts_as_terminated() {
    let mut child = Command::new("/bin/sh")
        .args(["-c", "read -r request"])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    let mut stat = std::fs::File::open(format!("/proc/{pid}/stat")).unwrap();
    assert!(!terminated(pid.cast_signed()).unwrap());
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"exit\n")
        .await
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    assert!(terminated(pid.cast_signed()).unwrap());

    // Reaping between open and read yields ESRCH, not a missing-path ENOENT.
    let error = std::io::Read::read_to_string(&mut stat, &mut String::new()).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ESRCH));
    assert!(terminated_stat(Err(error)).unwrap());
    assert_eq!(
        terminated_stat(Err(io::ErrorKind::PermissionDenied.into()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied,
        "inspection failures must still prevent cleanup confirmation"
    );
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
