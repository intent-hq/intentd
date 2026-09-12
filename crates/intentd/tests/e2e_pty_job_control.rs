//! PTY job-control harness (intent-hq/intent#4231 / intentd#1808 hazard
//! class): boots `intentd serve` inside a real PTY session as a member of a
//! *background* process group, next to a sibling process with default signal
//! dispositions, and fails if any process in that session is ever stopped or
//! if a daemon-spawned login shell keeps the controlling tty while sharing
//! the daemon's process group.
//!
//! Why a PTY: CI runners have no controlling terminal, so a daemon child that
//! stops its own process group via tty job control (`kill(0, SIGTTIN)` from an
//! interactive shell started in a background group) is invisible there — it
//! only surfaced in PTY-backed local gates. The daemon runs
//! `prewarm_login_shell_path` (`$SHELL -ilc …`) at boot, so the hazard fires
//! during startup with no RPC required. That prewarm is fire-and-forget
//! (`spawn_blocking`, never joined), so daemon health does not prove the
//! capture ran: the daemon gets a hermetic `HOME` whose `.bash_profile`
//! appends a marker line (`pid=$$ flags=$- stat=$(cat /proc/$$/stat)`) to
//! `capture.log`, and the scan window may not close until an *interactive*
//! marker has landed and that capture shell has exited. Under the pre-fix
//! layout the shell stops in its job-control initialisation before startup
//! files run, so the marker never appears and the `T` state is what fails.
//!
//! Layout: a `bash -m` driver is the PTY session leader and keeps the tty
//! foreground group for itself. Its single background job is a subshell that
//! forks the node mock-MCP fixture (default dispositions) and then `exec`s the
//! daemon, so both share one background process group of the PTY. Gated on
//! `node` + `/bin/bash` + the fixture + a readable `/proc`; skips cleanly
//! otherwise. Linux-only: the invariants are read from `/proc/<pid>/stat`.

#![cfg(target_os = "linux")]

mod common;

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nix::sys::signal::{killpg, SigHandler, Signal};
use nix::unistd::Pid;
use portable_pty::{native_pty_system, CommandBuilder, MasterPty, PtySize};

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

/// Base window during which the stopped-process and controlling-tty
/// invariants are enforced, measured from the driver spawn. Covers daemon boot
/// plus the login-shell capture (5 s `-ilc` budget) with margin; scaled by
/// `INTENTD_TEST_TIMEOUT_MULTIPLIER` through `common::test_timeout`.
const INVARIANT_WINDOW: Duration = Duration::from_secs(8);

/// Whole-test ceiling, kept below nextest's kill (`slow-timeout` period 90 s
/// × `terminate-after` 2 = 180 s, `.config/nextest.toml`) so a stall reports
/// the step that stalled instead of a bare "test timed out". Deliberately
/// unscaled, and every wait in the test is clamped to it: the setup waits
/// (`min(daemon_startup_timeout, budget)` — 180 s at multiplier 3 on its own,
/// so the clamp matters), the scan loop, and the capture barrier all end at
/// `started + 150 s` at the latest, after which only the fixture ping remains
/// (5 s connect + 5 s read). Worst case 150 + 10 = 160 s < 180 s.
const TEST_BUDGET: Duration = Duration::from_secs(150);

/// Connect/read bound for the post-window fixture ping; part of the
/// `TEST_BUDGET` arithmetic above.
const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// Poll cadence for the `/proc` scan.
const SCAN_INTERVAL: Duration = Duration::from_millis(50);

/// One `/proc/<pid>/stat` row: the fields after `(comm)` are
/// `state ppid pgrp session tty_nr tpgid …`.
#[derive(Debug, Clone, PartialEq)]
struct ProcStat {
    pid: i32,
    comm: String,
    state: char,
    ppid: i32,
    pgrp: i32,
    session: i32,
    tty_nr: i32,
    tpgid: i32,
}

fn parse_stat(pid: i32, raw: &str) -> Option<ProcStat> {
    let open = raw.find('(')?;
    let close = raw.rfind(')')?;
    let comm = raw[open + 1..close].to_string();
    let mut fields = raw[close + 1..].split_whitespace();
    let state = fields.next()?.chars().next()?;
    let mut next_i32 = || fields.next()?.parse::<i32>().ok();
    Some(ProcStat {
        pid,
        comm,
        state,
        ppid: next_i32()?,
        pgrp: next_i32()?,
        session: next_i32()?,
        tty_nr: next_i32()?,
        tpgid: next_i32()?,
    })
}

/// Root of the procfs the harness reads; a constant so the no-`/proc` skip
/// can be exercised by pointing it elsewhere.
const PROC_ROOT: &str = "/proc";

fn read_stat(pid: i32) -> Option<ProcStat> {
    let raw = std::fs::read_to_string(format!("{PROC_ROOT}/{pid}/stat")).ok()?;
    parse_stat(pid, &raw)
}

/// Snapshot every process visible in `/proc`. Rows that vanish mid-scan are
/// skipped (a process exiting between `read_dir` and `read_to_string`).
fn scan_procs() -> Vec<ProcStat> {
    let Ok(entries) = std::fs::read_dir(PROC_ROOT) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|e| e.file_name().to_str()?.parse::<i32>().ok())
        .filter_map(read_stat)
        .collect()
}

/// Pids of every (transitive) descendant of `root` in `procs`.
fn descendants(procs: &[ProcStat], root: i32) -> HashSet<i32> {
    let mut children: HashMap<i32, Vec<i32>> = HashMap::new();
    for p in procs {
        children.entry(p.ppid).or_default().push(p.pid);
    }
    let mut out = HashSet::new();
    let mut stack = vec![root];
    while let Some(pid) = stack.pop() {
        for &child in children.get(&pid).into_iter().flatten() {
            if out.insert(child) {
                stack.push(child);
            }
        }
    }
    out
}

fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// The fixture script path, or `None` when the e2e must be skipped.
fn fixture_script() -> Option<&'static str> {
    if !node_available() {
        eprintln!("skipping PTY job-control E2E: node not on PATH");
        return None;
    }
    if !Path::new("/bin/bash").exists() {
        eprintln!("skipping PTY job-control E2E: /bin/bash not present");
        return None;
    }
    let self_pid = i32::try_from(std::process::id()).expect("pid fits");
    if read_stat(self_pid).is_none() {
        eprintln!(
            "skipping PTY job-control E2E: {PROC_ROOT}/{self_pid}/stat unreadable \
             (procfs unavailable; the invariants are read from /proc)"
        );
        return None;
    }
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mock-mcp-server.mjs"
    );
    if !PathBuf::from(script).exists() {
        eprintln!("skipping PTY job-control E2E: fixture not found at {script}");
        return None;
    }
    Some(script)
}

/// Driver run by `bash -m` as the PTY session leader. `set -m` puts the one
/// background job into its own process group without handing it the tty, so
/// the subshell — and everything it forks or `exec`s into — is a *background*
/// group of the PTY: the node fixture (default dispositions, started first so
/// it is already a group member when the daemon boots) and then the daemon
/// itself, which the subshell `exec`s so `$!` is the daemon pid and the job
/// pgid at once. The driver then `exec`s into a long `sleep` (same pid, still
/// session leader and foreground group) so the session stays alive for the
/// whole window: a `wait` would return as soon as the job is *stopped*, the
/// leader would exit, and the orphaned stopped group would be reaped by
/// SIGHUP before the scan could observe the `T` state.
const DRIVER_SCRIPT: &str = r#"set -u
cd "$D"
(
  node "$FIXTURE" --http < /dev/null > "$D/fixture.log" 2>&1 &
  echo $! > "$D/fixture.pid"
  exec "$INTENTD_BIN" serve < /dev/null > /dev/null 2> "$D/daemon.log"
) &
echo $! > "$D/job.pgid"
exec sleep 1000
"#;

/// `.bash_profile` of the daemon's hermetic `HOME`. `bash -ilc` (and the
/// `-lc` fallback) is a login shell, so it sources this after `/etc/profile`;
/// the marker records only the shell's pid, `$-` flags (`i` iff interactive)
/// and its own `/proc/<pid>/stat` row — nothing from the environment.
const CAPTURE_PROFILE: &str = r#"echo "pid=$$ flags=$- stat=$(cat /proc/$$/stat)" >> "$INTENTD_PTY_CAPTURE_LOG"
"#;

/// One line of `capture.log`, written by [`CAPTURE_PROFILE`].
#[derive(Debug, Clone, PartialEq)]
struct CaptureMarker {
    pid: i32,
    interactive: bool,
    stat: ProcStat,
}

fn parse_capture_marker(line: &str) -> Option<CaptureMarker> {
    let rest = line.strip_prefix("pid=")?;
    let (pid, rest) = rest.split_once(' ')?;
    let pid = pid.parse::<i32>().ok()?;
    let rest = rest.strip_prefix("flags=")?;
    let (flags, rest) = rest.split_once(' ')?;
    let raw = rest.strip_prefix("stat=")?;
    let stat = parse_stat(pid, raw)?;
    (stat.pid == pid && raw.starts_with(&format!("{pid} ("))).then_some(CaptureMarker {
        pid,
        interactive: flags.contains('i'),
        stat,
    })
}

fn parse_capture_log(text: &str) -> Vec<CaptureMarker> {
    text.lines().filter_map(parse_capture_marker).collect()
}

/// The pre-fix layout, judged from the capture shell's own `stat` row: still
/// inside the daemon's process group, or still attached to the controlling
/// tty. `None` means the shell was detached (intentd#1808's `setsid`).
fn capture_layout_violation(m: &CaptureMarker, job_pgid: i32) -> Option<String> {
    if m.stat.pgrp == job_pgid {
        return Some(format!(
            "capture shell pid={} stayed in the daemon's process group {job_pgid}",
            m.pid
        ));
    }
    if m.stat.tty_nr != 0 {
        return Some(format!(
            "capture shell pid={} kept a controlling tty (tty_nr={})",
            m.pid, m.stat.tty_nr
        ));
    }
    None
}

fn tail(path: &Path, lines: usize) -> String {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let all: Vec<&str> = text.lines().collect();
            let start = all.len().saturating_sub(lines);
            all[start..].join("\n")
        }
        Err(e) => format!("<unreadable {}: {e}>", path.display()),
    }
}

/// Everything the harness owns; `Drop` tears the whole PTY session down
/// (job group, driver group, every remaining session member or daemon
/// descendant) before the tempdir is removed, on success and on panic alike.
struct Harness {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    _master: Box<dyn MasterPty + Send>,
    pty_out: Arc<Mutex<Vec<u8>>>,
    driver_pid: i32,
    job_pgid: Option<i32>,
    daemon_pid: Option<i32>,
    dir: tempfile::TempDir,
}

impl Harness {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// Processes the invariants apply to: every member of the PTY session
    /// plus every (transitive) descendant of the daemon — the latter catches
    /// a capture shell that `setsid` moved out of the session.
    fn tracked<'a>(&self, procs: &'a [ProcStat]) -> Vec<&'a ProcStat> {
        let desc = self
            .daemon_pid
            .map(|pid| descendants(procs, pid))
            .unwrap_or_default();
        procs
            .iter()
            .filter(|p| p.session == self.driver_pid || desc.contains(&p.pid))
            .collect()
    }

    fn diag(&self, procs: &[ProcStat]) -> String {
        let table: Vec<String> = self
            .tracked(procs)
            .into_iter()
            .map(|p| {
                format!(
                    "  pid={} comm={} state={} ppid={} pgrp={} session={} tty_nr={} tpgid={}",
                    p.pid, p.comm, p.state, p.ppid, p.pgrp, p.session, p.tty_nr, p.tpgid
                )
            })
            .collect();
        let pty = String::from_utf8_lossy(&self.pty_out.lock().unwrap()).into_owned();
        let pty_lines: Vec<&str> = pty.lines().collect();
        let pty_tail = pty_lines[pty_lines.len().saturating_sub(40)..].join("\n");
        format!(
            "driver_pid={} job_pgid={:?} daemon_pid={:?}\n--- tracked processes ---\n{}\n\
             --- pty output (tail) ---\n{}\n--- daemon.log (tail) ---\n{}\n\
             --- fixture.log (tail) ---\n{}",
            self.driver_pid,
            self.job_pgid,
            self.daemon_pid,
            table.join("\n"),
            pty_tail,
            tail(&self.path("daemon.log"), 60),
            tail(&self.path("fixture.log"), 20),
        )
    }

    fn fail(&self, msg: &str, procs: &[ProcStat]) -> ! {
        panic!("{msg}\n{}", self.diag(procs));
    }

    /// Block until `name` exists in the data dir and parses as a pid.
    fn await_pid_file(&self, name: &str, deadline: Instant) -> i32 {
        let path = self.path(name);
        loop {
            if let Some(pid) = std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| s.trim().parse::<i32>().ok())
            {
                return pid;
            }
            if Instant::now() >= deadline {
                self.fail(&format!("driver never wrote {name}"), &scan_procs());
            }
            std::thread::sleep(SCAN_INTERVAL);
        }
    }

    /// Enforce the two invariants against one `/proc` snapshot.
    fn check_invariants(&self, procs: &[ProcStat], fixture_pid: i32) {
        let job_pgid = self.job_pgid.expect("job pgid known");
        for p in self.tracked(procs) {
            if matches!(p.state, 'T' | 't') {
                self.fail(
                    &format!(
                        "process stopped by tty job control: pid={} comm={} state={}",
                        p.pid, p.comm, p.state
                    ),
                    procs,
                );
            }
            // A daemon-spawned shell that still shares the daemon's background
            // process group *and* the controlling tty is exactly the pre-fix
            // layout (intentd#1808); the fixture legitimately has both.
            if p.pid != fixture_pid
                && p.pgrp == job_pgid
                && p.tty_nr != 0
                && p.comm == "bash"
                && Some(p.pid) != self.daemon_pid
            {
                self.fail(
                    &format!(
                        "daemon-spawned shell kept the controlling tty inside the daemon's \
                         process group: pid={} ppid={} pgrp={} tty_nr={}",
                        p.pid, p.ppid, p.pgrp, p.tty_nr
                    ),
                    procs,
                );
            }
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Snapshot first: once the daemon is killed its detached children
        // (the `setsid` capture shell) reparent to init and fall out of
        // `tracked()`, so a later scan would miss them.
        let victims: Vec<i32> = self
            .tracked(&scan_procs())
            .into_iter()
            .map(|p| p.pid)
            .collect();
        if let Some(pgid) = self.job_pgid {
            let _ = killpg(Pid::from_raw(pgid), Signal::SIGKILL);
        }
        let _ = killpg(Pid::from_raw(self.driver_pid), Signal::SIGKILL);
        let _ = self.child.kill();
        for pid in victims {
            let _ = nix::sys::signal::kill(Pid::from_raw(pid), Signal::SIGKILL);
        }
        let _ = self.child.wait();
    }
}

/// Spawn the driver inside a fresh PTY. Terminal-stop signals are reset to
/// their defaults around the spawn so the session starts from the layout a
/// real terminal gives a backgrounded daemon, even when the harness itself
/// inherited `SIG_IGN` for them (nextest ignores `SIGTTIN` for test
/// binaries); the harness's own dispositions are restored right after.
fn spawn_driver(dir: tempfile::TempDir, fixture: &str) -> Harness {
    let data_dir = dir.path().to_path_buf();
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(&data_dir);
    let script = data_dir.join("driver.sh");
    std::fs::write(&script, DRIVER_SCRIPT).expect("write driver script");
    // Hermetic HOME for the daemon: its `$SHELL -ilc` capture is a login shell
    // and sources this `.bash_profile`, which is the capture barrier.
    let home = data_dir.join("home");
    std::fs::create_dir_all(&home).expect("mkdir hermetic home");
    std::fs::write(home.join(".bash_profile"), CAPTURE_PROFILE).expect("write .bash_profile");

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("openpty");
    // Clone the reader before anything is spawned so a failure here cannot
    // leak a live driver.
    let mut reader = pair.master.try_clone_reader().expect("pty reader");
    let mut cmd = CommandBuilder::new("/bin/bash");
    cmd.arg("-m");
    cmd.arg(&script);
    cmd.cwd(&data_dir);
    cmd.env("D", &data_dir);
    cmd.env("FIXTURE", fixture);
    cmd.env("INTENTD_BIN", env!("CARGO_BIN_EXE_intentd"));
    cmd.env("INTENTD_DATA_DIR", &data_dir);
    cmd.env("INTENTD_WORKSPACES_DIR", &workspaces_dir);
    cmd.env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"));
    cmd.env("INTENTD_ASSERT_HERMETIC_ROOT", "1");
    cmd.env("INTENTD_AUTH_TOKEN", TOKEN);
    cmd.env("INTENTD_TCP_PORT", "0");
    cmd.env("SHELL", "/bin/bash");
    cmd.env("HOME", &home);
    cmd.env("INTENTD_PTY_CAPTURE_LOG", data_dir.join("capture.log"));

    let stop_signals = [Signal::SIGTTIN, Signal::SIGTTOU, Signal::SIGTSTP];
    // SAFETY: only the disposition is changed (no handler function is
    // installed), and it is restored before this function returns.
    let previous: Vec<SigHandler> = stop_signals
        .iter()
        .map(|sig| unsafe { nix::sys::signal::signal(*sig, SigHandler::SigDfl) }.expect("reset"))
        .collect();
    let spawned = pair.slave.spawn_command(cmd);
    for (sig, handler) in stop_signals.iter().zip(previous) {
        // SAFETY: restoring the disposition captured above.
        unsafe { nix::sys::signal::signal(*sig, handler) }.expect("restore");
    }
    let mut child = spawned.expect("spawn bash -m driver in pty");
    drop(pair.slave);
    let Some(driver_pid) = child.process_id().and_then(|pid| i32::try_from(pid).ok()) else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("driver pid unavailable");
    };
    // From here on the driver is owned by the RAII guard, so any later panic
    // (in this function or the test) tears the session down.
    let pty_out = Arc::new(Mutex::new(Vec::new()));
    let harness = Harness {
        child,
        _master: pair.master,
        pty_out: Arc::clone(&pty_out),
        driver_pid,
        job_pgid: None,
        daemon_pid: None,
        dir,
    };

    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = reader.read(&mut buf) {
            if n == 0 {
                break;
            }
            pty_out.lock().unwrap().extend_from_slice(&buf[..n]);
        }
    });
    harness
}

/// Raw JSON-RPC `ping` over HTTP to the fixture; proves the sibling is not
/// merely alive but still serving after the window.
fn ping_fixture(port: u16) -> String {
    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    let mut stream =
        TcpStream::connect_timeout(&addr.into(), PING_TIMEOUT).expect("connect fixture");
    stream
        .set_read_timeout(Some(PING_TIMEOUT))
        .expect("read timeout");
    let body = r#"{"jsonrpc":"2.0","id":7,"method":"ping"}"#;
    write!(
        stream,
        "POST / HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("write ping");
    let mut out = String::new();
    let _ = stream.read_to_string(&mut out);
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_in_background_pty_process_group_never_stops_the_session() {
    let Some(fixture) = fixture_script() else {
        return;
    };
    let started = Instant::now();
    let budget_end = started + TEST_BUDGET;
    let window_end = (started + common::test_timeout(INVARIANT_WINDOW)).min(budget_end);
    // Clamped: `daemon_startup_timeout()` alone is 180 s at multiplier 3.
    let setup_deadline = (started + common::daemon_startup_timeout()).min(budget_end);

    let mut h = spawn_driver(common::test_tempdir_in("/tmp", "itd-pty-jc-"), fixture);

    // Layout preconditions: the job is a background group of the driver's
    // session on a real controlling tty, and the driver keeps the foreground.
    let job_pgid = h.await_pid_file("job.pgid", setup_deadline);
    h.job_pgid = Some(job_pgid);
    h.daemon_pid = Some(job_pgid);
    let fixture_pid = h.await_pid_file("fixture.pid", setup_deadline);
    let procs = scan_procs();
    // The hazard can fire within milliseconds of the daemon starting, so the
    // invariants are enforced on the very first snapshot as well.
    h.check_invariants(&procs, fixture_pid);
    let Some(job) = procs.iter().find(|p| p.pid == job_pgid) else {
        h.fail("job leader vanished before the window opened", &procs);
    };
    if job.pgrp != job_pgid || job.session != h.driver_pid || job.tty_nr == 0 {
        h.fail(
            "precondition: job is not its own process group inside the PTY session",
            &procs,
        );
    }
    if job.tpgid == job.pgrp || job.tpgid != h.driver_pid {
        h.fail(
            "precondition: job is not a *background* group (tty foreground pgid mismatch)",
            &procs,
        );
    }

    // Daemon health (WSS listener up via `system.status` over UDS) runs
    // concurrently with the invariant scan; the login-shell capture that
    // triggers the hazard happens during this boot.
    let socket = h.path("intentd.sock");
    let log = h.path("daemon.log");
    let health = tokio::spawn(async move { common::await_wss_status_logged(&socket, &log).await });

    // Capture barrier: the window may not close until an interactive
    // (`-ilc`) marker has landed and that capture shell has exited, so the
    // scan provably covers the hazard. A fallback (`-lc`) marker with no
    // interactive one means the `-ilc` attempt failed — under the fix it
    // must not.
    let capture_log = h.path("capture.log");
    let mut interactive_capture: Option<CaptureMarker> = None;
    loop {
        let procs = scan_procs();
        h.check_invariants(&procs, fixture_pid);
        if !procs.iter().any(|p| p.pid == job_pgid) {
            h.fail("daemon exited during the invariant window", &procs);
        }
        if interactive_capture.is_none() {
            let markers =
                parse_capture_log(&std::fs::read_to_string(&capture_log).unwrap_or_default());
            for m in &markers {
                if let Some(why) = capture_layout_violation(m, job_pgid) {
                    h.fail(&why, &procs);
                }
            }
            match markers.iter().find(|m| m.interactive) {
                Some(m) => interactive_capture = Some(m.clone()),
                None if !markers.is_empty() => h.fail(
                    &format!(
                        "login-shell capture fell back to non-interactive: markers={markers:?}"
                    ),
                    &procs,
                ),
                None => {}
            }
        }
        let capture_done = interactive_capture
            .as_ref()
            .is_some_and(|m| !procs.iter().any(|p| p.pid == m.pid));
        let now = Instant::now();
        if now >= window_end && health.is_finished() && capture_done {
            break;
        }
        if now >= budget_end {
            h.fail(
                &format!(
                    "test budget {TEST_BUDGET:?} exhausted (health finished: {}, interactive \
                     capture marker: {:?}, capture shell exited: {capture_done}, capture.log: {:?})",
                    health.is_finished(),
                    interactive_capture,
                    std::fs::read_to_string(&capture_log).unwrap_or_default()
                ),
                &procs,
            );
        }
        tokio::time::sleep(SCAN_INTERVAL).await;
    }
    let status = match health.await {
        Ok(status) => status,
        Err(e) => h.fail(&format!("daemon health check failed: {e}"), &scan_procs()),
    };
    assert!(
        status["result"]["port"].as_u64().is_some(),
        "system.status carried no WSS port: {status}"
    );

    // Sibling liveness after the window: still running, never stopped, and
    // still answering on the port it announced.
    let procs = scan_procs();
    match procs.iter().find(|p| p.pid == fixture_pid) {
        Some(p) if !matches!(p.state, 'T' | 't' | 'Z') => {}
        Some(p) => h.fail(
            &format!("fixture pid={} is in state {}", p.pid, p.state),
            &procs,
        ),
        None => h.fail("fixture process is gone", &procs),
    }
    let port = tail(&h.path("fixture.log"), 50)
        .lines()
        .find_map(|l| l.strip_prefix("PORT=")?.trim().parse::<u16>().ok())
        .unwrap_or_else(|| h.fail("fixture never announced PORT=", &procs));
    let reply = ping_fixture(port);
    if !(reply.starts_with("HTTP/1.1 200") && reply.contains(r#""id":7"#)) {
        h.fail(&format!("fixture ping reply: {reply:?}"), &scan_procs());
    }
}

#[cfg(test)]
mod helper_tests {
    use super::*;

    const DETACHED: &str =
        "pid=4242 flags=hiBHc stat=4242 (bash) S 4100 4242 4242 0 -1 4194304 379 406 0 1 0 0";

    #[test]
    fn parse_stat_handles_comm_with_spaces_and_parens() {
        let s = parse_stat(7, "7 (my (odd) comm) T 1 2 3 4 5 rest").unwrap();
        assert_eq!(s.comm, "my (odd) comm");
        assert_eq!(
            (s.state, s.ppid, s.pgrp, s.session, s.tty_nr, s.tpgid),
            ('T', 1, 2, 3, 4, 5)
        );
        assert!(parse_stat(7, "7 (bash) S 1 2").is_none());
        assert!(parse_stat(7, "no parens").is_none());
    }

    #[test]
    fn capture_marker_parses_interactive_and_fallback() {
        let m = parse_capture_marker(DETACHED).unwrap();
        assert_eq!(m.pid, 4242);
        assert!(m.interactive);
        assert_eq!(
            (m.stat.pgrp, m.stat.session, m.stat.tty_nr, m.stat.tpgid),
            (4242, 4242, 0, -1)
        );

        let fallback =
            parse_capture_marker("pid=9 flags=hBc stat=9 (bash) S 1 9 9 0 -1 0").unwrap();
        assert!(!fallback.interactive);
    }

    #[test]
    fn capture_marker_rejects_malformed_lines() {
        for line in [
            "",
            "garbage",
            "pid=x flags=i stat=1 (bash) S 1 1 1 0 -1",
            "pid=1 stat=1 (bash) S 1 1 1 0 -1",
            "pid=1 flags=i stat=2 (bash) S 1 1 1 0 -1",
            "pid=1 flags=i stat=1 (bash) S 1",
        ] {
            assert!(parse_capture_marker(line).is_none(), "accepted {line:?}");
        }
    }

    #[test]
    fn capture_log_skips_partial_lines() {
        let text = format!("{DETACHED}\nnoise\npid=4300 flags=hBc stat=4300 (bash) S 1 4300 4300 0 -1\npid=99 flags=");
        let markers = parse_capture_log(&text);
        assert_eq!(
            markers.iter().map(|m| m.pid).collect::<Vec<_>>(),
            [4242, 4300]
        );
        assert_eq!(markers.iter().filter(|m| m.interactive).count(), 1);
    }

    #[test]
    fn layout_violation_flags_shared_pgrp_or_tty() {
        let ok = parse_capture_marker(DETACHED).unwrap();
        assert_eq!(capture_layout_violation(&ok, 4100), None);

        let mut same_pgrp = ok.clone();
        same_pgrp.stat.pgrp = 4100;
        assert!(capture_layout_violation(&same_pgrp, 4100)
            .unwrap()
            .contains("process group"));

        let mut with_tty = ok.clone();
        with_tty.stat.tty_nr = 34816;
        assert!(capture_layout_violation(&with_tty, 4100)
            .unwrap()
            .contains("controlling tty"));
    }
}
