//! End-to-end supervisor tests: drive the real sitter binary against a
//! local HTTP fixture server and fake-daemon shell scripts (unix only; the
//! windows code paths are cfg-compiled but exercised via CI builds).
//!
//! Timing runs at millisecond scale through the `INTENTD_SITTER_*_MS` env
//! overrides so no test sleeps for hours. Positive-path waits go through a
//! [`Barrier`] or [`wait_until`]; the few fixed sleeps that remain carry a
//! `// timing-guard: <reason>` marker, enforced repo-wide by the
//! `fixed_sleep_lint` test in `intent-core`.

#![cfg(unix)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use intentd_sitter::cli::{Channel, CHANNEL_ENV};
use intentd_sitter::manifest::TARGET_TRIPLE;
use intentd_sitter::paths::{SitterPaths, DAEMON_BIN_NAME, DATA_DIR_ENV};
use intentd_sitter::state::{self, SitterState};
use intentd_sitter::supervisor::{
    BACKOFF_CAP_ENV, BACKOFF_INITIAL_ENV, BACKOFF_RESET_ENV, CHECK_MAX_ENV, CHECK_MIN_ENV,
    GIVE_UP_AFTER_ENV, IDLE_RESTART_ENV, KILL_TIMEOUT_ENV, MANIFEST_BASE_URL_ENV,
    RESTART_FOR_UPDATE_EXIT_CODE, UPDATE_RESTART_ENV,
};

const SITTER_BIN: &str = env!("CARGO_BIN_EXE_intentd-sitter");

/// Env var the fake daemon scripts log to (set on the sitter, inherited by
/// the child — which doubles as an env-inheritance check).
const FAKE_DAEMON_LOG: &str = "FAKE_DAEMON_LOG";

/// Serializes the load-sensitive serve-loop tests against one another. These
/// drive a real long-running sitter supervisor that respawns/updates its child
/// on wall-clock timers (backoff windows, periodic checks) and assert on the
/// resulting spawn counts. `cargo test` runs the tests within this binary in
/// parallel, so several live supervisors spawning children in tight loops
/// otherwise starve one another off-CPU — flaking the timing assertions (most
/// sharply `crash_respawn_backs_off_exponentially`, whose backed-off child can
/// miss its spawn budget under load). Holding this guard for each such test's
/// duration keeps only one live supervisor loop running at a time. Mirrors the
/// `CHILD_SPAWN_SERIAL` (`provider_models`) and `WATCHER_TEST_SERIAL`
/// (events/mod.rs) precedents. The brief one-shot tests (`doctor`, `restart`
/// without a live sitter, single-shot `serve`) spawn once and finish, so they
/// stay parallel. `unwrap_or_else(into_inner)` recovers from a poisoned lock so
/// one panicking test does not cascade into the rest.
static SERVE_LOOP_SERIAL: Mutex<()> = Mutex::new(());

type Routes = Arc<Mutex<HashMap<String, Vec<u8>>>>;
type RequestLog = Arc<Mutex<Vec<String>>>;

/// Minimal HTTP/1.1 fixture server over swappable routes: tests mutate the
/// map mid-run to publish a "new release".
fn serve(routes: Routes) -> String {
    serve_recording(routes).0
}

/// [`serve`] plus a log of every request path received, so tests can assert
/// the sitter made (or did not make) HTTP requests.
fn serve_recording(routes: Routes) -> (String, RequestLog) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let log: RequestLog = Arc::new(Mutex::new(Vec::new()));
    let server_log = Arc::clone(&log);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let routes = Arc::clone(&routes);
            let log = Arc::clone(&server_log);
            thread::spawn(move || handle(stream, &routes, &log));
        }
    });
    (format!("http://{addr}"), log)
}

fn handle(mut stream: TcpStream, routes: &Routes, log: &RequestLog) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(_) if header != "\r\n" && !header.is_empty() => {}
            _ => break,
        }
    }
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let (status, body) = match routes.lock().unwrap().get(path) {
        Some(body) => ("200 OK", body.clone()),
        None => ("404 Not Found", b"not found".to_vec()),
    };
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(&body);
    // Log only after the response is fully written: a logged request proves
    // its handler already read the route table, so a test that waits on the
    // log and then swaps a route knows the swap cannot have been seen by
    // that request.
    log.lock().unwrap().push(path.to_string());
}

/// A base URL whose port refuses requests (network down).
///
/// The listener stays bound for the life of the process and a detached
/// thread accepts each connection and immediately drops it, so the sitter's
/// update check deterministically fails. Binding and then dropping the
/// listener (the previous approach) released the ephemeral port back to the
/// OS, which could reassign it to a sibling test's fixture server before the
/// sitter connected — turning the "dead" URL into a live one under parallel
/// test load (intent-hq/monorepo#1158).
fn dead_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming() {
            drop(stream);
        }
    });
    url
}

/// [`serve`] with a stall switch: while the returned flag is false requests
/// are served normally (so the sitter's startup check succeeds); once a test
/// sets it, each accepted socket is parked in a detached thread that sleeps
/// forever — a stalled update endpoint whose checks hang until the updater's
/// own (minutes-long) timeout instead of failing fast.
fn serve_stallable(routes: Routes) -> (String, Arc<std::sync::atomic::AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let stalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let server_stalled = Arc::clone(&stalled);
    thread::spawn(move || {
        let log: RequestLog = Arc::new(Mutex::new(Vec::new()));
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let routes = Arc::clone(&routes);
            let log = Arc::clone(&log);
            let stalled = Arc::clone(&server_stalled);
            thread::spawn(move || {
                if stalled.load(std::sync::atomic::Ordering::SeqCst) {
                    let _hold = stream;
                    // timing-guard: park forever
                    thread::sleep(Duration::from_secs(3600));
                } else {
                    handle(stream, &routes, &log);
                }
            });
        }
    });
    (url, stalled)
}

/// [`serve`] with a hold switch: while the returned flag is set, each
/// accepted socket is parked (and counted in the returned counter) until
/// the flag clears, then served normally — an update check a test can keep
/// in flight for as long as it needs and then release.
fn serve_holdable(
    routes: Routes,
) -> (
    String,
    Arc<std::sync::atomic::AtomicBool>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let hold = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let parked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server_hold = Arc::clone(&hold);
    let server_parked = Arc::clone(&parked);
    thread::spawn(move || {
        let log: RequestLog = Arc::new(Mutex::new(Vec::new()));
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let routes = Arc::clone(&routes);
            let log = Arc::clone(&log);
            let hold = Arc::clone(&server_hold);
            let parked = Arc::clone(&server_parked);
            thread::spawn(move || {
                if hold.load(std::sync::atomic::Ordering::SeqCst) {
                    parked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    while hold.load(std::sync::atomic::Ordering::SeqCst) {
                        // timing-guard: poll interval
                        thread::sleep(Duration::from_millis(10));
                    }
                }
                handle(stream, &routes, &log);
            });
        }
    });
    (url, hold, parked)
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// `.tar.xz` with `intentd-<triple>/intentd` (mode 0755) — the cargo-dist
/// unix archive layout.
fn make_tar_xz(bin_contents: &[u8]) -> Vec<u8> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bin_contents.len() as u64);
    header.set_mode(0o755);
    header.set_cksum();
    let encoder = liblzma::write::XzEncoder::new(Vec::new(), 6);
    let mut builder = tar::Builder::new(encoder);
    builder
        .append_data(
            &mut header,
            format!("intentd-{TARGET_TRIPLE}/{DAEMON_BIN_NAME}"),
            bin_contents,
        )
        .unwrap();
    builder.into_inner().unwrap().finish().unwrap()
}

/// Schema-v1 manifest with one platform entry for this build's triple.
fn manifest_json(version: &str, base_url: &str, asset: &str, sha256: &str) -> Vec<u8> {
    serde_json::json!({
        "schema": 1,
        "channel": "stable",
        "version": version,
        "tag": format!("v{version}"),
        "platforms": {
            TARGET_TRIPLE: {
                "asset": asset,
                "url": format!("{base_url}/{asset}"),
                "sha256": sha256,
            }
        }
    })
    .to_string()
    .into_bytes()
}

/// Manifest with no platform entries: enough for an "already current"
/// check, which never looks at `platforms`.
fn manifest_bare(version: &str) -> Vec<u8> {
    serde_json::json!({ "schema": 1, "version": version, "platforms": {} })
        .to_string()
        .into_bytes()
}

/// Install a fake daemon script as `versions/<version>/intentd` and point
/// `state.json` at it.
fn preinstall(paths: &SitterPaths, version: &str, script: &str) {
    let bin = paths.daemon_binary(version);
    fs::create_dir_all(bin.parent().unwrap()).unwrap();
    fs::write(&bin, script).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&bin, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let state = SitterState {
        current_version: Some(version.to_string()),
        ..SitterState::default()
    };
    state::save(&paths.state_path, &state).unwrap();
}

/// Fake daemon: dump each arg on its own line, then exit 0.
fn args_dump_script() -> String {
    format!("#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"${FAKE_DAEMON_LOG}\"\nexit 0\n")
}

/// Fake daemon: log a start line, then run until SIGTERM/SIGINT (both exit
/// 0 — a graceful daemon shutdown).
fn long_running_script(version: &str) -> String {
    format!(
        "#!/bin/sh\n\
         printf 'start {version} :: %s\\n' \"$*\" >> \"${FAKE_DAEMON_LOG}\"\n\
         trap 'exit 0' TERM INT\n\
         sleep 60 &\n\
         wait $!\n\
         exit 0\n"
    )
}

/// Fake daemon: log one line and crash with `code`.
fn crash_script(code: i32) -> String {
    format!("#!/bin/sh\necho run >> \"${FAKE_DAEMON_LOG}\"\nexit {code}\n")
}

/// Like [`long_running_script`] but the start line records whether the
/// sitter injected the update-restart marker into the environment.
fn long_running_env_script(version: &str) -> String {
    format!(
        "#!/bin/sh\n\
         printf 'start {version} update_restart=%s\\n' \
         \"${{{UPDATE_RESTART_ENV}:-unset}}\" >> \"${FAKE_DAEMON_LOG}\"\n\
         trap 'exit 0' TERM INT\n\
         sleep 60 &\n\
         wait $!\n\
         exit 0\n"
    )
}

/// Like [`crash_script`] but the run line records whether the sitter
/// injected the update-restart marker into the environment.
fn crash_env_script(code: i32) -> String {
    format!(
        "#!/bin/sh\n\
         printf 'run update_restart=%s\\n' \
         \"${{{UPDATE_RESTART_ENV}:-unset}}\" >> \"${FAKE_DAEMON_LOG}\"\n\
         exit {code}\n"
    )
}

/// A release-file barrier between a test and its fake daemon: the daemon's
/// shell script blocks at [`Barrier::sh_wait`] until the test calls
/// [`Barrier::release`], so a held state stays stable for as long as the
/// test's assertions take instead of racing a fixed `sleep`. A script can
/// also [`Barrier::sh_arrive`] just before waiting so the test can prove,
/// via [`Barrier::entered`], that the daemon actually reached the hold.
struct Barrier {
    path: std::path::PathBuf,
}

impl Barrier {
    fn new(data_dir: &Path, name: &str) -> Self {
        Self {
            path: data_dir.join(format!("barrier-{name}")),
        }
    }

    /// Let every script blocked in [`Barrier::sh_wait`] proceed.
    fn release(&self) {
        fs::write(&self.path, b"").unwrap();
    }

    /// Shell snippet that blocks until the barrier is released.
    fn sh_wait(&self) -> String {
        let path = self.path.display();
        // timing-guard: poll interval
        format!("while [ ! -e \"{path}\" ]; do sleep 0.05; done")
    }

    /// Shell snippet that marks the barrier as reached (see [`Barrier::entered`]).
    fn sh_arrive(&self) -> String {
        format!(": > \"{}\"", self.entered_path().display())
    }

    /// Whether a script has run [`Barrier::sh_arrive`].
    fn entered(&self) -> bool {
        self.entered_path().exists()
    }

    fn entered_path(&self) -> std::path::PathBuf {
        let mut path = self.path.clone().into_os_string();
        path.push(".entered");
        path.into()
    }
}

/// Fake daemon speaking the idle-restart handshake: the start line records
/// the update-restart and idle-restart markers; SIGTERM/SIGINT log a `term`
/// line and exit 0; SIGUSR2 logs a `usr2` line, then holds at `release` —
/// so a test can assert the held state for as long as it needs — and only
/// once the test releases it exits with [`RESTART_FOR_UPDATE_EXIT_CODE`].
fn idle_restart_script(version: &str, release: &Barrier) -> String {
    format!(
        "#!/bin/sh\n\
         printf 'start {version} update_restart=%s idle_restart=%s\\n' \
         \"${{{UPDATE_RESTART_ENV}:-unset}}\" \"${{{IDLE_RESTART_ENV}:-unset}}\" \
         >> \"${FAKE_DAEMON_LOG}\"\n\
         trap 'echo \"term {version}\" >> \"${FAKE_DAEMON_LOG}\"; exit 0' TERM INT\n\
         trap 'echo \"usr2 {version}\" >> \"${FAKE_DAEMON_LOG}\"; {wait}; \
         exit {RESTART_FOR_UPDATE_EXIT_CODE}' USR2\n\
         sleep 60 &\n\
         wait $!\n\
         exit 0\n",
        wait = release.sh_wait(),
    )
}

/// Like [`idle_restart_script`] but SIGUSR2 is only logged, never acted on:
/// a daemon that never gets idle.
fn never_idle_script(version: &str) -> String {
    format!(
        "#!/bin/sh\n\
         printf 'start {version} update_restart=%s idle_restart=%s\\n' \
         \"${{{UPDATE_RESTART_ENV}:-unset}}\" \"${{{IDLE_RESTART_ENV}:-unset}}\" \
         >> \"${FAKE_DAEMON_LOG}\"\n\
         trap 'echo \"term {version}\" >> \"${FAKE_DAEMON_LOG}\"; exit 0' TERM INT\n\
         trap 'echo \"usr2 {version}\" >> \"${FAKE_DAEMON_LOG}\"' USR2\n\
         while :; do sleep 60 & wait $!; done\n"
    )
}

/// Fake daemon: the first run arrives at and holds on `release`, then exits
/// with [`RESTART_FOR_UPDATE_EXIT_CODE`] on its own once released (a daemon
/// that was already idle when asked); later runs behave like
/// [`long_running_script`]. The one-shot marker lives next to the log.
fn restart_once_script(version: &str, release: &Barrier) -> String {
    format!(
        "#!/bin/sh\n\
         printf 'start {version} update_restart=%s idle_restart=%s\\n' \
         \"${{{UPDATE_RESTART_ENV}:-unset}}\" \"${{{IDLE_RESTART_ENV}:-unset}}\" \
         >> \"${FAKE_DAEMON_LOG}\"\n\
         if [ ! -e \"${FAKE_DAEMON_LOG}.restarted\" ]; then\n\
         : > \"${FAKE_DAEMON_LOG}.restarted\"\n\
         {arrive}\n\
         {wait}\n\
         exit {RESTART_FOR_UPDATE_EXIT_CODE}\n\
         fi\n\
         trap 'exit 0' TERM INT\n\
         sleep 60 &\n\
         wait $!\n\
         exit 0\n",
        arrive = release.sh_arrive(),
        wait = release.sh_wait(),
    )
}

/// Fake daemon: log one line, stay up `secs`, then crash with `code` — a
/// daemon that serves for a while and dies, not one that can never start.
fn long_lived_crash_script(secs: &str, code: i32) -> String {
    // timing-guard: uptime > reset knob
    let stay_up = format!("sleep {secs}");
    format!(
        "#!/bin/sh\n\
         echo run >> \"${FAKE_DAEMON_LOG}\"\n\
         {stay_up}\n\
         exit {code}\n"
    )
}

/// Sitter command wired to a temp data dir, a manifest base URL, and the
/// fake-daemon log path; stderr goes to `<data_dir>/sitter-stderr.log`.
fn sitter_command(data_dir: &Path, base_url: &str) -> Command {
    let mut cmd = Command::new(SITTER_BIN);
    cmd.env(DATA_DIR_ENV, data_dir)
        .env(MANIFEST_BASE_URL_ENV, base_url)
        .env(FAKE_DAEMON_LOG, daemon_log_path(data_dir))
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            fs::File::create(stderr_path(data_dir)).unwrap(),
        ));
    cmd
}

/// A sitter `Child` spawned as the leader of its own process group and torn
/// down with that whole group if dropped while still running. A test that
/// parks its fake daemon on a [`Barrier`] and panics before releasing it
/// would otherwise drop a plain `Child` (no kill on drop) and the `TempDir`
/// holding the release file, leaving the sitter and its parked daemon
/// behind forever. Derefs to `Child` for the existing helpers.
struct GuardedSitter(Child);

/// Spawn `cmd` as a [`GuardedSitter`].
fn spawn_guarded(cmd: &mut Command) -> GuardedSitter {
    use std::os::unix::process::CommandExt;
    GuardedSitter(cmd.process_group(0).spawn().unwrap())
}

impl std::ops::Deref for GuardedSitter {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for GuardedSitter {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for GuardedSitter {
    fn drop(&mut self) {
        // Only while the sitter is still alive does its pid still name the
        // group (and cannot have been reused); a reaped child is the test's
        // own business.
        if matches!(self.0.try_wait(), Ok(None)) {
            let pgid = nix::unistd::Pid::from_raw(self.0.id().cast_signed());
            let _ = nix::sys::signal::killpg(pgid, nix::sys::signal::Signal::SIGKILL);
            let _ = self.0.wait();
        }
    }
}

fn daemon_log_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("fake-daemon.log")
}

fn stderr_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("sitter-stderr.log")
}

fn read_or_empty(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// Poll `cond` up to `timeout`; panic with `what` when it never holds.
fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        // timing-guard: poll interval
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out after {timeout:?} waiting for {what}");
}

/// Wait for the sitter to exit, force-killing it on timeout so a broken
/// build never wedges the test run.
fn wait_exit(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        // timing-guard: poll interval
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("sitter did not exit within {timeout:?}");
}

fn send_signal(child: &Child, signal: &str) {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(status.success(), "kill -{signal} failed");
}

const MANIFEST_PATH: &str = "/channel-stable/stable.json";
const BETA_MANIFEST_PATH: &str = "/channel-beta/beta.json";

#[test]
fn forwards_args_verbatim_and_clean_exit_passes_through() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &args_dump_script());
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(routes);

    // No timing overrides: the persisted schedule must use the real 12–24h
    // jitter window (the sitter exits with the one-shot child long before).
    let mut sitter = sitter_command(dir.path(), &base_url)
        .args([
            "serve",
            "--sitter-channel=stable",
            "--resume-all",
            "--weird-flag=x y",
            "-v",
            "positional arg",
        ])
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(0), "clean child exit passes through");

    // All args verbatim and in order; --sitter-* stripped; nothing injected.
    assert_eq!(
        read_or_empty(&daemon_log_path(dir.path())),
        "serve\n--resume-all\n--weird-flag=x y\n-v\npositional arg\n"
    );

    // `serve` performs the startup update check.
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        [MANIFEST_PATH.to_string()],
        "serve must fetch the channel manifest exactly once at startup"
    );

    let state = state::load(&paths.state_path);
    let last = state.last_check_at.expect("last_check_at persisted");
    let next = state.next_check_at.expect("next_check_at persisted");
    let delta_secs = (next - last).whole_seconds();
    assert!(
        (12 * 3600..24 * 3600).contains(&delta_secs),
        "next check jitter out of [12h,24h): {delta_secs}s"
    );
}

#[test]
fn no_network_startup_falls_back_to_installed_version() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &args_dump_script());

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .args(["serve", "--resume-all"])
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(0));

    assert_eq!(
        read_or_empty(&daemon_log_path(dir.path())),
        "serve\n--resume-all\n"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(stderr.contains("update check failed"), "stderr: {stderr}");
    assert!(
        stderr.contains("falling back to installed intentd 0.1.0"),
        "stderr: {stderr}"
    );
}

#[test]
fn no_network_and_nothing_installed_exits_nonzero() {
    let dir = tempfile::tempdir().unwrap();

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .arg("serve")
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(1), "expected a non-zero fail-fast exit");

    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(stderr.contains("update check failed"), "stderr: {stderr}");
    assert!(
        stderr.contains("no intentd daemon is installed for channel stable"),
        "stderr: {stderr}"
    );
    assert!(
        !daemon_log_path(dir.path()).exists(),
        "no daemon must have run"
    );
}

#[test]
fn update_mid_run_swaps_binary_and_preserves_args() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(CHECK_MIN_ENV, "300")
        .env(CHECK_MAX_ENV, "301")
        .env(KILL_TIMEOUT_ENV, "5000")
        .args(["serve", "--resume-all", "--extra=flag"])
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Publish 0.2.0; the next periodic check installs it and restarts.
    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    wait_until("daemon 0.2.0 to start", Duration::from_secs(20), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    // Each version ran exactly once, with identical forwarded args.
    let expected_args = "serve --resume-all --extra=flag";
    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            format!("start 0.1.0 :: {expected_args}"),
            format!("start 0.2.0 :: {expected_args}"),
        ]
    );
    assert!(paths.daemon_binary("0.2.0").exists());
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );
}

/// A mid-run periodic-check update respawns a different version than the
/// one that just ran: that respawn (and only it) must carry
/// `INTENTD_UPDATE_RESTART=1`; the first spawn must not.
#[test]
fn update_mid_run_respawn_sets_update_restart_env() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_env_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(UPDATE_RESTART_ENV)
        .env(CHECK_MIN_ENV, "300")
        .env(CHECK_MAX_ENV, "301")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Publish 0.2.0; the next periodic check installs it and restarts.
    let archive = make_tar_xz(long_running_env_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    wait_until("daemon 0.2.0 to start", Duration::from_secs(20), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset".to_string(),
            "start 0.2.0 update_restart=1".to_string(),
        ],
        "only the update-triggered respawn may carry the env var"
    );
}

/// A SIGHUP restart that re-resolves to the same version (plain
/// `intentd restart`, no update) must not mark the respawn as
/// update-triggered.
#[test]
fn sighup_same_version_respawn_does_not_set_update_restart_env() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_env_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    // Hour-long check interval: only the SIGHUP may restart the child.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(UPDATE_RESTART_ENV)
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    send_signal(&sitter, "HUP");
    wait_until("daemon 0.1.0 to restart", Duration::from_secs(15), || {
        read_or_empty(&log_path).lines().count() >= 2
    });

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset".to_string(),
            "start 0.1.0 update_restart=unset".to_string(),
        ],
        "a same-version SIGHUP respawn must not carry the env var"
    );
}

/// A sitter launched with the update-restart marker already in its own
/// environment (e.g. respawned by a wrapper that set it) must not leak it
/// to children: first spawns and same-version respawns clear it rather
/// than inherit it.
#[test]
fn inherited_update_restart_env_is_cleared_on_plain_spawns() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_env_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    // Hour-long check interval: only the SIGHUP may restart the child.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(UPDATE_RESTART_ENV, "1")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    send_signal(&sitter, "HUP");
    wait_until("daemon 0.1.0 to restart", Duration::from_secs(15), || {
        read_or_empty(&log_path).lines().count() >= 2
    });

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset".to_string(),
            "start 0.1.0 update_restart=unset".to_string(),
        ],
        "plain spawns must clear an inherited marker, not pass it through"
    );
}

/// Crash respawns of the same version are plain restarts: none of them
/// may carry the update-restart marker.
#[test]
fn crash_respawn_does_not_set_update_restart_env() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_env_script(7));
    // "Already current" on every failed-start re-check: the loop keeps
    // respawning the same crashing 0.1.0.
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(UPDATE_RESTART_ENV)
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "100")
        .env(GIVE_UP_AFTER_ENV, "10000")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("two crash respawns", Duration::from_secs(15), || {
        read_or_empty(&log_path).lines().count() >= 3
    });

    send_signal(&sitter, "TERM");
    wait_exit(&mut sitter, Duration::from_secs(10));

    let contents = read_or_empty(&log_path);
    let lines: Vec<&str> = contents.lines().collect();
    assert!(lines.len() >= 3, "expected at least 3 runs: {lines:?}");
    for line in &lines {
        assert_eq!(
            *line, "run update_restart=unset",
            "crash respawns must not carry the env var: {lines:?}"
        );
    }
}

/// The CLI update path: an update installed out-of-band (new version in
/// `state.json`) followed by SIGHUP respawns a different version than the
/// one that just ran, so that respawn must carry the update-restart
/// marker.
#[test]
fn sighup_respawn_with_version_change_sets_update_restart_env() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_env_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    // Hour-long check interval: only the SIGHUP may restart the child.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(UPDATE_RESTART_ENV)
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Simulate `intentd update`: install 0.2.0 and point state.json at it
    // while the sitter keeps running, then SIGHUP.
    preinstall(&paths, "0.2.0", &long_running_env_script("0.2.0"));
    send_signal(&sitter, "HUP");
    wait_until("daemon 0.2.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset".to_string(),
            "start 0.2.0 update_restart=1".to_string(),
        ],
        "a SIGHUP respawn onto a new version must carry the env var"
    );
}

#[test]
fn config_channel_switch_applies_at_next_periodic_check() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));

    // No --sitter-channel flag and no INTENTD_CHANNEL env: the channel comes
    // from config.toml / the stable default, so the supervisor re-resolves
    // it before each periodic check.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(CHANNEL_ENV)
        .env(CHECK_MIN_ENV, "300")
        .env(CHECK_MAX_ENV, "301")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Publish 0.2.0 on the beta channel only, then pin channel=beta in
    // config.toml mid-run (what `intentd sitter channel beta` writes).
    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            BETA_MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    fs::write(&paths.config_path, "channel = \"beta\"\n").unwrap();
    wait_until("daemon 0.2.0 to start", Duration::from_secs(20), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let requests = requests.lock().unwrap();
    assert_eq!(
        requests.first().map(String::as_str),
        Some(MANIFEST_PATH),
        "startup check must use the stable default: {requests:?}"
    );
    assert!(
        requests.iter().any(|p| p == BETA_MANIFEST_PATH),
        "the check after the config switch must fetch beta.json: {requests:?}"
    );
    let state = state::load(&paths.state_path);
    assert_eq!(state.current_version.as_deref(), Some("0.2.0"));
    assert_eq!(state.channel, Channel::Beta);
    assert!(paths.daemon_binary("0.2.0").exists());
}

#[test]
fn flag_pinned_channel_ignores_config_switch_mid_run() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_script("0.1.0"));
    // A fully installable beta 0.2.0 is on offer; the flag-pinned sitter
    // must never even fetch its manifest.
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            BETA_MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(CHECK_MIN_ENV, "100")
        .env(CHECK_MAX_ENV, "101")
        .env(KILL_TIMEOUT_ENV, "5000")
        .args(["serve", "--sitter-channel=stable"])
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Write the config pin mid-run, then let several more checks elapse.
    fs::write(&paths.config_path, "channel = \"beta\"\n").unwrap();
    let checks_at_switch = requests.lock().unwrap().len();
    wait_until(
        "several more periodic checks",
        Duration::from_secs(15),
        || requests.lock().unwrap().len() >= checks_at_switch + 3,
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let requests = requests.lock().unwrap();
    assert!(
        requests.iter().all(|p| p == MANIFEST_PATH),
        "flag-pinned sitter must never fetch beta.json: {requests:?}"
    );
    let starts = read_or_empty(&log_path)
        .lines()
        .filter(|line| line.starts_with("start "))
        .count();
    assert_eq!(starts, 1, "pinned sitter must not install/restart");
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.1.0")
    );
}

#[test]
fn crash_respawn_backs_off_exponentially() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_script(7));

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "400")
        .env(BACKOFF_RESET_ENV, "60000")
        // This test measures the backoff curve, not the give-up threshold:
        // raise the threshold out of reach so the loop runs for the whole
        // window (`permanent_startup_failure_gives_up_and_exits_zero` owns
        // the give-up behaviour).
        .env(GIVE_UP_AFTER_ENV, "10000")
        .arg("serve")
        .spawn()
        .unwrap();
    // timing-guard: measurement window
    thread::sleep(Duration::from_secs(5));
    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_ne!(
        status.code(),
        Some(0),
        "crash-looping sitter must not exit 0"
    );

    // Doubling from 50ms, capped at 400ms (spawns at ~0/50/150/350/750/1150/
    // 1550/1950/2350ms, then every ~400ms), gives ~15 runs in 5s; a constant
    // 50ms delay would give ~100. Bound both sides to keep proving "backoff,
    // not a constant-delay flood" while leaving the floor low. The window is
    // deliberately generous (widened 2.4s -> 5s): `SERVE_LOOP_SERIAL` is inert
    // under nextest (each test is its own process), so the backed-off child
    // races the whole oversubscribed suite and can be starved to zero spawns
    // through the first several seconds — the longer window lets it accumulate
    // a safe margin of runs once that transient load clears.
    let runs = read_or_empty(&daemon_log_path(dir.path()))
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert!(
        (3..=30).contains(&runs),
        "expected backed-off respawns, got {runs} runs"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("exited unexpectedly (exit code 7)"),
        "stderr: {stderr}"
    );
    assert!(stderr.contains("respawning intentd in"), "stderr: {stderr}");
}

/// The bug this guards: a daemon that can never start (e.g. its data dir was
/// written by a newer intentd) used to be respawned forever, so the service
/// burned CPU and the user saw nothing but a timeout. After
/// `give_up_after_failures` failed starts the sitter must stop — with exit
/// **0**, because launchd (`KeepAlive`/`SuccessfulExit: false`) and systemd
/// (`Restart=on-failure`) both relaunch a non-zero exit.
#[test]
fn permanent_startup_failure_gives_up_and_exits_zero() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_script(9));

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "100")
        // No start can ever reach this uptime, so nothing resets the count.
        .env(BACKOFF_RESET_ENV, "60000")
        .env(GIVE_UP_AFTER_ENV, "4")
        .arg("serve")
        .spawn()
        .unwrap();

    // Nothing signals the sitter: it must exit on its own.
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(
        status.code(),
        Some(0),
        "giving up must exit 0 or the service manager relaunches the crash loop"
    );

    let runs = read_or_empty(&daemon_log_path(dir.path()))
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert_eq!(runs, 4, "must stop at the give-up threshold, not before it");

    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("intentd 0.1.0 exited unexpectedly (exit code 9)"),
        "the daemon's actual failure must be logged: {stderr}"
    );
    assert!(
        stderr.contains("failed 4 times in a row")
            && stderr.contains("giving up instead of respawning it forever"),
        "stderr: {stderr}"
    );
}

/// The other half of the contract: a daemon that keeps serving for a while
/// before dying is transiently, not permanently, broken — every start that
/// outlives `backoff_reset_after` clears the counter, so it is respawned
/// forever exactly as before.
#[test]
fn crashes_after_a_healthy_run_never_trip_the_give_up() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // Up for ~500ms (well past the 200ms reset window), then exit 1.
    preinstall(&paths, "0.1.0", &long_lived_crash_script("0.5", 1));

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "100")
        .env(BACKOFF_RESET_ENV, "200")
        .env(GIVE_UP_AFTER_ENV, "4")
        .arg("serve")
        .spawn()
        .unwrap();

    // Six runs is past the threshold of 4: without the reset the sitter
    // would already have given up and this would time out.
    wait_until(
        "six respawns of the long-lived daemon",
        Duration::from_secs(30),
        || {
            read_or_empty(&daemon_log_path(dir.path()))
                .lines()
                .filter(|line| *line == "run")
                .count()
                >= 6
        },
    );
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must still be supervising a daemon that keeps recovering"
    );

    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        !stderr.contains("giving up"),
        "a recovering daemon must never trigger give-up: {stderr}"
    );

    send_signal(&sitter, "TERM");
    wait_exit(&mut sitter, Duration::from_secs(10));
}

/// The log lines `scripts/install.sh` and `scripts/install.ps1` grep for in
/// their post-timeout diagnosis: the give-up banner plus the failure
/// phrasings the serve loop emits. This is a prose contract, not a
/// machine-readable marker — the sitter binary and both install scripts ship
/// from the same `sitter-latest` release and the installer replaces the
/// sitter before (re)starting the service, so the emitter and the matchers
/// can never skew in version; these tests are what keeps the sides in
/// lockstep. Rewording a sitter line (or a script pattern) without updating
/// the other side fails here instead of silently degrading the installers
/// back to the misleading "may still be downloading" warning.
const INSTALL_LOG_CONTRACT: [&str; 4] = [
    "times in a row without ever staying up",
    "exited unexpectedly",
    "failed to spawn",
    "failed waiting on intentd",
];

/// Both install scripts and the supervisor source must carry every contract
/// substring. Substring presence in `supervisor.rs` is the only pin for the
/// "failed waiting on intentd" arm, which no test can trigger (it needs
/// `child.wait()` itself to fail); the other three are additionally proven
/// emitted, verbatim, by the two behavioral tests below.
#[test]
fn install_log_contract_scripts_and_supervisor_match() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sources = [
        (root.join("src/supervisor.rs"), "//"),
        (root.join("../../scripts/install.sh"), "#"),
        (root.join("../../scripts/install.ps1"), "#"),
    ];
    for (path, comment_prefix) in &sources {
        let content =
            fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        // Comment lines also quote the contract substrings (the lockstep
        // pointers in supervisor.rs and both scripts), so scan only code
        // lines: rewording a real emission or grep pattern must fail here
        // even while a comment still carries the old text.
        let code = content
            .lines()
            .filter(|line| !line.trim_start().starts_with(comment_prefix))
            .collect::<Vec<_>>()
            .join("\n");
        for needle in INSTALL_LOG_CONTRACT {
            assert!(
                code.contains(needle),
                "{} lost the install log-contract substring {needle:?} from its \
                 code (comment lines are ignored); sitter lines and \
                 install-script patterns must change in lockstep",
                path.display()
            );
        }
    }
}

/// A permanently-crashing daemon must produce the "exited unexpectedly"
/// respawn line and the give-up banner carrying the contract substrings
/// verbatim — this is what pins the actually-emitted text, not just the
/// source.
#[test]
fn install_log_contract_crash_loop_lines_are_emitted_verbatim() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_script(3));

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "100")
        .env(BACKOFF_RESET_ENV, "60000")
        .env(GIVE_UP_AFTER_ENV, "2")
        .arg("serve")
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(0));

    let stderr = read_or_empty(&stderr_path(dir.path()));
    for needle in [
        "exited unexpectedly",
        "times in a row without ever staying up",
    ] {
        assert!(
            stderr.contains(needle),
            "install.sh/install.ps1 grep for {needle:?}, which the sitter no \
             longer emits — update both scripts and INSTALL_LOG_CONTRACT in \
             lockstep; stderr: {stderr}"
        );
    }
}

/// Same pin for the spawn-failure arm: a binary that exists but cannot be
/// executed must produce the "failed to spawn" line verbatim.
#[test]
fn install_log_contract_spawn_failure_line_is_emitted_verbatim() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // Preinstall, then strip the exec bit: the binary exists (so nothing
    // tries to download a replacement) but cannot be spawned.
    preinstall(&paths, "0.1.0", &crash_script(3));
    {
        use std::os::unix::fs::PermissionsExt;
        let bin = paths.daemon_binary("0.1.0");
        fs::set_permissions(&bin, fs::Permissions::from_mode(0o644)).unwrap();
    }

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "100")
        .env(BACKOFF_RESET_ENV, "60000")
        .env(GIVE_UP_AFTER_ENV, "1")
        .arg("serve")
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(0));

    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("failed to spawn"),
        "install.sh/install.ps1 grep for \"failed to spawn\", which the sitter \
         no longer emits — update both scripts and INSTALL_LOG_CONTRACT in \
         lockstep; stderr: {stderr}"
    );
}

/// The crash-loop self-heal (intent-hq/monorepo#3191): a daemon stuck in a
/// crash loop must force off-schedule channel re-checks so a fixed build
/// published on the channel is installed and respawned — without waiting
/// out the periodic schedule (12–24h in production, disabled here) and
/// without any give-up in play. Before the fix the sitter only re-ran the
/// startup check's version forever, so this test wedges until timeout.
/// Uses the env-recording scripts to also cover the third update path's
/// marker behavior: a fix adopted during failed-start backoff
/// (`FailedStartCheck::Respawn`) differs from the version that last ran,
/// so that respawn (and only it) must carry the update-restart marker.
#[test]
fn crash_loop_self_heals_when_the_channel_publishes_a_fix() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_env_script(7));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));

    // Periodic checks disabled (hour-long interval) and give-up out of
    // reach: only the failed-start re-check can find the fix.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(UPDATE_RESTART_ENV)
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "200")
        .env(BACKOFF_RESET_ENV, "60000")
        .env(GIVE_UP_AFTER_ENV, "10000")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());

    // Wait for the startup check plus at least one failed-start re-check
    // against the still-broken manifest, so the fix below is definitely
    // found by a re-check and not by the startup check.
    wait_until(
        "a failed-start re-check against the 0.1.0 manifest",
        Duration::from_secs(15),
        || read_or_empty(&log_path).contains("run") && requests.lock().unwrap().len() >= 2,
    );

    // Publish fixed 0.2.0; the crash loop's next re-check must install it.
    let archive = make_tar_xz(long_running_env_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    wait_until("daemon 0.2.0 to start", Duration::from_secs(20), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must still be supervising after self-healing"
    );
    // Per-cause marker matrix: the crash respawns of broken 0.1.0 are plain
    // restarts (unmarked), while the fix adopted by a failed-start re-check
    // respawns a different version than the one that last ran — that spawn
    // (and only it) must carry the update-restart marker.
    let contents = read_or_empty(&log_path);
    let lines: Vec<&str> = contents.lines().collect();
    assert!(
        lines
            .iter()
            .filter(|l| l.starts_with("run"))
            .all(|l| *l == "run update_restart=unset"),
        "crash respawns must not carry the env var: {lines:?}"
    );
    assert_eq!(
        lines.last(),
        Some(&"start 0.2.0 update_restart=1"),
        "the adopted-fix respawn must carry the env var: {lines:?}"
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        !stderr.contains("giving up"),
        "self-heal must not go through give-up: {stderr}"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));
}

/// The give-up half of #3191: when the failure count reaches the give-up
/// threshold, the final failure's re-check runs first — a fix published on
/// the channel is installed and respawned instead of the sitter wedging
/// itself with a give-up exit. Before the fix the sitter gave up here.
#[test]
fn fix_published_at_the_give_up_threshold_heals_instead_of_giving_up() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_script(9));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));

    // The 1s backoff after the first crash is the window in which the test
    // publishes the fix, so the second (= threshold) failure's re-check
    // deterministically sees 0.2.0.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(BACKOFF_INITIAL_ENV, "1000")
        .env(BACKOFF_CAP_ENV, "2000")
        .env(BACKOFF_RESET_ENV, "60000")
        .env(GIVE_UP_AFTER_ENV, "2")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());

    // Startup check + the first failure's re-check both saw 0.1.0: the fix
    // is published strictly between failure 1 and failure 2.
    wait_until(
        "the first failure's re-check against the 0.1.0 manifest",
        Duration::from_secs(15),
        || read_or_empty(&log_path).contains("run") && requests.lock().unwrap().len() >= 2,
    );
    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }

    // Failure 2 hits the threshold, but its re-check finds and installs
    // 0.2.0: the sitter must respawn it, not give up.
    wait_until("daemon 0.2.0 to start", Duration::from_secs(20), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive: a published fix beats the give-up"
    );
    let runs = read_or_empty(&log_path)
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert_eq!(runs, 2, "0.1.0 must have failed exactly twice");
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        !stderr.contains("giving up"),
        "a healed crash loop must never give up: {stderr}"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));
}

/// Give-up still works when there is genuinely no fix: with a reachable
/// channel that keeps answering "0.1.0 is current", every failed start
/// re-checks (startup + one per failure) and the sitter still gives up at
/// the threshold with exit 0 — the re-checks must not reset the counter.
#[test]
fn give_up_still_fires_when_the_recheck_confirms_no_newer_version() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_script(9));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(routes);

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "100")
        .env(BACKOFF_RESET_ENV, "60000")
        .env(GIVE_UP_AFTER_ENV, "4")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .arg("serve")
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(
        status.code(),
        Some(0),
        "an unfixable crash loop must still give up with exit 0"
    );

    let runs = read_or_empty(&daemon_log_path(dir.path()))
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert_eq!(runs, 4, "must stop at the give-up threshold");
    // Startup check + one re-check per failed start: last_check_at keeps
    // moving while crash-looping, so the stall is diagnosable.
    assert_eq!(
        requests.lock().unwrap().len(),
        5,
        "expected the startup check plus one re-check per failure"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("failed 4 times in a row")
            && stderr.contains("giving up instead of respawning it forever"),
        "stderr: {stderr}"
    );
    let state = state::load(&paths.state_path);
    assert!(
        state.last_check_at.is_some(),
        "re-checks must persist the check schedule"
    );
}

/// Same self-heal for the spawn-failure arm: a binary that exists but can
/// never be spawned must also trigger failed-start re-checks and pick up a
/// published fix.
#[test]
fn spawn_failure_loop_self_heals_when_the_channel_publishes_a_fix() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // Preinstall, then strip the exec bit: the binary exists (so the
    // startup check stays AlreadyCurrent) but cannot be spawned.
    preinstall(&paths, "0.1.0", &crash_script(3));
    {
        use std::os::unix::fs::PermissionsExt;
        let bin = paths.daemon_binary("0.1.0");
        fs::set_permissions(&bin, fs::Permissions::from_mode(0o644)).unwrap();
    }
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "200")
        .env(BACKOFF_RESET_ENV, "60000")
        .env(GIVE_UP_AFTER_ENV, "10000")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let stderr = stderr_path(dir.path());
    wait_until(
        "a failed-spawn re-check against the 0.1.0 manifest",
        Duration::from_secs(15),
        || {
            read_or_empty(&stderr).contains("failed to spawn")
                && requests.lock().unwrap().len() >= 2
        },
    );

    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.2.0 to start", Duration::from_secs(20), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must still be supervising after self-healing"
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));
}

#[test]
fn sighup_during_crash_backoff_respawns_the_state_json_version() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // 0.1.0 crash-loops; a 30s backoff (never elapsing within the test)
    // guarantees the SIGHUP below lands during the backoff sleep.
    preinstall(&paths, "0.1.0", &crash_script(7));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(CHANNEL_ENV)
        .env(BACKOFF_INITIAL_ENV, "30000")
        .env(BACKOFF_CAP_ENV, "30000")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    let stderr = stderr_path(dir.path());
    wait_until(
        "the crashed daemon to enter backoff",
        Duration::from_secs(15),
        || {
            read_or_empty(&log_path).contains("run")
                && read_or_empty(&stderr).contains("respawning intentd in")
        },
    );

    // The recovery a crash-looping user reaches for: force-install a fixed
    // 0.2.0 (`sitter channel beta --redownload`), then `intentd restart`
    // (SIGHUP) while the sitter is still deep in its backoff sleep.
    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            BETA_MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    let output = run_one_shot(
        dir.path(),
        &base_url,
        &["sitter", "channel", "beta", "--redownload"],
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );

    send_signal(&sitter, "HUP");
    // Well under the 30s backoff: the SIGHUP must cut the sleep short AND
    // re-resolve the version from state.json, not respawn crashing 0.1.0.
    wait_until("daemon 0.2.0 to start", Duration::from_secs(10), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the restart"
    );
    let runs = read_or_empty(&log_path)
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert_eq!(runs, 1, "crashing 0.1.0 must not have been respawned");

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));
}

/// SIGUSR1 ("update now"): the on-demand check installs a newer published
/// version and restarts the daemon on it — an update-triggered respawn, so
/// it must carry the update-restart marker — without exiting the sitter.
#[test]
fn sigusr1_installs_newer_version_and_restarts_with_update_env() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_env_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    // Hour-long check interval: only the SIGUSR1 may check and restart.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(UPDATE_RESTART_ENV)
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Publish 0.2.0, then `kill -USR1`: the sitter must check immediately,
    // install it, and restart the daemon on it.
    let archive = make_tar_xz(long_running_env_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    send_signal(&sitter, "USR1");
    wait_until("daemon 0.2.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the update restart"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset".to_string(),
            "start 0.2.0 update_restart=1".to_string(),
        ],
        "the SIGUSR1 update respawn must carry the env var"
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("SIGUSR1 received; checking for updates now"),
        "stderr: {stderr}"
    );
}

/// SIGUSR1 when the channel has nothing newer: the check runs (one extra
/// manifest fetch) but the daemon is left untouched — no restart, no exit.
#[test]
fn sigusr1_when_already_current_leaves_daemon_running() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(routes);

    // Hour-long check interval: only the SIGUSR1 may trigger a check.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });
    let startup_requests = requests.lock().unwrap().len();

    let stderr = stderr_path(dir.path());
    send_signal(&sitter, "USR1");
    wait_until(
        "the SIGUSR1 check against the 0.1.0 manifest",
        Duration::from_secs(15),
        || {
            requests.lock().unwrap().len() > startup_requests
                && read_or_empty(&stderr).contains("intentd 0.1.0 is already current")
        },
    );
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "an already-current check must not exit the sitter"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let starts = read_or_empty(&log_path)
        .lines()
        .filter(|line| line.starts_with("start "))
        .count();
    assert_eq!(starts, 1, "an already-current SIGUSR1 must not restart");
}

/// A SIGUSR1 check against a stalled update endpoint (accepts connections,
/// never responds) must not deafen the sitter: a SIGTERM arriving while that
/// check hangs shuts the sitter down promptly — the updater's own download
/// timeout is minutes long, so waiting the check out is not an option for
/// service management.
#[test]
fn sigterm_during_a_stalled_sigusr1_check_still_shuts_down() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, stall) = serve_stallable(routes);

    // Hour-long check interval: only the SIGUSR1 may trigger a check.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Stall the endpoint, then trigger the on-demand check; it hangs.
    stall.store(true, std::sync::atomic::Ordering::SeqCst);
    let stderr = stderr_path(dir.path());
    send_signal(&sitter, "USR1");
    wait_until(
        "the SIGUSR1 check to be in flight",
        Duration::from_secs(15),
        || read_or_empty(&stderr).contains("SIGUSR1 received; checking for updates now"),
    );

    // SIGTERM while the check hangs: the sitter must shut down promptly
    // (well under the stalled check's own timeout), forwarding the signal
    // to the daemon and exiting with its status as usual.
    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(
        status.code(),
        Some(0),
        "SIGTERM during a stalled check must shut down promptly"
    );
}

/// A SIGUSR1 that lands during a crash-backoff sleep cuts the wait short:
/// the check runs immediately, installs the published fix, and respawns it
/// well before the backoff would have elapsed.
#[test]
fn sigusr1_during_crash_backoff_checks_and_respawns_the_fix() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // 0.1.0 crash-loops; a 30s backoff (never elapsing within the test)
    // guarantees the SIGUSR1 below lands during the backoff sleep.
    preinstall(&paths, "0.1.0", &crash_script(7));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env(BACKOFF_INITIAL_ENV, "30000")
        .env(BACKOFF_CAP_ENV, "30000")
        .env(GIVE_UP_AFTER_ENV, "10000")
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    let stderr = stderr_path(dir.path());
    wait_until(
        "the crashed daemon to enter backoff",
        Duration::from_secs(15),
        || {
            read_or_empty(&log_path).contains("run")
                && read_or_empty(&stderr).contains("respawning intentd in")
        },
    );

    // Publish the fixed 0.2.0, then `kill -USR1` while the sitter is still
    // deep in its 30s backoff sleep.
    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    send_signal(&sitter, "USR1");
    // Well under the 30s backoff: the SIGUSR1 must cut the sleep short,
    // check, install 0.2.0, and respawn it.
    wait_until("daemon 0.2.0 to start", Duration::from_secs(10), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the update restart"
    );
    let runs = read_or_empty(&log_path)
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert_eq!(runs, 1, "crashing 0.1.0 must not have been respawned");
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));
}

/// SIGUSR2 ("update when idle"): the on-demand check installs the newer
/// published version but hands it to the daemon as SIGUSR2 instead of a
/// SIGTERM; the daemon stays up until it exits with the restart-for-update
/// code, and only then does the sitter respawn — the new version, marked as
/// an update restart, immediately (no backoff). The child also sees the
/// handshake advertised via `INTENTD_SITTER_IDLE_RESTART=1`.
#[test]
fn sigusr2_stages_update_and_respawns_when_daemon_exits_for_restart() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    let release = Barrier::new(dir.path(), "release");
    preinstall(&paths, "0.1.0", &idle_restart_script("0.1.0", &release));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    // Hour-long check interval: only the SIGUSR2 may check. A 30s backoff
    // (never elapsing within the test) proves the restart-for-update exit
    // respawns without one.
    let mut sitter = spawn_guarded(
        sitter_command(dir.path(), &base_url)
            .env_remove(UPDATE_RESTART_ENV)
            .env_remove(IDLE_RESTART_ENV)
            .env(CHECK_MIN_ENV, "3600000")
            .env(CHECK_MAX_ENV, "3600001")
            .env(BACKOFF_INITIAL_ENV, "30000")
            .env(BACKOFF_CAP_ENV, "30000")
            .env(KILL_TIMEOUT_ENV, "5000")
            .arg("serve"),
    );
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Publish 0.2.0, then `kill -USR2`: the sitter installs it and asks the
    // daemon to restart when idle.
    let archive = make_tar_xz(idle_restart_script("0.2.0", &release).as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    let stderr = stderr_path(dir.path());
    send_signal(&sitter, "USR2");
    wait_until(
        "daemon 0.1.0 to receive SIGUSR2 and the sitter to log the hand-off",
        Duration::from_secs(15),
        || {
            read_or_empty(&log_path).contains("usr2 0.1.0")
                && read_or_empty(&stderr).contains("asking daemon to restart when idle")
        },
    );
    // The daemon holds at the hand-off until the test releases it, so this
    // state is stable for as long as the assertions take: the sitter must
    // not have stopped it or respawned anything, and must still be
    // supervising it.
    let staged = read_or_empty(&log_path);
    assert!(
        !staged.contains("term") && !staged.contains("start 0.2.0"),
        "the staged daemon must be left running: {staged}"
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0"),
        "the install must be committed before the daemon is asked to restart"
    );
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must keep supervising the daemon it asked to restart"
    );

    // Release the daemon: it exits with the restart-for-update code, and —
    // well under the 30s backoff — the staged version must respawn at once.
    release.release();
    wait_until("daemon 0.2.0 to start", Duration::from_secs(10), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the staged restart"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset idle_restart=1".to_string(),
            "usr2 0.1.0".to_string(),
            "start 0.2.0 update_restart=1 idle_restart=1".to_string(),
            "term 0.2.0".to_string(),
        ],
        "SIGUSR2 hand-off, no SIGTERM to 0.1.0, update-marked respawn"
    );
    let stderr = read_or_empty(&stderr);
    assert!(
        stderr.contains("SIGUSR2 received; checking for updates now"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("installed intentd 0.2.0 (was 0.1.0); asking daemon to restart when idle"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("exited to restart for a staged update"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("exited unexpectedly") && !stderr.contains("respawning intentd in"),
        "the restart exit is neither a crash nor backed off: {stderr}"
    );
}

/// SIGUSR2 when the channel has nothing newer: the check runs but nothing
/// is signaled to the child — no SIGUSR2, no SIGTERM, no restart.
#[test]
fn sigusr2_when_already_current_signals_nothing_to_the_daemon() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // Never released: an already-current check must not even reach the hold.
    let release = Barrier::new(dir.path(), "release");
    preinstall(&paths, "0.1.0", &idle_restart_script("0.1.0", &release));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(routes);

    // Hour-long check interval: only the SIGUSR2 may trigger a check.
    let mut sitter = spawn_guarded(
        sitter_command(dir.path(), &base_url)
            .env(CHECK_MIN_ENV, "3600000")
            .env(CHECK_MAX_ENV, "3600001")
            .env(KILL_TIMEOUT_ENV, "5000")
            .arg("serve"),
    );
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });
    let startup_requests = requests.lock().unwrap().len();

    let stderr = stderr_path(dir.path());
    send_signal(&sitter, "USR2");
    wait_until(
        "the SIGUSR2 check against the 0.1.0 manifest",
        Duration::from_secs(15),
        || {
            requests.lock().unwrap().len() > startup_requests
                && read_or_empty(&stderr).contains("intentd 0.1.0 is already current")
        },
    );
    // Give a stray signal to the child time to be logged before asserting.
    // timing-guard: negative assertion
    thread::sleep(Duration::from_millis(200));
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "an already-current check must not exit the sitter"
    );
    assert_eq!(
        read_or_empty(&log_path).trim(),
        "start 0.1.0 update_restart=unset idle_restart=1",
        "an already-current SIGUSR2 must signal nothing to the daemon"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));
}

/// SIGUSR1 ("update now") keeps its semantics when it lands while a
/// SIGUSR2 idle-mode check is still in flight: the running check is
/// escalated, so when it installs the new version the daemon is stopped
/// (SIGTERM) and the new version respawned at once — not handed a SIGUSR2
/// and left to restart when idle.
#[test]
fn sigusr1_during_an_idle_mode_check_escalates_it_to_restart_now() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // Never released: the escalated check SIGTERMs the daemon instead.
    let release = Barrier::new(dir.path(), "release");
    preinstall(&paths, "0.1.0", &idle_restart_script("0.1.0", &release));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, hold, parked) = serve_holdable(Arc::clone(&routes));

    // Hour-long check interval: only the signals may check.
    let mut sitter = spawn_guarded(
        sitter_command(dir.path(), &base_url)
            .env_remove(UPDATE_RESTART_ENV)
            .env(CHECK_MIN_ENV, "3600000")
            .env(CHECK_MAX_ENV, "3600001")
            .env(KILL_TIMEOUT_ENV, "5000")
            .arg("serve"),
    );
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Publish 0.2.0 but hold the endpoint so the SIGUSR2 check stays in
    // flight; once its manifest request is parked, send SIGUSR1.
    let archive = make_tar_xz(idle_restart_script("0.2.0", &release).as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    hold.store(true, std::sync::atomic::Ordering::SeqCst);
    send_signal(&sitter, "USR2");
    wait_until(
        "the SIGUSR2 check to be parked at the endpoint",
        Duration::from_secs(15),
        || parked.load(std::sync::atomic::Ordering::SeqCst) >= 1,
    );
    let stderr = stderr_path(dir.path());
    send_signal(&sitter, "USR1");
    wait_until(
        "the in-flight check to be escalated",
        Duration::from_secs(15),
        || {
            read_or_empty(&stderr).contains(
                "SIGUSR1 received; an update check is already running, escalating it to restart now",
            )
        },
    );
    hold.store(false, std::sync::atomic::Ordering::SeqCst);

    wait_until("daemon 0.2.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the escalated restart"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset idle_restart=1".to_string(),
            "term 0.1.0".to_string(),
            "start 0.2.0 update_restart=1 idle_restart=1".to_string(),
            "term 0.2.0".to_string(),
        ],
        "SIGUSR1 mid-check must SIGTERM 0.1.0 and respawn 0.2.0 — no SIGUSR2 hand-off"
    );
    let stderr = read_or_empty(&stderr);
    assert!(
        stderr.contains("installed intentd 0.2.0 (was 0.1.0); restarting daemon"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("asking daemon to restart when idle"),
        "the escalated check must not promise an idle restart: {stderr}"
    );
}

/// A staged version the daemon never restarted into (`state.json` names an
/// installed version other than the running one) is caught by the periodic
/// check: "already current" relative to the manifest, but not the running
/// version, so the sitter forces the restart — graceful SIGTERM + respawn of
/// the staged version, marked as an update restart.
#[test]
fn periodic_check_force_restarts_a_staged_version_the_daemon_never_took() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &never_idle_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(UPDATE_RESTART_ENV)
        .env(CHECK_MIN_ENV, "300")
        .env(CHECK_MAX_ENV, "301")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Stage 0.2.0 exactly as a SIGUSR2 check leaves it behind a daemon that
    // never gets idle: installed and named by state.json, not running. The
    // 0.1.0 manifest is "not newer" than it, so the next periodic check
    // reports it as already current.
    preinstall(&paths, "0.2.0", &never_idle_script("0.2.0"));
    wait_until("daemon 0.2.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the forced restart"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset idle_restart=1".to_string(),
            "term 0.1.0".to_string(),
            "start 0.2.0 update_restart=1 idle_restart=1".to_string(),
            "term 0.2.0".to_string(),
        ],
        "the periodic check must SIGTERM the stale daemon and respawn the staged version"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("found staged intentd 0.2.0 (was 0.1.0); restarting daemon"),
        "stderr: {stderr}"
    );
}

/// A child exiting with the restart-for-update code while `state.json` still
/// names the running version is respawned on that same version at once —
/// no backoff, not counted as a crash — and, being same-version, without
/// the update-restart marker.
#[test]
fn restart_for_update_exit_with_unchanged_state_respawns_same_version_unmarked() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    let release = Barrier::new(dir.path(), "release");
    preinstall(&paths, "0.1.0", &restart_once_script("0.1.0", &release));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let (base_url, requests) = serve_recording(routes);

    // Hour-long check interval and a 30s backoff: the respawn below can
    // only happen promptly if the exit is neither checked nor backed off.
    let mut sitter = spawn_guarded(
        sitter_command(dir.path(), &base_url)
            .env_remove(UPDATE_RESTART_ENV)
            .env(CHECK_MIN_ENV, "3600000")
            .env(CHECK_MAX_ENV, "3600001")
            .env(BACKOFF_INITIAL_ENV, "30000")
            .env(BACKOFF_CAP_ENV, "30000")
            .env(KILL_TIMEOUT_ENV, "5000")
            .arg("serve"),
    );
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });
    let startup_requests = requests.lock().unwrap().len();

    // The daemon holds at the barrier before exiting: nothing may respawn
    // while it is still alive.
    wait_until(
        "daemon 0.1.0 to reach the hold",
        Duration::from_secs(10),
        || release.entered(),
    );
    assert_eq!(read_or_empty(&log_path).lines().count(), 1);
    assert!(sitter.try_wait().unwrap().is_none());

    release.release();
    wait_until(
        "daemon 0.1.0 to be respawned",
        Duration::from_secs(10),
        || read_or_empty(&log_path).lines().count() >= 2,
    );
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the restart exit"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 update_restart=unset idle_restart=1".to_string(),
            "start 0.1.0 update_restart=unset idle_restart=1".to_string(),
        ],
        "a same-version restart-for-update respawn must not carry the update marker"
    );
    assert_eq!(
        requests.lock().unwrap().len(),
        startup_requests,
        "the restart exit is not a failed start: no off-schedule re-check"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("intentd 0.1.0 exited to restart for a staged update; respawning"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("exited unexpectedly") && !stderr.contains("respawning intentd in"),
        "the restart exit is neither a crash nor backed off: {stderr}"
    );
}

#[test]
fn one_shot_subcommand_nonzero_exit_passes_through_without_respawn() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_script(7));

    // A non-`serve` invocation (e.g. `doctor`) is one-shot: a non-zero exit
    // is the daemon's answer, not a crash — no respawn, status passes through.
    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(BACKOFF_INITIAL_ENV, "50")
        .arg("doctor")
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(
        status.code(),
        Some(7),
        "one-shot exit status passes through"
    );

    let runs = read_or_empty(&daemon_log_path(dir.path()))
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert_eq!(runs, 1, "one-shot subcommands must run exactly once");
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(!stderr.contains("respawning"), "stderr: {stderr}");
}

/// Routes serving a fully installable 0.2.0 release (manifest + archive):
/// a one-shot must never even ask for it.
fn routes_with_release(base_url: &str, version: &str) -> (String, Vec<u8>, Vec<u8>) {
    let archive = make_tar_xz(args_dump_script().as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    let manifest = manifest_json(version, base_url, &asset, &sha);
    (asset, archive, manifest)
}

#[test]
fn one_shot_with_installed_version_never_touches_the_updater() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &args_dump_script());

    // A reachable manifest server offering a newer release: the one-shot
    // must make zero HTTP requests, install nothing, and leave state.json
    // untouched.
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let (asset, archive, manifest) = routes_with_release(&base_url, "0.2.0");
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(MANIFEST_PATH.to_string(), manifest);
    }

    let mut sitter = sitter_command(dir.path(), &base_url)
        .args(["doctor", "--verbose"])
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(0));

    assert_eq!(
        read_or_empty(&daemon_log_path(dir.path())),
        "doctor\n--verbose\n"
    );
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[] as &[String],
        "one-shot must not make any HTTP requests"
    );
    let state = state::load(&paths.state_path);
    assert_eq!(state.current_version.as_deref(), Some("0.1.0"));
    assert!(
        state.last_check_at.is_none(),
        "state.json must not be rewritten"
    );
    assert!(
        state.next_check_at.is_none(),
        "state.json must not be rewritten"
    );
    assert!(
        !paths.daemon_binary("0.2.0").exists(),
        "one-shot must not install"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        !stderr.contains("note: channel"),
        "no channel-mismatch notice when channels match; stderr: {stderr}"
    );
}

#[test]
fn one_shot_channel_mismatch_warns_and_runs_installed_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // preinstall records channel `stable` in state.json.
    preinstall(&paths, "0.1.0", &args_dump_script());
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(routes);

    // The channel flag only governs updater behavior, which one-shots don't
    // have: a mismatch prints a notice but still runs the installed daemon.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .args(["--sitter-channel=beta", "doctor"])
        .spawn()
        .unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(0));

    assert_eq!(read_or_empty(&daemon_log_path(dir.path())), "doctor\n");
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[] as &[String],
        "one-shot must not make any HTTP requests"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains(
            "note: channel beta requested but the installed daemon was installed \
             from channel stable"
        ),
        "stderr: {stderr}"
    );
}

#[test]
fn one_shot_with_nothing_installed_fails_fast_without_installing() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());

    // A reachable server could bootstrap-install, but a one-shot must fail
    // fast with guidance instead. Empty passthrough args are also one-shot.
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let (asset, archive, manifest) = routes_with_release(&base_url, "0.2.0");
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(MANIFEST_PATH.to_string(), manifest);
    }

    let mut sitter = sitter_command(dir.path(), &base_url).spawn().unwrap();
    let status = wait_exit(&mut sitter, Duration::from_secs(30));
    assert_eq!(status.code(), Some(1), "expected a non-zero fail-fast exit");

    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("no intentd daemon is installed for channel stable"),
        "stderr: {stderr}"
    );
    assert!(stderr.contains("intentd serve"), "stderr: {stderr}");
    assert!(
        stderr.contains("brew services start intentd"),
        "stderr: {stderr}"
    );
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[] as &[String],
        "one-shot must not make any HTTP requests"
    );
    assert!(
        !daemon_log_path(dir.path()).exists(),
        "no daemon must have run"
    );
    assert!(
        !paths.daemon_binary("0.2.0").exists(),
        "one-shot must not install"
    );
}

/// Run a second sitter process to completion against the same data dir,
/// capturing stdout/stderr (without truncating the serve sitter's logs).
fn run_one_shot(data_dir: &Path, base_url: &str, args: &[&str]) -> std::process::Output {
    Command::new(SITTER_BIN)
        .env_remove(CHANNEL_ENV)
        .env(DATA_DIR_ENV, data_dir)
        .env(MANIFEST_BASE_URL_ENV, base_url)
        .env(FAKE_DAEMON_LOG, daemon_log_path(data_dir))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn restart_command_respawns_state_version_without_exiting_sitter() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_script("0.1.0"));
    let routes: Routes = Arc::new(Mutex::new(HashMap::from([(
        MANIFEST_PATH.to_string(),
        manifest_bare("0.1.0"),
    )])));
    let base_url = serve(Arc::clone(&routes));

    // Hour-long check interval: only the SIGHUP may restart the child.
    let mut sitter = sitter_command(dir.path(), &base_url)
        .env_remove(CHANNEL_ENV)
        .env(CHECK_MIN_ENV, "3600000")
        .env(CHECK_MAX_ENV, "3600001")
        .env(KILL_TIMEOUT_ENV, "5000")
        .arg("serve")
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon 0.1.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });
    assert_eq!(
        read_or_empty(&paths.pid_path).trim(),
        sitter.id().to_string(),
        "serve mode must write its pid to sitter.pid"
    );

    // Publish beta 0.2.0 and force-install it (`sitter channel beta
    // --redownload`); the running child must stay on 0.1.0.
    let archive = make_tar_xz(long_running_script("0.2.0").as_bytes());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            BETA_MANIFEST_PATH.to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }
    let output = run_one_shot(
        dir.path(),
        &base_url,
        &["sitter", "channel", "beta", "--redownload"],
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );
    assert!(
        !read_or_empty(&log_path).contains("start 0.2.0"),
        "--redownload must not restart the running daemon"
    );

    // `intentd restart` respawns the child on the state.json version.
    let output = run_one_shot(dir.path(), &base_url, &["restart"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("restarting intentd"), "stdout: {stdout}");
    wait_until("daemon 0.2.0 to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.2.0")
    });
    assert!(
        sitter.try_wait().unwrap().is_none(),
        "the sitter must survive the restart"
    );

    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    let lines: Vec<String> = read_or_empty(&log_path).lines().map(String::from).collect();
    assert_eq!(
        lines,
        vec![
            "start 0.1.0 :: serve".to_string(),
            "start 0.2.0 :: serve".to_string(),
        ],
        "each version must run exactly once"
    );
    assert!(
        !paths.pid_path.exists(),
        "the pidfile must be removed on exit"
    );
}

#[test]
fn restart_without_live_sitter_or_with_stale_pidfile_fails() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    let base_url = dead_url();

    // No pidfile at all.
    let output = run_one_shot(dir.path(), &base_url, &["restart"]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no running supervised intentd"),
        "stderr: {stderr}"
    );

    // Stale pidfile: the pid of an already-reaped process reads as absent.
    let mut dead = Command::new("true").spawn().unwrap();
    let dead_pid = dead.id();
    dead.wait().unwrap();
    fs::create_dir_all(paths.pid_path.parent().unwrap()).unwrap();
    fs::write(&paths.pid_path, format!("{dead_pid}\n")).unwrap();
    let output = run_one_shot(dir.path(), &base_url, &["restart"]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no running supervised intentd"),
        "stderr: {stderr}"
    );

    assert!(
        !daemon_log_path(dir.path()).exists(),
        "`intentd restart` must never spawn the daemon"
    );
}

#[test]
fn double_dash_restart_forwards_verbatim_to_the_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &args_dump_script());

    let output = run_one_shot(dir.path(), &dead_url(), &["--", "restart"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        read_or_empty(&daemon_log_path(dir.path())),
        "--\nrestart\n",
        "after `--` a literal restart must reach the daemon verbatim"
    );
}

#[test]
fn sitter_initiated_stop_does_not_respawn() {
    let _serial = SERVE_LOOP_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &long_running_script("0.1.0"));

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(KILL_TIMEOUT_ENV, "5000")
        .args(["serve", "--resume-all"])
        .spawn()
        .unwrap();
    let log_path = daemon_log_path(dir.path());
    wait_until("daemon to start", Duration::from_secs(15), || {
        read_or_empty(&log_path).contains("start 0.1.0")
    });

    // Forwarded SIGINT: the daemon exits 0 gracefully, the sitter passes
    // that status through and never respawns.
    send_signal(&sitter, "INT");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_eq!(status.code(), Some(0));

    // timing-guard: negative assertion
    thread::sleep(Duration::from_millis(300));
    let starts = read_or_empty(&log_path)
        .lines()
        .filter(|line| line.starts_with("start "))
        .count();
    assert_eq!(starts, 1, "sitter-initiated stop must not respawn");
}
