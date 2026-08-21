//! End-to-end supervisor tests: drive the real sitter binary against a
//! local HTTP fixture server and fake-daemon shell scripts (unix only; the
//! windows code paths are cfg-compiled but exercised via CI builds).
//!
//! Timing runs at millisecond scale through the `INTENTD_SITTER_*_MS` env
//! overrides so no test sleeps for hours.

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
    GIVE_UP_AFTER_ENV, KILL_TIMEOUT_ENV, MANIFEST_BASE_URL_ENV,
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
    log.lock().unwrap().push(path.to_string());
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

/// Fake daemon: log one line, stay up `secs`, then crash with `code` — a
/// daemon that serves for a while and dies, not one that can never start.
fn long_lived_crash_script(secs: &str, code: i32) -> String {
    format!(
        "#!/bin/sh\n\
         echo run >> \"${FAKE_DAEMON_LOG}\"\n\
         sleep {secs}\n\
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
    thread::sleep(Duration::from_millis(5000));
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

    thread::sleep(Duration::from_millis(300));
    let starts = read_or_empty(&log_path)
        .lines()
        .filter(|line| line.starts_with("start "))
        .count();
    assert_eq!(starts, 1, "sitter-initiated stop must not respawn");
}
