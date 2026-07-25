//! End-to-end supervisor tests: drive the real sitter binary against a
//! local HTTP fixture server and fake-daemon shell scripts (unix only; the
//! windows code paths are cfg-compiled but exercised via CI builds).
//!
//! Timing runs at millisecond scale through the `INTENTD_SITTER_*_MS` env
//! overrides so no test sleeps for hours.

#![cfg(unix)]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use intentd_sitter::manifest::TARGET_TRIPLE;
use intentd_sitter::paths::{SitterPaths, DAEMON_BIN_NAME, DATA_DIR_ENV};
use intentd_sitter::state::{self, SitterState};
use intentd_sitter::supervisor::{
    BACKOFF_CAP_ENV, BACKOFF_INITIAL_ENV, BACKOFF_RESET_ENV, CHECK_MAX_ENV, CHECK_MIN_ENV,
    KILL_TIMEOUT_ENV, MANIFEST_BASE_URL_ENV,
};

const SITTER_BIN: &str = env!("CARGO_BIN_EXE_intentd-sitter");

/// Env var the fake daemon scripts log to (set on the sitter, inherited by
/// the child — which doubles as an env-inheritance check).
const FAKE_DAEMON_LOG: &str = "FAKE_DAEMON_LOG";

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
            Ok(_) if header != "\r\n" && !header.is_empty() => continue,
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

/// A base URL whose port refuses connections (network down).
fn dead_url() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    url
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
fn crash_respawn_backs_off_exponentially() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0", &crash_script(7));

    let mut sitter = sitter_command(dir.path(), &dead_url())
        .env(BACKOFF_INITIAL_ENV, "50")
        .env(BACKOFF_CAP_ENV, "400")
        .env(BACKOFF_RESET_ENV, "60000")
        .arg("serve")
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_millis(1300));
    send_signal(&sitter, "TERM");
    let status = wait_exit(&mut sitter, Duration::from_secs(10));
    assert_ne!(
        status.code(),
        Some(0),
        "crash-looping sitter must not exit 0"
    );

    // Doubling from 50ms (spawns at ~0/50/150/350/750/1150ms) gives ~6 runs
    // in 1.3s; a constant 50ms delay would give ~26. Bound both sides.
    let runs = read_or_empty(&daemon_log_path(dir.path()))
        .lines()
        .filter(|line| *line == "run")
        .count();
    assert!(
        (3..=9).contains(&runs),
        "expected backed-off respawns, got {runs} runs"
    );
    let stderr = read_or_empty(&stderr_path(dir.path()));
    assert!(
        stderr.contains("exited unexpectedly (exit code 7)"),
        "stderr: {stderr}"
    );
    assert!(stderr.contains("respawning intentd in"), "stderr: {stderr}");
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

#[test]
fn sitter_initiated_stop_does_not_respawn() {
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
