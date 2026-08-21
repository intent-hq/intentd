//! End-to-end tests for the intercepted `intentd update [--check]` command:
//! drive the real sitter binary against a temp data dir and a local HTTP
//! fixture server (unix only, matching `sitter_channel_e2e.rs`). The command
//! is sitter-owned: it must never spawn the daemon; `--check` must never
//! download an archive; the full form installs newer-only and SIGHUPs a
//! running supervised sitter.

#![cfg(unix)]

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use intentd_sitter::cli::CHANNEL_ENV;
use intentd_sitter::manifest::TARGET_TRIPLE;
use intentd_sitter::paths::{SitterPaths, DAEMON_BIN_NAME, DATA_DIR_ENV};
use intentd_sitter::state::{self, SitterState};
use intentd_sitter::supervisor::MANIFEST_BASE_URL_ENV;

const SITTER_BIN: &str = env!("CARGO_BIN_EXE_intentd-sitter");

/// Env var the fake daemon script logs to; the log file existing at all
/// proves a daemon was spawned.
const FAKE_DAEMON_LOG: &str = "FAKE_DAEMON_LOG";

type Routes = Arc<Mutex<HashMap<String, Vec<u8>>>>;
type RequestLog = Arc<Mutex<Vec<String>>>;

/// Minimal HTTP/1.1 fixture server plus a log of every request path, so
/// tests can assert the sitter made (or did not make) HTTP requests.
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

/// A base URL whose port refuses requests (network down). The listener
/// stays bound for the life of the process so a sibling test's fixture
/// server can never be assigned the same port (intent-hq/monorepo#1211).
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

/// Install a fake daemon script as `versions/<version>/intentd` and point
/// `state.json` at it.
fn preinstall(paths: &SitterPaths, version: &str) {
    let bin = paths.daemon_binary(version);
    fs::create_dir_all(bin.parent().unwrap()).unwrap();
    fs::write(
        &bin,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"${FAKE_DAEMON_LOG}\"\nexit 0\n",
    )
    .unwrap();
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

/// Publish a release on `channel`: manifest + archive routes.
fn publish_channel(
    routes: &Routes,
    base_url: &str,
    channel: &str,
    version: &str,
    bin_contents: &[u8],
) -> String {
    let asset = format!("intentd-{channel}-{TARGET_TRIPLE}.tar.xz");
    let archive = make_tar_xz(bin_contents);
    let sha = sha256_hex(&archive);
    let mut routes = routes.lock().unwrap();
    routes.insert(format!("/{asset}"), archive);
    routes.insert(
        format!("/channel-{channel}/{channel}.json"),
        manifest_json(version, base_url, &asset, &sha),
    );
    asset
}

/// Publish a release on the stable channel: manifest + archive routes.
fn publish_stable(routes: &Routes, base_url: &str, version: &str, bin_contents: &[u8]) -> String {
    publish_channel(routes, base_url, "stable", version, bin_contents)
}

fn daemon_log_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("fake-daemon.log")
}

/// Run the sitter to completion, capturing stdout/stderr. `INTENTD_CHANNEL`
/// is scrubbed so the host environment never leaks into channel resolution.
fn run_sitter(data_dir: &Path, base_url: &str, args: &[&str]) -> Output {
    Command::new(SITTER_BIN)
        .env_remove(CHANNEL_ENV)
        .env(DATA_DIR_ENV, data_dir)
        .env(MANIFEST_BASE_URL_ENV, base_url)
        .env(FAKE_DAEMON_LOG, daemon_log_path(data_dir))
        .args(args)
        .output()
        .unwrap()
}

/// Like [`run_sitter`] but with `INTENTD_CHANNEL` set instead of scrubbed.
fn run_sitter_with_channel_env(
    data_dir: &Path,
    base_url: &str,
    channel: &str,
    args: &[&str],
) -> Output {
    Command::new(SITTER_BIN)
        .env(CHANNEL_ENV, channel)
        .env(DATA_DIR_ENV, data_dir)
        .env(MANIFEST_BASE_URL_ENV, base_url)
        .env(FAKE_DAEMON_LOG, daemon_log_path(data_dir))
        .args(args)
        .output()
        .unwrap()
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn check_reports_update_available_without_downloading() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let asset = publish_stable(&routes, &base_url, "0.2.0", b"new daemon 0.2.0");

    let output = run_sitter(dir.path(), &base_url, &["update", "--check"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("installed: intentd 0.1.0"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("latest on channel stable: intentd 0.2.0"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("update available; run `intentd update` to install it"),
        "stdout: {stdout}"
    );

    assert_eq!(
        requests.lock().unwrap().as_slice(),
        ["/channel-stable/stable.json".to_string()],
        "--check must fetch only the manifest, never the archive {asset}"
    );
    assert!(
        !paths.daemon_binary("0.2.0").exists(),
        "--check must not install anything"
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.1.0"),
        "--check must not touch state.json"
    );
    assert!(
        !daemon_log_path(dir.path()).exists(),
        "`intentd update` must never spawn the daemon"
    );
}

#[test]
fn check_honors_flag_and_env_channel_precedence() {
    // `update` must resolve the effective channel like serve mode does:
    // --sitter-channel > INTENTD_CHANNEL > config pin > stable default.
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    publish_stable(&routes, &base_url, "0.1.0", b"stable daemon 0.1.0");
    publish_channel(&routes, &base_url, "beta", "0.2.0", b"beta daemon 0.2.0");
    publish_channel(&routes, &base_url, "alpha", "0.3.0", b"alpha daemon 0.3.0");

    // Env override selects beta.
    let output = run_sitter_with_channel_env(dir.path(), &base_url, "beta", &["update", "--check"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("latest on channel beta: intentd 0.2.0"),
        "stdout: {stdout}"
    );

    // Env override selects alpha.
    let output =
        run_sitter_with_channel_env(dir.path(), &base_url, "alpha", &["update", "--check"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("latest on channel alpha: intentd 0.3.0"),
        "stdout: {stdout}"
    );

    // Flag beats env: --sitter-channel stable while the env says beta.
    let output = run_sitter_with_channel_env(
        dir.path(),
        &base_url,
        "beta",
        &["update", "--check", "--sitter-channel", "stable"],
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("latest on channel stable: intentd 0.1.0"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("already up to date"), "stdout: {stdout}");

    assert_eq!(
        requests.lock().unwrap().as_slice(),
        [
            "/channel-beta/beta.json".to_string(),
            "/channel-alpha/alpha.json".to_string(),
            "/channel-stable/stable.json".to_string(),
        ],
        "each check must fetch exactly the resolved channel's manifest"
    );
}

#[test]
fn check_reports_already_up_to_date_and_nothing_installed() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, _requests) = serve_recording(Arc::clone(&routes));
    publish_stable(&routes, &base_url, "0.2.0", b"daemon 0.2.0");

    // Nothing installed yet: an update is available.
    let output = run_sitter(dir.path(), &base_url, &["update", "--check"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_of(&output);
    assert!(stdout.contains("installed: none"), "stdout: {stdout}");
    assert!(stdout.contains("update available"), "stdout: {stdout}");

    // Already on the manifest version: up to date.
    preinstall(&paths, "0.2.0");
    let output = run_sitter(dir.path(), &base_url, &["update", "--check"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("installed: intentd 0.2.0"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("already up to date"), "stdout: {stdout}");
    assert!(!stdout.contains("update available"), "stdout: {stdout}");
}

#[test]
fn update_installs_newer_version_and_reports_no_running_service() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let asset = publish_stable(&routes, &base_url, "0.2.0", b"new daemon 0.2.0");

    let output = run_sitter(dir.path(), &base_url, &["update"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("installed intentd 0.2.0 from channel stable (was 0.1.0)"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("no running supervised intentd found"),
        "stdout: {stdout}"
    );

    assert!(paths.daemon_binary("0.2.0").exists());
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        [
            "/channel-stable/stable.json".to_string(),
            format!("/{asset}")
        ],
        "expected exactly one manifest fetch and one archive download"
    );
    assert!(
        !daemon_log_path(dir.path()).exists(),
        "`intentd update` must never spawn the daemon"
    );
}

#[test]
fn update_when_already_current_is_a_noop() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.2.0");
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let asset = publish_stable(&routes, &base_url, "0.2.0", b"daemon 0.2.0");

    let output = run_sitter(dir.path(), &base_url, &["update"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("already up to date: intentd 0.2.0 (channel stable)"),
        "stdout: {stdout}"
    );
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        ["/channel-stable/stable.json".to_string()],
        "an up-to-date check must not download the archive: {asset}"
    );
}

#[test]
fn update_signals_running_supervised_sitter() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, _requests) = serve_recording(Arc::clone(&routes));
    publish_stable(&routes, &base_url, "0.2.0", b"new daemon 0.2.0");

    // Stand in for a serve-mode sitter: a shell that logs SIGHUP receipt.
    // Writing its pid to sitter.pid is exactly what serve mode does.
    let hup_log = dir.path().join("hup.log");
    let mut fake_sitter = Command::new("/bin/sh")
        .arg("-c")
        .arg(format!(
            "trap 'echo HUP >> \"{}\"' HUP; while :; do sleep 0.1; done",
            hup_log.display()
        ))
        .spawn()
        .unwrap();
    fs::create_dir_all(paths.pid_path.parent().unwrap()).unwrap();
    fs::write(&paths.pid_path, format!("{}\n", fake_sitter.id())).unwrap();

    let output = run_sitter(dir.path(), &base_url, &["update"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("installed intentd 0.2.0 from channel stable (was 0.1.0)"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("restarting intentd: sent SIGHUP"),
        "stdout: {stdout}"
    );

    // The trap handler appends asynchronously; poll briefly.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline
        && !fs::read_to_string(&hup_log).is_ok_and(|s| s.contains("HUP"))
    {
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        fs::read_to_string(&hup_log).is_ok_and(|s| s.contains("HUP")),
        "the supervised sitter must receive SIGHUP"
    );

    let _ = fake_sitter.kill();
    let _ = fake_sitter.wait();
}

#[test]
fn update_with_channel_override_skips_restart_of_mismatched_service() {
    // `intentd update --sitter-channel beta` while the running service
    // follows stable (the default): the beta binary is installed, but the
    // service must NOT be SIGHUP'd onto a channel it does not follow.
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, _requests) = serve_recording(Arc::clone(&routes));
    publish_channel(&routes, &base_url, "beta", "0.2.0", b"beta daemon 0.2.0");

    let hup_log = dir.path().join("hup.log");
    let mut fake_sitter = Command::new("/bin/sh")
        .arg("-c")
        .arg(format!(
            "trap 'echo HUP >> \"{}\"' HUP; while :; do sleep 0.1; done",
            hup_log.display()
        ))
        .spawn()
        .unwrap();
    fs::create_dir_all(paths.pid_path.parent().unwrap()).unwrap();
    fs::write(&paths.pid_path, format!("{}\n", fake_sitter.id())).unwrap();

    let output = run_sitter(
        dir.path(),
        &base_url,
        &["update", "--sitter-channel", "beta"],
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("installed intentd 0.2.0 from channel beta (was 0.1.0)"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("not restarting the running service"),
        "stdout: {stdout}"
    );
    assert!(
        !stdout.contains("sent SIGHUP"),
        "must not signal a service on a different channel; stdout: {stdout}"
    );

    // Give any stray (buggy) SIGHUP a moment to land, then assert none did.
    thread::sleep(Duration::from_millis(300));
    assert!(
        !fs::read_to_string(&hup_log).is_ok_and(|s| s.contains("HUP")),
        "the supervised sitter must not receive SIGHUP on a channel mismatch"
    );

    let _ = fake_sitter.kill();
    let _ = fake_sitter.wait();
}

#[test]
fn update_failure_exits_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");

    for args in [&["update"][..], &["update", "--check"][..]] {
        let output = run_sitter(dir.path(), &dead_url(), args);
        assert_eq!(output.status.code(), Some(1), "args: {args:?}");
        let stderr = stderr_of(&output);
        assert!(
            stderr.contains("failed"),
            "args: {args:?}, stderr: {stderr}"
        );
    }
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.1.0"),
        "a failed update must not touch state.json"
    );
}

#[test]
fn update_usage_errors_exit_nonzero_without_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    let base_url = dead_url();

    for (args, expected) in [
        (&["update", "now"][..], "unexpected argument"),
        (&["update", "--force"][..], "unexpected argument"),
    ] {
        let output = run_sitter(dir.path(), &base_url, args);
        assert_eq!(output.status.code(), Some(2), "args: {args:?}");
        let stderr = stderr_of(&output);
        assert!(
            stderr.contains(expected),
            "args: {args:?}, stderr: {stderr}"
        );
    }
    assert!(
        !daemon_log_path(dir.path()).exists(),
        "usage errors must never spawn the daemon"
    );
}

#[test]
fn double_dash_forwards_update_to_the_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");

    let output = run_sitter(dir.path(), &dead_url(), &["--", "update"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );
    assert_eq!(
        fs::read_to_string(daemon_log_path(dir.path())).unwrap(),
        "--\nupdate\n",
        "after `--` a literal update must reach the daemon verbatim"
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.1.0"),
        "the forwarded form must not update anything"
    );
}
