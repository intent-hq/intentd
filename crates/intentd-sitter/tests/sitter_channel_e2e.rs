//! End-to-end tests for the intercepted `intentd sitter channel` command:
//! drive the real sitter binary against a temp data dir and a local HTTP
//! fixture server (unix only, matching `supervisor_e2e.rs`). The command is
//! sitter-owned: it must never spawn the daemon, and only `--redownload`
//! may touch the network.

#![cfg(unix)]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;

use sha2::{Digest, Sha256};

use intentd_sitter::cli::{Channel, CHANNEL_ENV};
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

/// A base URL whose port refuses requests (network down).
///
/// The listener stays bound for the life of the process and a detached
/// thread accepts each connection and immediately drops it, so the sitter's
/// update check deterministically fails. Binding and then dropping the
/// listener (the previous approach) released the ephemeral port back to the
/// OS, which could reassign it to a sibling test's fixture server before the
/// sitter connected — turning the "dead" URL into a live one under parallel
/// test load (intent-hq/monorepo#1211).
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

/// Install a fake args-dumping daemon script as `versions/<version>/intentd`
/// and point `state.json` at it.
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

fn daemon_log_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("fake-daemon.log")
}

/// Run the sitter to completion, capturing stdout/stderr. `INTENTD_CHANNEL`
/// is scrubbed so the host environment never leaks into origin assertions.
fn run_sitter(data_dir: &Path, base_url: &str, envs: &[(&str, &str)], args: &[&str]) -> Output {
    let mut cmd = Command::new(SITTER_BIN);
    cmd.env_remove(CHANNEL_ENV)
        .env(DATA_DIR_ENV, data_dir)
        .env(MANIFEST_BASE_URL_ENV, base_url)
        .env(FAKE_DAEMON_LOG, daemon_log_path(data_dir))
        .args(args);
    for (key, value) in envs {
        cmd.env(key, value);
    }
    cmd.output().unwrap()
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn set_form_writes_config_and_exits_without_daemon_or_network() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    // A reachable server offering a release: the set form must not call it.
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let archive = make_tar_xz(b"new daemon");
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            "/channel-beta/beta.json".to_string(),
            manifest_json("0.2.0", &base_url, &asset, &sha),
        );
    }

    let output = run_sitter(dir.path(), &base_url, &[], &["sitter", "channel", "beta"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );

    assert_eq!(
        fs::read_to_string(&paths.config_path).unwrap(),
        "channel = \"beta\"\n"
    );

    let alpha_output = run_sitter(dir.path(), &base_url, &[], &["sitter", "channel", "alpha"]);
    assert_eq!(
        alpha_output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&alpha_output)
    );
    assert_eq!(
        fs::read_to_string(&paths.config_path).unwrap(),
        "channel = \"alpha\"\n"
    );
    assert!(
        stdout_of(&alpha_output).contains("channel alpha pinned"),
        "stdout: {}",
        stdout_of(&alpha_output)
    );

    let stdout = stdout_of(&output);
    assert!(stdout.contains("channel beta pinned"), "stdout: {stdout}");
    assert!(stdout.contains("`intentd restart`"), "stdout: {stdout}");
    assert!(
        stdout.contains("brew services restart intentd")
            && stdout.contains("systemctl --user restart intentd"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("next periodic update check"),
        "stdout: {stdout}"
    );
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[] as &[String],
        "set form without --redownload must not make HTTP requests"
    );
    assert!(
        !daemon_log_path(dir.path()).exists(),
        "sitter channel must never spawn the daemon"
    );
}

#[test]
fn get_form_prints_effective_channel_and_origin() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    let base_url = dead_url();
    let get = &["sitter", "channel"][..];

    // No pin anywhere: the built-in default.
    let output = run_sitter(dir.path(), &base_url, &[], get);
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(stdout_of(&output), "stable (from default)\n");

    // Config pin.
    fs::create_dir_all(paths.config_path.parent().unwrap()).unwrap();
    fs::write(&paths.config_path, "channel = \"beta\"\n").unwrap();
    let output = run_sitter(dir.path(), &base_url, &[], get);
    assert_eq!(stdout_of(&output), "beta (from config)\n");
    fs::write(&paths.config_path, "channel = \"alpha\"\n").unwrap();
    let output = run_sitter(dir.path(), &base_url, &[], get);
    assert_eq!(stdout_of(&output), "alpha (from config)\n");

    // Env overrides config; flag overrides env.
    let output = run_sitter(dir.path(), &base_url, &[(CHANNEL_ENV, "stable")], get);
    assert_eq!(stdout_of(&output), "stable (from env)\n");
    let output = run_sitter(
        dir.path(),
        &base_url,
        &[(CHANNEL_ENV, "stable")],
        &["--sitter-channel=beta", "sitter", "channel"],
    );
    assert_eq!(stdout_of(&output), "beta (from flag)\n");
    let output = run_sitter(
        dir.path(),
        &base_url,
        &[(CHANNEL_ENV, "stable")],
        &["--sitter-channel=alpha", "sitter", "channel"],
    );
    assert_eq!(stdout_of(&output), "alpha (from flag)\n");

    assert!(!daemon_log_path(dir.path()).exists());
}

#[test]
fn redownload_force_installs_older_manifest_version() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    // Installed 0.2.0 (e.g. from beta); the stable manifest points at the
    // older 0.1.0 — the newer-only comparison must be bypassed.
    preinstall(&paths, "0.2.0");
    let routes: Routes = Arc::new(Mutex::new(HashMap::new()));
    let (base_url, requests) = serve_recording(Arc::clone(&routes));
    let archive = make_tar_xz(b"stable daemon 0.1.0");
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let sha = sha256_hex(&archive);
    {
        let mut routes = routes.lock().unwrap();
        routes.insert(format!("/{asset}"), archive);
        routes.insert(
            "/channel-stable/stable.json".to_string(),
            manifest_json("0.1.0", &base_url, &asset, &sha),
        );
    }

    let output = run_sitter(
        dir.path(),
        &base_url,
        &[],
        &["sitter", "channel", "stable", "--redownload"],
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );

    assert_eq!(
        fs::read_to_string(&paths.config_path).unwrap(),
        "channel = \"stable\"\n"
    );
    let state = state::load(&paths.state_path);
    assert_eq!(state.current_version.as_deref(), Some("0.1.0"));
    assert_eq!(state.channel, Channel::Stable);
    assert!(paths.daemon_binary("0.1.0").exists());
    let stdout = stdout_of(&output);
    assert!(
        stdout.contains("installed intentd 0.1.0 from channel stable"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("active after a restart"),
        "stdout: {stdout}"
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
        "redownload must not spawn or restart the daemon"
    );
}

#[test]
fn redownload_failure_still_writes_pin_and_exits_nonzero() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());

    let output = run_sitter(
        dir.path(),
        &dead_url(),
        &[],
        &["sitter", "channel", "beta", "--redownload"],
    );
    assert_eq!(output.status.code(), Some(1));

    assert_eq!(
        fs::read_to_string(&paths.config_path).unwrap(),
        "channel = \"beta\"\n",
        "the pin must be written before the install is attempted"
    );
    let stderr = stderr_of(&output);
    assert!(
        stderr.contains("failed to install from channel beta"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("the channel pin was still written"),
        "stderr: {stderr}"
    );
}

#[test]
fn usage_errors_exit_nonzero_without_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");
    let base_url = dead_url();

    for (args, expected) in [
        (
            &["sitter", "channel", "--redownload"][..],
            "--redownload requires a channel value",
        ),
        (&["sitter", "channel", "nightly"][..], "invalid channel"),
        (&["sitter", "restart"][..], "unknown sitter subcommand"),
        (&["sitter"][..], "missing sitter subcommand"),
    ] {
        let output = run_sitter(dir.path(), &base_url, &[], args);
        assert_eq!(output.status.code(), Some(2), "args: {args:?}");
        let stderr = stderr_of(&output);
        assert!(
            stderr.contains(expected),
            "args: {args:?}, stderr: {stderr}"
        );
    }

    assert!(!paths.config_path.exists(), "no pin may be written");
    assert!(
        !daemon_log_path(dir.path()).exists(),
        "usage errors must never spawn the daemon"
    );
}

#[test]
fn double_dash_still_forwards_sitter_to_the_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let paths = SitterPaths::from_data_dir(dir.path());
    preinstall(&paths, "0.1.0");

    let output = run_sitter(
        dir.path(),
        &dead_url(),
        &[],
        &["--", "sitter", "channel", "beta"],
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr: {}",
        stderr_of(&output)
    );

    assert_eq!(
        fs::read_to_string(daemon_log_path(dir.path())).unwrap(),
        "--\nsitter\nchannel\nbeta\n",
        "after `--` the literal args must reach the daemon verbatim"
    );
    assert!(
        !paths.config_path.exists(),
        "the forwarded form must not write the config pin"
    );
}
