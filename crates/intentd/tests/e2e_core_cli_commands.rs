//! E2E tests for CLI subcommands (status, stop, doctor, pair) to exercise
//! intentd binary coverage through daemon control paths.

#![cfg(unix)]

mod common;

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use uuid::Uuid;

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn spawn_daemon(data_dir: &PathBuf) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_TCP_PORT", "0")
        .env_remove("INTENTD_AUTH_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
}

/// Wait for the daemon UDS to accept, failing fast (with the daemon log) if
/// the child dies first. Shares the coverage-aware startup budget with the
/// other e2e harnesses via `common::await_daemon_listening`.
async fn await_socket(daemon: &mut Daemon, socket: &Path) {
    let log_path = daemon.data_dir.join("daemon.log");
    common::await_daemon_listening(&mut daemon.child, socket, &log_path).await;
}

#[tokio::test]
async fn doctor_checks_data_dir_and_migrations() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");

    let child = spawn_daemon(&data_dir);
    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    await_socket(&mut daemon, &socket).await;

    // Run `intentd doctor` command
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("doctor")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_TCP_PORT", "0")
        .output()
        .expect("run intentd doctor");

    assert!(
        output.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Assert that doctor checks the key health indicators
    assert!(
        stdout.contains("[ok] data dir writable"),
        "doctor should check data dir: {stdout}"
    );
    assert!(
        stdout.contains("[ok] sqlite openable"),
        "doctor should check sqlite: {stdout}"
    );
    assert!(
        stdout.contains("[ok] migrations current"),
        "doctor should check migrations: {stdout}"
    );
    assert!(
        stdout.contains("[ok] migration 0004 (agent_session) applied"),
        "doctor should verify critical migration: {stdout}"
    );
}

#[tokio::test]
async fn status_reports_down_when_daemon_not_running() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");

    // Run status without a running daemon
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("status")
        .env("INTENTD_DATA_DIR", &data_dir)
        .output()
        .expect("run intentd status");

    // Status should exit with failure when daemon is down
    assert!(
        !output.status.success(),
        "status should fail when daemon down"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("intentd: down"),
        "status should report daemon down: {stdout}"
    );
    assert!(
        stdout.contains("not reachable"),
        "status should mention socket unreachable: {stdout}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn stop_succeeds_when_daemon_not_running() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");

    // Run stop without a running daemon
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("stop")
        .env("INTENTD_DATA_DIR", &data_dir)
        .output()
        .expect("run intentd stop");

    // Stop should succeed (idempotent) when daemon is not running
    assert!(
        output.status.success(),
        "stop should succeed when daemon not running"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("not running"),
        "stop should report not running: {stdout}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Spawn a daemon with both UDS and TCP (WSS) listeners, as `intentd pair`
/// requires a running TCP listener to build the payload. The WSS listener is
/// enabled via `server.wsApi.enabled` in config.toml. `token` fixes the bearer
/// token via `INTENTD_AUTH_TOKEN`; `None` uses the file-backed secrets store
/// (required by rotation tests — an env-fixed token cannot rotate).
fn spawn_daemon_both(data_dir: &PathBuf, token: Option<&str>) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_TCP_PORT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    match token {
        Some(token) => cmd.env("INTENTD_AUTH_TOKEN", token),
        None => cmd.env_remove("INTENTD_AUTH_TOKEN"),
    };
    cmd.spawn().expect("spawn intentd serve (ws api enabled)")
}

/// Run `intentd pair` with `args`, retrying until it succeeds: the WSS
/// listener binds asynchronously after the UDS socket accepts, so early runs
/// can fail with listener-down (bounded by the startup budget).
async fn run_pair_until_success(data_dir: &Path, args: &[&str]) -> std::process::Output {
    let deadline = std::time::Instant::now() + common::daemon_startup_timeout();
    loop {
        let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
            .arg("pair")
            .args(args)
            .env("INTENTD_DATA_DIR", data_dir)
            .stdin(Stdio::null())
            .output()
            .expect("run intentd pair");
        if output.status.success() || std::time::Instant::now() >= deadline {
            break output;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Extract the value of a labeled credential line (`Token:`, `Fingerprint:`)
/// from `intentd pair` stdout.
fn labeled_value<'a>(stdout: &'a str, label: &str) -> Option<&'a str> {
    stdout
        .lines()
        .find(|l| l.starts_with(label))
        .and_then(|l| l.split_whitespace().nth(1))
}

#[tokio::test]
async fn pair_prints_qr_and_payload_uri_and_writes_png_svg() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");
    let token = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

    let child = spawn_daemon_both(&data_dir, Some(token));
    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    await_socket(&mut daemon, &socket).await;

    let png_path = data_dir.join("pair.png");
    let svg_path = data_dir.join("pair.svg");
    let output = run_pair_until_success(
        &data_dir,
        &[
            "--png",
            png_path.to_str().unwrap(),
            "--svg",
            svg_path.to_str().unwrap(),
        ],
    )
    .await;

    assert!(
        output.status.success(),
        "pair command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("URL:         intent://pair?v=1&host="),
        "pair output should include the labeled payload URL: {stdout}"
    );
    assert!(
        stdout.contains(&format!("token={token}")),
        "payload URI should embed the token"
    );
    assert!(
        stdout.contains(&format!("Token:       {token}")),
        "pair output should include the labeled token line: {stdout}"
    );
    let fingerprint =
        labeled_value(&stdout, "Fingerprint:").expect("labeled fingerprint line present");
    assert!(
        fingerprint.contains(':') && fingerprint.len() == 95,
        "fingerprint should be a colon-separated sha256 hex: {fingerprint}"
    );
    assert!(
        stdout.contains("Scan with the Intent iOS app"),
        "pair output should explain the QR code: {stdout}"
    );
    // Each credential line carries a one-line usage explanation.
    assert!(
        stdout.contains("Same payload as the QR code"),
        "URL line should have a usage note: {stdout}"
    );
    assert!(
        stdout.contains("Bearer token"),
        "token line should have a usage note: {stdout}"
    );
    assert!(
        stdout.contains("TLS certificate fingerprint"),
        "fingerprint line should have a usage note: {stdout}"
    );
    // The ANSI QR rendering uses unicode block characters.
    assert!(
        stdout.contains('\u{2588}'),
        "pair output should render a QR code in unicode blocks"
    );

    let png = std::fs::read(&png_path).expect("PNG file written");
    assert!(png.starts_with(b"\x89PNG"), "valid PNG magic bytes");
    let svg = std::fs::read_to_string(&svg_path).expect("SVG file written");
    assert!(svg.contains("<svg"), "valid SVG document");

    // Exported images embed the bearer token — must be owner-only (0600).
    use std::os::unix::fs::PermissionsExt;
    for path in [&png_path, &svg_path] {
        let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "{} should be 0600", path.display());
    }
}

#[tokio::test]
async fn pair_without_listener_non_tty_requires_yes_flag() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");

    // UDS-only daemon: the WSS listener is down. Without --yes and with a
    // non-TTY stdin, pair must refuse (it cannot prompt) and point at --yes.
    let child = spawn_daemon(&data_dir);
    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    await_socket(&mut daemon, &socket).await;

    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("pair")
        .env("INTENTD_DATA_DIR", &data_dir)
        .stdin(Stdio::null())
        .output()
        .expect("run intentd pair");

    assert!(
        !output.status.success(),
        "pair should fail without a TCP listener when it cannot prompt"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("re-run with --yes"),
        "should point at --yes for unattended enabling: {stderr}"
    );
    // The setting must NOT have been flipped behind the user's back. The
    // config template ships a commented-out `# enabled = true` example, so
    // only uncommented lines count.
    let config = std::fs::read_to_string(data_dir.join("config.toml")).unwrap_or_default();
    assert!(
        !has_uncommented_enabled_true(&config),
        "server.wsApi.enabled must stay off without consent: {config}"
    );
}

/// True when config.toml has an active (uncommented) `enabled = true` line
/// inside the `[server.wsApi]` table — the persisted form of
/// `server.wsApi.enabled = true`. Commented template examples
/// (`# enabled = true`) and other tables' `enabled` keys do not count.
fn has_uncommented_enabled_true(config: &str) -> bool {
    let mut in_ws_api = false;
    for line in config.lines() {
        let t = line.trim_start();
        if t.starts_with('[') {
            in_ws_api = t.starts_with("[server.wsApi]");
            continue;
        }
        if in_ws_api
            && !t.starts_with('#')
            && t.starts_with("enabled")
            && t.replace(' ', "").starts_with("enabled=true")
        {
            return true;
        }
    }
    false
}

#[tokio::test]
async fn pair_with_yes_enables_wss_and_prints_payload() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");

    // UDS-only daemon: the WSS listener is down. `pair --yes` must enable it
    // via settings.update (persisting server.wsApi.enabled = true), then
    // succeed end-to-end. INTENTD_TCP_PORT=0 makes the started listener bind
    // an OS-assigned ephemeral port, so parallel suites cannot collide.
    let child = spawn_daemon(&data_dir);
    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    await_socket(&mut daemon, &socket).await;

    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("pair")
        .arg("--yes")
        .env("INTENTD_DATA_DIR", &data_dir)
        .stdin(Stdio::null())
        .output()
        .expect("run intentd pair --yes");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "pair --yes should enable the listener and succeed: {stderr}"
    );
    assert!(
        stderr.contains("External connections enabled"),
        "should report external connections were enabled: {stderr}"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("intent://pair?v=1&host="),
        "pair output should include the payload URI: {stdout}"
    );

    // The enable must be persisted to config.toml, not just in-memory.
    let config = std::fs::read_to_string(data_dir.join("config.toml"))
        .expect("config.toml written by settings.update");
    assert!(
        has_uncommented_enabled_true(&config),
        "server.wsApi.enabled = true should be persisted: {config}"
    );

    // A second run finds the listener already up — no prompt, no re-enable.
    let output2 = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("pair")
        .env("INTENTD_DATA_DIR", &data_dir)
        .stdin(Stdio::null())
        .output()
        .expect("run intentd pair (second)");
    let stderr2 = String::from_utf8_lossy(&output2.stderr);
    assert!(
        output2.status.success(),
        "pair should succeed with the listener already up: {stderr2}"
    );
    assert!(
        !stderr2.contains("External connections enabled"),
        "second run must not re-enable: {stderr2}"
    );
}

#[tokio::test]
async fn pair_fails_when_daemon_down() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");

    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("pair")
        .env("INTENTD_DATA_DIR", &data_dir)
        .output()
        .expect("run intentd pair");

    assert!(
        !output.status.success(),
        "pair should fail when the daemon is down"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot connect to daemon"),
        "should report the daemon is unreachable: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn pair_rotate_mints_new_token() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");

    // File-backed token store (no INTENTD_AUTH_TOKEN): rotation goes through
    // the daemon's `server.rotateToken`, so the subsequent `pairing.getInfo`
    // must serve the NEW token (not a stale in-process cache entry).
    let child = spawn_daemon_both(&data_dir, None);
    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    await_socket(&mut daemon, &socket).await;

    let output1 = run_pair_until_success(&data_dir, &[]).await;
    assert!(
        output1.status.success(),
        "first pair run failed: {}",
        String::from_utf8_lossy(&output1.stderr)
    );
    let stdout1 = String::from_utf8_lossy(&output1.stdout).to_string();
    let token1 = labeled_value(&stdout1, "Token:").expect("extract token1");

    let output2 = run_pair_until_success(&data_dir, &["--rotate"]).await;
    assert!(
        output2.status.success(),
        "pair --rotate failed: {}",
        String::from_utf8_lossy(&output2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&output2.stdout).to_string();
    let token2 = labeled_value(&stdout2, "Token:").expect("extract token2");

    assert_ne!(token1, token2, "rotated token should be different");
    assert!(
        stdout2.contains(&format!("token={token2}")),
        "payload URI should embed the rotated token: {stdout2}"
    );
}

#[tokio::test]
async fn pair_rotate_refuses_when_env_var_set() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");
    let token = "abababababababababababababababababababababababababababababababab";

    // Daemon and CLI share a fixed INTENTD_AUTH_TOKEN: rotation is a no-op
    // with a stderr note, and the env token is printed unchanged.
    let child = spawn_daemon_both(&data_dir, Some(token));
    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    await_socket(&mut daemon, &socket).await;

    let deadline = std::time::Instant::now() + common::daemon_startup_timeout();
    let output = loop {
        let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
            .arg("pair")
            .arg("--rotate")
            .env("INTENTD_DATA_DIR", &data_dir)
            .env("INTENTD_AUTH_TOKEN", token)
            .stdin(Stdio::null())
            .output()
            .expect("run intentd pair --rotate with env var");
        if output.status.success() || std::time::Instant::now() >= deadline {
            break output;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert!(
        output.status.success(),
        "pair --rotate should succeed but not rotate: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("fixed by the env var and cannot be rotated"),
        "should warn about env var: {stderr}"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(&format!("Token:       {token}")),
        "should still print the fixed token: {stdout}"
    );
}

/// Regression test for intent-hq/monorepo#1827: piping one-shot CLI output
/// into a consumer that closes the pipe early (`intentd status | head`) must
/// not panic with a broken-pipe backtrace. The read end of the pipe is closed
/// before the child spawns, so its very first stdout write hits EPIPE
/// deterministically (no daemon is running: `status` prints the
/// "intentd: down" lines). Expected: a quiet exit with the SIGPIPE-style
/// status 141 (128 + SIGPIPE) — never a panic backtrace.
#[tokio::test]
async fn status_exits_quietly_when_stdout_pipe_closes_early() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");

    let (pipe_read, pipe_write) = nix::unistd::pipe().expect("pipe(2)");
    drop(pipe_read);
    let stdout = Stdio::from(pipe_write);

    let child = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("status")
        .env("INTENTD_DATA_DIR", &data_dir)
        .stdout(stdout)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn intentd status");
    let output = child.wait_with_output().expect("wait intentd status");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.to_lowercase().contains("panic"),
        "broken-pipe stdout must not panic; stderr: {stderr}"
    );
    assert!(
        output.status.code() == Some(141)
            || output.status.signal() == Some(nix::sys::signal::Signal::SIGPIPE as i32),
        "expected quiet SIGPIPE-style exit (141) or SIGPIPE death, got {:?}; stderr: {stderr}",
        output.status
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}
