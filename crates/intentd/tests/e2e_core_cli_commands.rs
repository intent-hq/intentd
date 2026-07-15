//! E2E tests for CLI subcommands (status, stop, doctor, token) to exercise intentd
//! binary coverage through daemon control paths.

#![cfg(unix)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::time::timeout;
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
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
}

async fn await_socket(socket: &PathBuf) -> bool {
    timeout(Duration::from_secs(10), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn doctor_checks_data_dir_and_migrations() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");

    // Ensure INTENTD_TCP_PORT is not inherited from parent environment
    std::env::remove_var("INTENTD_TCP_PORT");

    let child = spawn_daemon(&data_dir);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    assert!(await_socket(&socket).await, "daemon did not start");

    // Run `intentd doctor` command
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("doctor")
        .env("INTENTD_DATA_DIR", &data_dir)
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

#[tokio::test]
async fn token_generates_and_prints_token_and_fingerprint() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let secrets_file = data_dir.join("secrets.json");

    // Ensure INTENTD_AUTH_TOKEN is not inherited from parent environment
    std::env::remove_var("INTENTD_AUTH_TOKEN");

    // Run `intentd token` (no rotation)
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("token")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .output()
        .expect("run intentd token");

    assert!(
        output.status.success(),
        "token command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("token:"),
        "token output should include token: {stdout}"
    );
    assert!(
        stdout.contains("fingerprint:"),
        "token output should include fingerprint: {stdout}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn token_rotate_flag_generates_new_token() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let secrets_file = data_dir.join("secrets.json");

    // Ensure INTENTD_AUTH_TOKEN is not inherited from parent environment
    std::env::remove_var("INTENTD_AUTH_TOKEN");

    // First token
    let output1 = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("token")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .output()
        .expect("run intentd token");

    assert!(output1.status.success(), "first token command failed");
    let stdout1 = String::from_utf8_lossy(&output1.stdout);
    let token1 = stdout1
        .lines()
        .find(|l| l.starts_with("token:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .expect("extract token1");

    // Rotate
    let output2 = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("token")
        .arg("--rotate")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .output()
        .expect("run intentd token --rotate");

    assert!(output2.status.success(), "rotate token command failed");
    let stdout2 = String::from_utf8_lossy(&output2.stdout);
    let token2 = stdout2
        .lines()
        .find(|l| l.starts_with("token:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .expect("extract token2");

    assert_ne!(token1, token2, "rotated token should be different");

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn token_rotate_refuses_when_env_var_set() {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("itdc-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");

    // Run with INTENTD_AUTH_TOKEN set
    let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("token")
        .arg("--rotate")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_AUTH_TOKEN", "fixed-token-from-env")
        .output()
        .expect("run intentd token --rotate with env var");

    assert!(
        output.status.success(),
        "token command should succeed but not rotate"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("fixed by the env var and cannot be rotated"),
        "should warn about env var: {stderr}"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("token:"),
        "should still print token: {stdout}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}
