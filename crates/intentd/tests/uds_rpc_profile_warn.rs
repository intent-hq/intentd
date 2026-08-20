//! Per-RPC profiling WARN integration test (expensive-RPC guardrail).
//!
//! Launches the REAL `intentd serve` daemon over UDS with the profiling
//! thresholds lowered via env (`INTENTD_RPC_STATEMENT_WARN_THRESHOLD=0`,
//! `INTENTD_RPC_DURATION_WARN_MS=0`), drives a normal `workspace.list`
//! dispatch, and asserts the daemon logs exactly one statement-budget WARN
//! and one duration-budget WARN naming the method. A second daemon with the
//! default thresholds proves normal traffic logs neither. Logging only — no
//! wire-contract assertions beyond the standard response envelope.

#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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

/// Spawn the real daemon with stderr (the tracing stderr layer) captured to
/// `data_dir/daemon.log`, plus the given extra env vars.
fn spawn_daemon(prefix: &str, envs: &[(&str, &str)]) -> (Daemon, PathBuf, PathBuf) {
    // Keep the data dir short so `data_dir/intentd.sock` fits within SUN_LEN.
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let socket = data_dir.join("intentd.sock");
    let log_path = data_dir.join("daemon.log");
    let log = std::fs::File::create(&log_path).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let child = cmd.spawn().expect("spawn intentd serve");
    (
        Daemon {
            child,
            data_dir: data_dir.clone(),
        },
        socket,
        log_path,
    )
}

async fn await_socket(socket: &PathBuf) -> bool {
    timeout(common::daemon_startup_timeout(), async {
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

async fn rpc(socket: &PathBuf, method: &str) -> Value {
    rpc_with_params(socket, method, json!({})).await
}

async fn rpc_with_params(socket: &PathBuf, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let mut line = serde_json::to_string(&frame).unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
        .await
        .expect("rpc timed out")
        .expect("read rpc response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

/// Strip ANSI escape sequences (the stderr fmt layer colors its output even
/// when redirected to a file) so needles like `method=workspace.list` match.
fn strip_ansi(log: &str) -> String {
    let mut out = String::with_capacity(log.len());
    let mut chars = log.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Count log lines containing all of `needles` (ANSI codes stripped).
fn count_lines(log: &str, needles: &[&str]) -> usize {
    strip_ansi(log)
        .lines()
        .filter(|line| needles.iter().all(|n| line.contains(n)))
        .count()
}

#[tokio::test]
async fn lowered_thresholds_fire_statement_and_duration_warns() {
    let (_daemon, socket, log_path) = spawn_daemon(
        "itdp-warn",
        &[
            ("INTENTD_RPC_STATEMENT_WARN_THRESHOLD", "0"),
            ("INTENTD_RPC_DURATION_WARN_MS", "0"),
        ],
    );
    assert!(await_socket(&socket).await, "daemon did not start");

    let resp = rpc(&socket, "workspace.list").await;
    assert!(resp["result"]["workspaces"].is_array(), "resp: {resp}");

    // The WARNs are emitted on span close, before the response frame is
    // written, but poll briefly to absorb stderr write scheduling.
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    let log = loop {
        let log = std::fs::read_to_string(&log_path).expect("read daemon log");
        let stmt = count_lines(
            &log,
            &["exceeded SQL statement budget", "method=workspace.list"],
        );
        let dur = count_lines(&log, &["exceeded duration budget", "method=workspace.list"]);
        if (stmt >= 1 && dur >= 1) || tokio::time::Instant::now() >= deadline {
            break log;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(
        count_lines(
            &log,
            &["exceeded SQL statement budget", "method=workspace.list"]
        ),
        1,
        "expected exactly one statement-budget WARN, log:\n{log}"
    );
    assert_eq!(
        count_lines(&log, &["exceeded duration budget", "method=workspace.list"]),
        1,
        "expected exactly one duration-budget WARN, log:\n{log}"
    );
}

#[tokio::test]
async fn default_thresholds_stay_quiet_for_normal_traffic() {
    let (_daemon, socket, log_path) = spawn_daemon("itdp-quiet", &[]);
    assert!(await_socket(&socket).await, "daemon did not start");

    let resp = rpc(&socket, "workspace.list").await;
    assert!(resp["result"]["workspaces"].is_array(), "resp: {resp}");

    // The warn (were it wrongly emitted) lands on stderr before the response
    // frame is written, so a single read after the response is sufficient.
    let log = std::fs::read_to_string(&log_path).expect("read daemon log");
    assert_eq!(
        count_lines(&log, &["intentd::rpc_profile"]),
        0,
        "expected no profiling WARNs, log:\n{log}"
    );
}

struct TempRepo(PathBuf);

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Create a temporary git repo (initial commit on `main`) carrying the given
/// `.intent/config.json` contents.
fn create_repo_with_config(config: &str) -> TempRepo {
    let repo_path = std::env::temp_dir().join(format!("itdp-repo-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&repo_path).expect("mkdir repo");
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .current_dir(&repo_path)
            .output()
            .expect("spawn git");
        assert!(out.status.success(), "git {args:?} failed: {out:?}");
    };
    git(&["init", "--initial-branch=main"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "user.name", "Test User"]);
    let intent_dir = repo_path.join(".intent");
    std::fs::create_dir_all(&intent_dir).expect("mkdir .intent");
    std::fs::write(intent_dir.join("config.json"), config).expect("write config");
    std::fs::write(repo_path.join("README.md"), "test").expect("write README");
    git(&["add", "."]);
    git(&["commit", "-m", "Initial commit"]);
    TempRepo(repo_path)
}

/// Regression test for intent-hq/monorepo#1778: the first `script.list` for a
/// fresh workspace bootstraps repo-config scripts, and used to persist each
/// one individually (1 × get_workspace + N × upsert_script), so any repo with
/// ≥ 25 configured scripts tripped the default statement budget on that first
/// call. The bootstrap now persists in chunked batched upserts (2048 rows per
/// statement), keeping the dispatch at ~2 statements for any plausible config
/// size.
#[tokio::test]
async fn script_list_bootstrap_stays_within_statement_budget() {
    // Default thresholds: with 30 configured scripts the pre-fix bootstrap
    // executed 31 statements — over the default budget of 25.
    let (_daemon, socket, log_path) = spawn_daemon("itdp-boot", &[]);
    assert!(await_socket(&socket).await, "daemon did not start");

    let scripts: Vec<Value> = (0..30)
        .map(|i| json!({ "name": format!("script-{i}"), "command": format!("echo {i}"), "mode": "command" }))
        .collect();
    let repo = create_repo_with_config(&json!({ "scripts": scripts }).to_string());

    let resp = rpc_with_params(
        &socket,
        "workspace.create",
        json!({ "repositoryPath": repo.0.to_str().unwrap() }),
    )
    .await;
    let workspace_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Regression test for intent-hq/monorepo#2672: `workspace.create` is a
    // legitimately compound op (39-40 statements) and sits on the compound-op
    // statement tier (budget 50), so at default thresholds it must not trip
    // the statement budget.
    let log = std::fs::read_to_string(&log_path).expect("read daemon log");
    assert_eq!(
        count_lines(
            &log,
            &["exceeded SQL statement budget", "method=workspace.create"]
        ),
        0,
        "workspace.create exceeded the compound statement budget, log:\n{log}"
    );

    // First script.list triggers the repo-config bootstrap.
    let resp = rpc_with_params(
        &socket,
        "script.list",
        json!({ "workspaceId": workspace_id }),
    )
    .await;
    let listed = resp["result"]["scripts"].as_array().expect("scripts array");
    assert_eq!(listed.len(), 30, "all repo-config scripts seeded");

    // The statement-budget WARN (were it wrongly emitted) lands on stderr
    // before the response frame is written, so a single read is sufficient.
    let log = std::fs::read_to_string(&log_path).expect("read daemon log");
    assert_eq!(
        count_lines(
            &log,
            &["exceeded SQL statement budget", "method=script.list"]
        ),
        0,
        "script.list bootstrap exceeded the statement budget, log:\n{log}"
    );
}

/// Regression test for intent-hq/monorepo#2994: `workspace.transfer.plan`
/// used to compute its per-table row stats with 2 statements per
/// TRANSFER_TABLES entry (PRAGMA table_info + per-table aggregate, ~56
/// statements for 28 tables), tripping the default statement budget on every
/// call. The stats read is now batched to exactly two statements (one
/// pragma_table_info join, one UNION ALL aggregate), keeping the whole
/// dispatch well within the default budget of 25.
#[tokio::test]
async fn transfer_plan_stays_within_statement_budget() {
    let (_daemon, socket, log_path) = spawn_daemon("itdp-plan", &[]);
    assert!(await_socket(&socket).await, "daemon did not start");

    let repo = create_repo_with_config("{}");
    let resp = rpc_with_params(
        &socket,
        "workspace.create",
        json!({ "repositoryPath": repo.0.to_str().unwrap() }),
    )
    .await;
    let workspace_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let resp = rpc_with_params(
        &socket,
        "workspace.transfer.plan",
        json!({ "workspaceId": workspace_id }),
    )
    .await;
    assert!(
        resp["result"]["plan"]["manifest"]["tables"].is_array(),
        "resp: {resp}"
    );

    // The statement-budget WARN (were it wrongly emitted) lands on stderr
    // before the response frame is written, so a single read is sufficient.
    let log = std::fs::read_to_string(&log_path).expect("read daemon log");
    assert_eq!(
        count_lines(
            &log,
            &[
                "exceeded SQL statement budget",
                "method=workspace.transfer.plan"
            ]
        ),
        0,
        "workspace.transfer.plan exceeded the statement budget, log:\n{log}"
    );
}
