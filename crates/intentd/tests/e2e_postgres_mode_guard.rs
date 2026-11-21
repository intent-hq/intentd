//! Postgres runs only in orchestrator mode: `intentd serve` with
//! `INTENTD_POSTGRES_URL` set refuses any other daemon mode before it opens a
//! store or writes a file (batch G v2 §6, step 0a).
//!
//! Every refusal snapshots the data dir after the fixture is built and
//! asserts the daemon left it byte-for-byte unchanged: no `config.toml`, no
//! `intentd.lock`, no pidfile, no database and no log file. Orchestrator mode
//! passes the guard and runs startup up to the store open, which no shipped
//! build has yet; it must never fall back to the `SQLite` database.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};

use intentd_test_support::GuardedChild;

use crate::common;

const URL: &str = "postgres://intent:secret-pw@127.0.0.1:1/intent";
/// What the refusal must name: the URL setting and the mode setting.
const URL_SETTING: &str = "INTENTD_POSTGRES_URL";
const MODE_SETTING: &str = "daemon.mode";
/// The orchestrator path passes the guard and stops at the open, which no
/// shipped build has yet.
const NO_BACKEND: &str = "no Postgres store backend";

struct Run {
    status: ExitStatus,
    stderr: String,
    before: BTreeMap<PathBuf, Vec<u8>>,
    after: BTreeMap<PathBuf, Vec<u8>>,
}

/// Every file under `dir` with its contents, so any create, write or delete
/// shows up as a difference.
fn snapshot(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).expect("read snapshot dir") {
            let path = entry.expect("dir entry").path();
            let rel = path.strip_prefix(dir).expect("under dir").to_path_buf();
            if path.is_dir() {
                out.insert(rel, b"<dir>".to_vec());
                stack.push(path);
            } else {
                out.insert(rel, std::fs::read(&path).expect("read snapshot file"));
            }
        }
    }
    out
}

/// Run `intentd serve <args>` with the Postgres URL set and an optional
/// persisted `config.toml`; record the data dir before and after.
fn serve_with_postgres_url(args: &[&str], config_toml: Option<&str>) -> Run {
    let root = common::test_tempdir("intentd-pg-guard-");
    let data_dir = root.path().join("data");
    std::fs::create_dir(&data_dir).expect("mkdir data dir");
    if let Some(text) = config_toml {
        std::fs::write(data_dir.join("config.toml"), text).expect("write config.toml");
    }
    let stderr_path = root.path().join("serve.stderr");
    let stderr = std::fs::File::create(&stderr_path).expect("create stderr file");
    let mut cmd = common::hermetic_serve_command(&data_dir);
    cmd.args(args)
        .env(URL_SETTING, URL)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr));
    common::hermetic_fixture_identity(&mut cmd, &data_dir);
    // The builder creates the private `gh-config` dir; snapshot after it.
    let before = snapshot(&data_dir);
    let mut child = GuardedChild::spawn(&mut cmd).expect("spawn intentd serve");
    // Both outcomes exit during startup; a daemon still up after a minute is
    // serving, which is the bug.
    let status = child
        .wait_with_timeout(common::test_timeout(std::time::Duration::from_secs(60)))
        .expect("wait for intentd serve")
        .unwrap_or_else(|| {
            let _ = child.kill_tree();
            panic!(
                "intentd serve kept running with {URL_SETTING} set; stderr:\n{}",
                std::fs::read_to_string(&stderr_path).unwrap_or_default()
            )
        });
    let stderr = std::fs::read_to_string(&stderr_path).expect("read stderr");
    let after = snapshot(&data_dir);
    for (path, bytes) in &after {
        assert!(
            !String::from_utf8_lossy(bytes).contains("secret-pw"),
            "the URL's password must never be written ({}): {stderr}",
            path.display()
        );
    }
    assert!(
        !stderr.contains("secret-pw"),
        "the URL's password must never be printed: {stderr}"
    );
    Run {
        status,
        stderr,
        before,
        after,
    }
}

#[track_caller]
fn assert_mode_refusal(run: &Run, mode: &str) {
    assert!(!run.status.success(), "must exit non-zero: {}", run.stderr);
    assert_eq!(
        run.after, run.before,
        "a refusal must not touch the data dir; stderr:\n{}",
        run.stderr
    );
    for needle in [URL_SETTING, MODE_SETTING, mode, "orchestrator"] {
        assert!(
            run.stderr.contains(needle),
            "refusal must name `{needle}`: {}",
            run.stderr
        );
    }
}

#[track_caller]
fn assert_passed_mode_guard(run: &Run) {
    assert!(!run.status.success(), "must exit non-zero: {}", run.stderr);
    assert!(
        run.stderr.contains(NO_BACKEND),
        "orchestrator mode must reach the store open: {}",
        run.stderr
    );
    assert!(
        !run.stderr.contains("only in orchestrator mode"),
        "orchestrator mode must not hit the mode refusal: {}",
        run.stderr
    );
    let sqlite: Vec<_> = run
        .after
        .keys()
        .filter(|path| path.to_string_lossy().starts_with("intentd.db"))
        .collect();
    assert!(
        sqlite.is_empty(),
        "a Postgres choice must never open the SQLite store: {sqlite:?}"
    );
    assert!(
        !run.after.contains_key(Path::new("intentd.pid")),
        "startup must release its pidfile on the failed open"
    );
}

#[test]
fn regular_mode_with_postgres_url_refuses_and_touches_nothing() {
    assert_mode_refusal(&serve_with_postgres_url(&[], None), "regular");
}

#[test]
fn node_flag_with_postgres_url_refuses_and_touches_nothing() {
    assert_mode_refusal(&serve_with_postgres_url(&["--node"], None), "node");
}

#[test]
fn persisted_node_mode_with_postgres_url_refuses_and_touches_nothing() {
    let run = serve_with_postgres_url(&[], Some("[daemon]\nmode = \"node\"\n"));
    assert_mode_refusal(&run, "node");
}

#[test]
fn orchestrator_flag_with_postgres_url_passes_the_mode_guard() {
    assert_passed_mode_guard(&serve_with_postgres_url(&["--orchestrator"], None));
}

#[test]
fn persisted_orchestrator_mode_with_postgres_url_passes_the_mode_guard() {
    let run = serve_with_postgres_url(&[], Some("[daemon]\nmode = \"orchestrator\"\n"));
    assert_passed_mode_guard(&run);
}

/// The flag wins over the persisted setting, as for every other start.
#[test]
fn node_flag_over_persisted_orchestrator_mode_refuses() {
    let run = serve_with_postgres_url(&["--node"], Some("[daemon]\nmode = \"orchestrator\"\n"));
    assert_mode_refusal(&run, "node");
}
