//! Postgres runs only in orchestrator mode: `intentd serve` with
//! `INTENTD_POSTGRES_URL` set refuses any other daemon mode before it opens a
//! store or writes a file (batch G v2 §6, step 0a).
//!
//! Every refusal snapshots the fixture root (the data dir and any
//! `INTENTD_CONFIG` file) after the fixture is built and asserts no file was
//! created, deleted or changed in content: no `config.toml`, no
//! `intentd.lock`, no pidfile, no database and no log file. Orchestrator mode
//! passes the guard and runs startup up to the store open, which no shipped
//! build has yet; it must never fall back to the `SQLite` database.
#![cfg(unix)]

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
/// shows up as a difference (contents only: a `touch` or a permission change
/// on an existing file would not). `skip` names a top-level entry to leave
/// out.
fn snapshot(dir: &Path, skip: &str) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).expect("read snapshot dir") {
            let path = entry.expect("dir entry").path();
            let rel = path.strip_prefix(dir).expect("under dir").to_path_buf();
            if rel == Path::new(skip) {
                continue;
            }
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

/// One `intentd serve` start with the Postgres URL set.
#[derive(Default)]
struct Case<'a> {
    args: &'a [&'a str],
    /// `config.toml` in the data dir.
    config_toml: Option<&'a str>,
    /// `config.toml` outside the data dir, through `INTENTD_CONFIG`.
    external_config_toml: Option<&'a str>,
    /// The URL value; `URL` when `None`.
    url: Option<&'a str>,
}

/// Run the case and record everything under the fixture root (data dir and
/// any external config) before and after; only the stderr capture is left
/// out.
fn serve_with_postgres_url(case: &Case<'_>) -> Run {
    const STDERR: &str = "serve.stderr";
    let root = common::test_tempdir("intentd-pg-guard-");
    let data_dir = root.path().join("data");
    std::fs::create_dir(&data_dir).expect("mkdir data dir");
    if let Some(text) = case.config_toml {
        std::fs::write(data_dir.join("config.toml"), text).expect("write config.toml");
    }
    let stderr_path = root.path().join(STDERR);
    let stderr = std::fs::File::create(&stderr_path).expect("create stderr file");
    let mut cmd = common::hermetic_serve_command(&data_dir);
    cmd.args(case.args)
        .env(URL_SETTING, case.url.unwrap_or(URL))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr));
    if let Some(text) = case.external_config_toml {
        let elsewhere = root.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).expect("mkdir external config dir");
        let path = elsewhere.join("config.toml");
        std::fs::write(&path, text).expect("write external config.toml");
        cmd.env("INTENTD_CONFIG", &path);
    }
    // The env writes above end the builder's identity isolation for
    // serve_spawn_lint; restate it (it only re-checks the `gh-config` dir the
    // builder made).
    common::hermetic_fixture_identity(&mut cmd, &data_dir);
    let before = snapshot(root.path(), STDERR);
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
    let after = snapshot(root.path(), STDERR);
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
fn assert_refused_untouched(run: &Run) {
    assert!(!run.status.success(), "must exit non-zero: {}", run.stderr);
    assert_eq!(
        run.after, run.before,
        "a refusal must not touch the data dir or the config; stderr:\n{}",
        run.stderr
    );
}

#[track_caller]
fn assert_mode_refusal(run: &Run, mode: &str) {
    assert_refused_untouched(run);
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
        .filter(|path| path.to_string_lossy().starts_with("data/intentd.db"))
        .collect();
    assert!(
        sqlite.is_empty(),
        "a Postgres choice must never open the SQLite store: {sqlite:?}"
    );
    assert!(
        !run.after.contains_key(Path::new("data/intentd.pid")),
        "startup must release its pidfile on the failed open"
    );
}

const NODE: Option<&str> = Some("[daemon]\nmode = \"node\"\n");
const ORCHESTRATOR: Option<&str> = Some("[daemon]\nmode = \"orchestrator\"\n");

#[test]
fn regular_mode_with_postgres_url_refuses_and_touches_nothing() {
    let run = serve_with_postgres_url(&Case::default());
    assert_mode_refusal(&run, "regular");
    assert!(
        run.stderr.contains("there is no config.toml"),
        "{}",
        run.stderr
    );
}

#[test]
fn node_flag_with_postgres_url_refuses_and_touches_nothing() {
    let run = serve_with_postgres_url(&Case {
        args: &["--node"],
        ..Case::default()
    });
    assert_mode_refusal(&run, "node");
}

#[test]
fn persisted_node_mode_with_postgres_url_refuses_and_touches_nothing() {
    let run = serve_with_postgres_url(&Case {
        config_toml: NODE,
        ..Case::default()
    });
    assert_mode_refusal(&run, "node");
}

#[test]
fn orchestrator_flag_with_postgres_url_passes_the_mode_guard() {
    assert_passed_mode_guard(&serve_with_postgres_url(&Case {
        args: &["--orchestrator"],
        ..Case::default()
    }));
}

#[test]
fn persisted_orchestrator_mode_with_postgres_url_passes_the_mode_guard() {
    assert_passed_mode_guard(&serve_with_postgres_url(&Case {
        config_toml: ORCHESTRATOR,
        ..Case::default()
    }));
}

/// The flag wins over the persisted setting, as for every other start.
#[test]
fn node_flag_over_persisted_orchestrator_mode_refuses() {
    let run = serve_with_postgres_url(&Case {
        args: &["--node"],
        config_toml: ORCHESTRATOR,
        ..Case::default()
    });
    assert_mode_refusal(&run, "node");
}

/// The guard reads `config.toml` where `Config::resolve` does: through
/// `INTENTD_CONFIG` when it is set, not the data dir.
#[test]
fn external_config_orchestrator_mode_passes_the_mode_guard() {
    assert_passed_mode_guard(&serve_with_postgres_url(&Case {
        external_config_toml: ORCHESTRATOR,
        ..Case::default()
    }));
}

#[test]
fn external_config_node_mode_refuses_over_a_data_dir_orchestrator_config() {
    let run = serve_with_postgres_url(&Case {
        config_toml: ORCHESTRATOR,
        external_config_toml: NODE,
        ..Case::default()
    });
    assert_mode_refusal(&run, "node");
}

#[test]
fn malformed_config_with_postgres_url_refuses_and_touches_nothing() {
    let run = serve_with_postgres_url(&Case {
        config_toml: Some("[daemon]\nbogusKey = 1\n"),
        ..Case::default()
    });
    assert_refused_untouched(&run);
    assert!(run.stderr.contains("config.toml"), "{}", run.stderr);
}

/// A set but blank URL is a mistake, not "use SQLite": even an orchestrator
/// refuses rather than quietly opening `intentd.db`.
#[test]
fn empty_postgres_url_refuses_and_touches_nothing() {
    for url in ["", "   "] {
        let run = serve_with_postgres_url(&Case {
            args: &["--orchestrator"],
            url: Some(url),
            ..Case::default()
        });
        assert_refused_untouched(&run);
        assert!(run.stderr.contains("is set but empty"), "{}", run.stderr);
    }
}
