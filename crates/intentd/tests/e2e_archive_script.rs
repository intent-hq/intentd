//! E2E: `.intent/config.json` `archiveScript` runs when a workspace leaves
//! Active (intent-hq/intent#8850) — on archive, on delete of an Active
//! workspace, and on purge (retire) of an Active workspace — in the
//! workspace folder, before the folder is removed, and only once. A failing
//! script never blocks the archive and is reported on the workspace card
//! (`statusMessage`).

#![cfg(unix)]

use crate::common;

use common::uds_exchange::uds_rpc;
use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::time::timeout;

struct Daemon {
    child: GuardedChild,
    data_dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill_tree();
        if let Ok(log) = std::fs::read_to_string(self.log_path()) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

impl Daemon {
    async fn start() -> Self {
        let data_dir = common::test_tempdir_in("/tmp", "itd-archscript-");
        let path = data_dir.path();
        let log = std::fs::File::create(path.join("daemon.log")).expect("create daemon log");
        let workspaces_dir = path.join("workspaces");
        std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
        let mut cmd = common::hermetic_serve_command(path);
        cmd.env("INTENTD_DATA_DIR", path)
            .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
            .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::from(log));
        common::hermetic_fixture_identity(&mut cmd, path);
        let child = GuardedChild::spawn(&mut cmd).expect("spawn intentd serve");
        let daemon = Self { child, data_dir };
        let socket = daemon.socket();
        let ready = timeout(common::daemon_startup_timeout(), async {
            while !common::daemon_is_ready(&socket).await {
                // timing-guard: poll interval between reads of an observable state, not a wait for time to pass
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(ready.is_ok(), "daemon did not start");
        daemon
    }

    fn socket(&self) -> PathBuf {
        self.data_dir.path().join("intentd.sock")
    }

    fn log_path(&self) -> PathBuf {
        self.data_dir.path().join("daemon.log")
    }

    async fn rpc(&self, id: i64, method: &str, params: Value) -> Value {
        let resp = uds_rpc(&self.socket(), id, method, params).await;
        assert!(resp.get("error").is_none(), "{method} failed: {resp}");
        resp["result"].clone()
    }

    /// Create a workspace from `repo`; returns `(workspaceId, worktreePath)`.
    async fn create_workspace(&self, repo: &Path) -> (String, PathBuf) {
        let result = self
            .rpc(
                1,
                "workspace.create",
                json!({ "title": "archive-script", "repositoryPath": repo.to_string_lossy() }),
            )
            .await;
        let ws = &result["workspace"];
        let id = ws["id"].as_str().expect("workspace id").to_string();
        let worktree = PathBuf::from(ws["worktreePath"].as_str().expect("worktreePath"));
        (id, worktree)
    }
}

/// A git repo whose committed `.intent/config.json` carries `archive_script`.
fn repo_with_archive_script(archive_script: &str) -> tempfile::TempDir {
    let repo = common::test_tempdir("archscript-repo-");
    let path = repo.path();
    let git = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .stdout(Stdio::null())
            .status()
            .expect("spawn git");
        assert!(status.success(), "git {args:?} failed");
    };
    git(&["init", "--initial-branch=main"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "user.name", "Test"]);
    std::fs::write(path.join("README.md"), "# Test\n").expect("write readme");
    std::fs::create_dir_all(path.join(".intent")).expect("mkdir .intent");
    std::fs::write(
        path.join(".intent/config.json"),
        json!({ "archiveScript": archive_script }).to_string(),
    )
    .expect("write config");
    git(&["add", "."]);
    git(&["commit", "-m", "initial commit"]);
    repo
}

/// Script that appends `<cwd>|<README present>|<BRANCH_NAME>` to `marker`.
fn recording_script(marker: &Path) -> String {
    format!(
        r#"if [ -f README.md ]; then readme=present; else readme=missing; fi; printf '%s|%s|%s\n' "$(pwd -P)" "$readme" "$BRANCH_NAME" >> '{}'"#,
        marker.display()
    )
}

fn marker_lines(marker: &Path) -> Vec<String> {
    std::fs::read_to_string(marker)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

async fn eventually<F: FnMut() -> bool>(what: &str, mut done: F) {
    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(20));
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        // timing-guard: poll interval between reads of an observable state, not a wait for time to pass
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait for exactly one script run and return its recorded line.
async fn await_single_run(marker: &Path) -> String {
    eventually("archive script ran", || !marker_lines(marker).is_empty()).await;
    let lines = marker_lines(marker);
    assert_eq!(
        lines.len(),
        1,
        "archive script ran more than once: {lines:?}"
    );
    lines[0].clone()
}

fn expected_line(worktree: &Path, branch: &str) -> String {
    let cwd = std::fs::canonicalize(worktree).expect("canonical worktree");
    format!("{}|present|{branch}", cwd.display())
}

async fn branch_of(daemon: &Daemon, id: &str) -> String {
    daemon
        .rpc(2, "workspace.get", json!({ "workspaceId": id }))
        .await["workspace"]["branch"]
        .as_str()
        .expect("branch")
        .to_string()
}

#[tokio::test]
async fn archive_runs_archive_script_once_in_the_workspace_folder() {
    let marker_dir = common::test_tempdir("archscript-marker-");
    let marker = marker_dir.path().join("runs.log");
    let repo = repo_with_archive_script(&recording_script(&marker));
    let daemon = Daemon::start().await;
    let (id, worktree) = daemon.create_workspace(repo.path()).await;
    let branch = branch_of(&daemon, &id).await;
    assert!(
        marker_lines(&marker).is_empty(),
        "script ran before archive"
    );

    let archived = daemon
        .rpc(3, "workspace.archive", json!({ "workspaceId": id }))
        .await;
    assert_eq!(archived["workspace"]["archived"], json!(true));
    assert_eq!(
        await_single_run(&marker).await,
        expected_line(&worktree, &branch)
    );

    // Archiving again, then deleting, does not run the script a second time.
    // A rerun would hold the gate the delete waits on before removing the
    // folder, so it would land before the folder is gone.
    daemon
        .rpc(5, "workspace.archive", json!({ "workspaceId": id }))
        .await;
    daemon
        .rpc(4, "workspace.delete", json!({ "workspaceId": id }))
        .await;
    eventually("folder removed", || !worktree.exists()).await;
    assert_eq!(
        marker_lines(&marker).len(),
        1,
        "script re-ran on re-archive or delete"
    );

    let log = std::fs::read_to_string(daemon.log_path()).unwrap_or_default();
    assert!(
        log.contains("executing archive script"),
        "no start log line"
    );
    assert!(log.contains("archive script finished"), "no exit log line");
}

#[tokio::test]
async fn deleting_an_active_workspace_runs_archive_script_before_folder_removal() {
    let marker_dir = common::test_tempdir("archscript-marker-");
    let marker = marker_dir.path().join("runs.log");
    let repo = repo_with_archive_script(&recording_script(&marker));
    let daemon = Daemon::start().await;
    let (id, worktree) = daemon.create_workspace(repo.path()).await;
    let branch = branch_of(&daemon, &id).await;
    let expected = expected_line(&worktree, &branch);

    daemon
        .rpc(3, "workspace.delete", json!({ "workspaceId": id }))
        .await;
    assert_eq!(await_single_run(&marker).await, expected);
    eventually("folder removed", || !worktree.exists()).await;
}

#[tokio::test]
async fn purging_an_active_workspace_runs_archive_script_once_before_folder_removal() {
    let marker_dir = common::test_tempdir("archscript-marker-");
    let marker = marker_dir.path().join("runs.log");
    let repo = repo_with_archive_script(&recording_script(&marker));
    let daemon = Daemon::start().await;
    let (id, worktree) = daemon.create_workspace(repo.path()).await;
    let branch = branch_of(&daemon, &id).await;
    let expected = expected_line(&worktree, &branch);

    let retired = daemon
        .rpc(3, "workspace.retire", json!({ "workspaceId": id }))
        .await;
    assert_eq!(retired["workspace"]["status"], json!("Retired"));
    assert_eq!(await_single_run(&marker).await, expected);
    eventually("folder removed", || !worktree.exists()).await;
    assert_eq!(marker_lines(&marker).len(), 1, "script ran more than once");
}

#[tokio::test]
async fn failing_archive_script_still_archives_and_shows_the_failure_on_the_card() {
    let repo = repo_with_archive_script("echo cleanup broke >&2; exit 7");
    let daemon = Daemon::start().await;
    let (id, worktree) = daemon.create_workspace(repo.path()).await;

    let archived = daemon
        .rpc(3, "workspace.archive", json!({ "workspaceId": id }))
        .await;
    assert_eq!(archived["workspace"]["archived"], json!(true));

    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(20));
    loop {
        let ws = daemon
            .rpc(4, "workspace.get", json!({ "workspaceId": id }))
            .await["workspace"]
            .clone();
        if ws["statusMessage"] == json!("Archive script failed (exit code 7).") {
            assert_eq!(ws["archived"], json!(true), "workspace must stay archived");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "statusMessage never reported the failure: {ws}"
        );
        // timing-guard: poll interval between reads of an observable state, not a wait for time to pass
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(worktree.exists(), "archive keeps the folder");
    let log = std::fs::read_to_string(daemon.log_path()).unwrap_or_default();
    assert!(
        log.contains("cleanup broke"),
        "script output tail not logged"
    );
}

#[tokio::test]
async fn unarchive_waits_for_a_running_archive_script() {
    let marker_dir = common::test_tempdir("archscript-marker-");
    let marker = marker_dir.path().join("runs.log");
    let script = format!(
        // timing-guard: the script must still be running when unarchive is called
        "printf start >> '{m}'; sleep 2; printf ' end' >> '{m}'",
        m = marker.display()
    );
    let repo = repo_with_archive_script(&script);
    let daemon = Daemon::start().await;
    let (id, _worktree) = daemon.create_workspace(repo.path()).await;

    daemon
        .rpc(3, "workspace.archive", json!({ "workspaceId": id }))
        .await;
    eventually("archive script started", || marker.exists()).await;
    let unarchived = daemon
        .rpc(4, "workspace.unarchive", json!({ "workspaceId": id }))
        .await;
    assert_eq!(unarchived["workspace"]["archived"], json!(false));
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap_or_default(),
        "start end",
        "unarchive returned while the archive script was still running"
    );
}
