//! Runs a workspace's `archiveScript` (intent-hq/intent#8850) as a plain
//! child process in the workspace folder. Unlike the setup script it has no
//! terminal: a delete runs it after the workspace row is gone. Output goes to
//! a private file under `.intent/`; its tail is logged when the run fails.

use std::path::Path;
use std::process::Stdio;
use std::time::Instant;

use intent_services::feature_api::feature_ports::{
    ArchiveScriptOutcome, ArchiveScriptRun, ARCHIVE_SCRIPT_TIMEOUT,
};
use intent_services::feature_api::write_private_script;

use crate::{script_ops, setup_runner};

/// How much of the script's output a failure log line carries.
const OUTPUT_TAIL_BYTES: usize = 4096;

pub(crate) async fn run(run: ArchiveScriptRun) -> ArchiveScriptOutcome {
    let workspace = run.workspace_id.as_str().to_string();
    let trigger = run.trigger.as_str();
    tracing::info!(
        workspace = %workspace,
        trigger,
        script_length = run.script.len(),
        worktree = %run.worktree.display(),
        "executing archive script"
    );
    let started = Instant::now();
    let (outcome, output) = execute(&run).await;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    if outcome == ArchiveScriptOutcome::Succeeded {
        tracing::info!(
            workspace = %workspace,
            trigger,
            exit_code = 0,
            elapsed_ms,
            "archive script finished"
        );
    } else {
        let exit_code = match outcome {
            ArchiveScriptOutcome::Failed { exit_code } => exit_code,
            _ => None,
        };
        tracing::warn!(
            workspace = %workspace,
            trigger,
            exit_code = ?exit_code,
            outcome = ?outcome,
            elapsed_ms,
            output = %output,
            "archive script finished with a failure"
        );
    }
    outcome
}

/// Spawn the script and wait for it; returns the outcome and the tail of its
/// combined output.
async fn execute(run: &ArchiveScriptRun) -> (ArchiveScriptOutcome, String) {
    let intent_dir = run.worktree.join(".intent");
    if let Err(reason) = ensure_private_dir(&intent_dir).await {
        tracing::warn!(
            workspace = %run.workspace_id.as_str(),
            reason = %reason,
            "archive script skipped: cannot use the .intent directory"
        );
        return (ArchiveScriptOutcome::CouldNotStart, String::new());
    }
    let runner = setup_runner::select(
        cfg!(windows),
        if cfg!(windows) {
            setup_runner::find_windows_sh()
        } else {
            None
        },
    );
    let id = uuid::Uuid::new_v4().simple();
    let script_path = intent_dir.join(format!("archive-{id}.{}", runner.script_extension()));
    let log_path = intent_dir.join(format!("archive-{id}.log"));
    let outcome = spawn_and_wait(run, &runner, &script_path, &log_path).await;
    let output = read_tail(&log_path).await;
    let _ = tokio::fs::remove_file(&script_path).await;
    let _ = tokio::fs::remove_file(&log_path).await;
    (outcome, output)
}

async fn spawn_and_wait(
    run: &ArchiveScriptRun,
    runner: &setup_runner::SetupScriptRunner,
    script_path: &Path,
    log_path: &Path,
) -> ArchiveScriptOutcome {
    let workspace = run.workspace_id.as_str();
    if let Err(e) = write_private_script(script_path, &run.script).await {
        tracing::warn!(workspace, error = %e, "failed to write archive script to private file");
        return ArchiveScriptOutcome::CouldNotStart;
    }
    let (stdout, stderr) = match std::fs::File::create(log_path)
        .and_then(|file| Ok((file.try_clone()?, file)))
    {
        Ok(files) => files,
        Err(e) => {
            tracing::warn!(workspace, error = %e, "failed to create archive script output file");
            return ArchiveScriptOutcome::CouldNotStart;
        }
    };
    let mut command = match runner {
        setup_runner::SetupScriptRunner::PosixSh { sh, .. } => {
            let mut command = tokio::process::Command::new(sh);
            command.arg(script_path.to_string_lossy().replace('\\', "/"));
            command
        }
        setup_runner::SetupScriptRunner::CmdWrapper => {
            let mut command = tokio::process::Command::new("cmd.exe");
            command.args(["/d", "/c"]).arg(script_path);
            command
        }
    };
    command
        .current_dir(&run.worktree)
        .env("MAIN_CHECKOUT", &run.repo_path)
        .env("WORKTREE_PATH", &run.worktree)
        .env("BRANCH_NAME", &run.branch_name)
        .env("SOURCE_BRANCH", &run.source_branch)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true);
    if let Some(path) = tokio::task::spawn_blocking(script_ops::enhanced_shell_path)
        .await
        .ok()
        .flatten()
    {
        command.env("PATH", path);
    }
    #[cfg(unix)]
    command.process_group(0);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            tracing::warn!(workspace, error = %e, "failed to spawn archive script");
            return ArchiveScriptOutcome::CouldNotStart;
        }
    };
    let pid = child.id();
    match tokio::time::timeout(ARCHIVE_SCRIPT_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) if status.success() => ArchiveScriptOutcome::Succeeded,
        Ok(Ok(status)) => ArchiveScriptOutcome::Failed {
            exit_code: status.code(),
        },
        Ok(Err(e)) => {
            tracing::warn!(workspace, error = %e, "failed to wait for archive script");
            ArchiveScriptOutcome::Failed { exit_code: None }
        }
        Err(_) => {
            kill_process_group(pid);
            let _ = child.kill().await;
            ArchiveScriptOutcome::TimedOut
        }
    }
}

/// Use `dir` for private script files: refuse a symlink or a file there
/// (a repository could commit one to redirect the write), else create it
/// owner-only.
async fn ensure_private_dir(dir: &Path) -> Result<(), String> {
    if let Ok(meta) = tokio::fs::symlink_metadata(dir).await {
        return if meta.is_dir() {
            Ok(())
        } else {
            Err("not a real directory (symlink or file)".to_string())
        };
    }
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = tokio::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).await;
    }
    Ok(())
}

async fn read_tail(path: &Path) -> String {
    let bytes = tokio::fs::read(path).await.unwrap_or_default();
    let start = bytes.len().saturating_sub(OUTPUT_TAIL_BYTES);
    String::from_utf8_lossy(&bytes[start..]).trim().to_string()
}

#[cfg(unix)]
fn kill_process_group(pid: Option<u32>) {
    if let Some(pid) = pid.and_then(|pid| i32::try_from(pid).ok()) {
        let _ = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(pid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
}

#[cfg(not(unix))]
fn kill_process_group(_pid: Option<u32>) {}
