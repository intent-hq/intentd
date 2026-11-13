//! The workspace setup script on the machine (nodeProtocol 10): run once in
//! a fresh checkout, after its submodules and before the provider starts.
//! The script file, its last result, its output log and the excludes that
//! keep its files out of `wip` pushes live in the agent's storage directory,
//! beside the checkout and never in it.
//!
//! The checkout's progress lines and the script's output reach head as
//! `setupOutput` frames; the result is journaled by the runner
//! (`setupStarted`, `setupFinished`).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::oneshot;

use intent_core::node_link::{
    SetupCommand, SetupOwner, SetupState, SubmodulePin, HOMES_PROTOCOL, MAX_SCRIPT_OUTPUT_BYTES,
    PREPARED_CHECKOUT_PROTOCOL,
};
use intent_pty::{OutputSink, PtyHost, SpawnSpec};
use intent_services::feature_api::shell::{saved_script_env_overlay, scrubbed_env_vars_except};
use intent_transport::node_link::NodeLink;

use super::scripts::{split_utf8, take_utf8};
use super::workspace_api::HeadLink;

/// `setupFinished.error` codes.
pub(super) mod codes {
    /// The script could not be written or spawned.
    pub const FAILED: &str = intent_core::node_link::SETUP_FAILED_CODE;
    /// The script ran past its timeout and was killed.
    pub const TIMEOUT: &str = "node-setup-timeout";
    /// A stop or machine restart ended the script before it finished.
    pub const INTERRUPTED: &str = "node-setup-interrupted";
}

/// How long a setup script may run when the launch names no timeout.
pub(super) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// The output kept on the machine (and resent once after a reconnect).
const OUTPUT_TAIL_BYTES: usize = 256 * 1024;

/// The output a failure notice quotes.
const NOTICE_TAIL_BYTES: usize = 4 * 1024;

/// How long the run waits for the terminal's output to end after the
/// script's process group is reaped.
const OUTPUT_EOF_WAIT: Duration = Duration::from_secs(2);

/// The PTY's own scrollback; the output itself is kept by [`SetupOutput`].
const SCROLLBACK_BYTES: usize = 64 * 1024;

const STATUS_FILE: &str = "setup.json";
const LOG_FILE: &str = "setup.log";
/// The untracked paths before the script ran, so an interrupted run's
/// files can still be excluded at the next launch.
const BEFORE_FILE: &str = "setup-before.json";
const EXCLUDES_DIR: &str = "setup-exclude";

/// The agent's storage directory: its checkout's parent.
pub(super) fn agent_dir(worktree: &Path) -> PathBuf {
    worktree
        .parent()
        .map_or_else(|| worktree.to_path_buf(), Path::to_path_buf)
}

/// Where the excludes of the agent's setup-created files are kept, one
/// file per repository ([`intent_git::node_git::exclude_file`]).
pub(super) fn excludes_dir(worktree: &Path) -> PathBuf {
    agent_dir(worktree).join(EXCLUDES_DIR)
}

/// The recorded state of the checkout's setup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum Recorded {
    /// Started and not finished: a stop or restart interrupted it.
    Running,
    Completed,
    Failed,
    Skipped,
}

/// The checkout's setup result, kept in the agent's storage directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SetupStatus {
    pub state: Recorded,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

impl SetupStatus {
    pub(super) fn skipped() -> Self {
        Self {
            state: Recorded::Skipped,
            exit_code: None,
            error: None,
            started_at: None,
            finished_at: Some(intent_core::now_iso()),
        }
    }

    fn failed(started_at: Option<String>, code: &str, detail: &str) -> Self {
        Self {
            state: Recorded::Failed,
            exit_code: None,
            error: Some(format!("{code}: {detail}")),
            started_at,
            finished_at: Some(intent_core::now_iso()),
        }
    }

    /// A setup that was running when it was stopped or the machine
    /// restarted, recorded at the next launch.
    pub(super) fn interrupted(self) -> Self {
        Self::failed(
            self.started_at,
            codes::INTERRUPTED,
            "the setup script was stopped before it finished and does not run again",
        )
    }

    /// A kept checkout with no recorded setup (a launch stopped before its
    /// checkout was ready, or one from a machine without setup): the script
    /// never ran there and does not run now.
    pub(super) fn never_ran() -> Self {
        Self::failed(
            None,
            codes::INTERRUPTED,
            "this checkout was kept from a launch that did not run the setup script; it does not run again",
        )
    }

    /// The `setupFinished` state; `None` while running.
    pub(super) fn finished_state(&self) -> Option<SetupState> {
        match self.state {
            Recorded::Running => None,
            Recorded::Completed => Some(SetupState::Completed),
            Recorded::Failed => Some(SetupState::Failed),
            Recorded::Skipped => Some(SetupState::Skipped),
        }
    }

    /// The `setupStatus` a remote agent's `workspace.details` shows.
    pub(super) fn overlay(&self) -> Value {
        let state = match self.state {
            Recorded::Running => "running",
            Recorded::Completed => "completed",
            Recorded::Failed => "failed",
            Recorded::Skipped => "skipped",
        };
        let mut value = json!({ "state": state });
        if let Some(code) = self.exit_code {
            value["exitCode"] = json!(code);
        }
        if let Some(at) = &self.started_at {
            value["startedAt"] = json!(at);
        }
        if let Some(at) = &self.finished_at {
            value["finishedAt"] = json!(at);
        }
        value
    }
}

/// The checkout's recorded setup, if any.
pub(super) fn read_status(worktree: &Path) -> Option<SetupStatus> {
    let body = std::fs::read(agent_dir(worktree).join(STATUS_FILE)).ok()?;
    serde_json::from_slice(&body).ok()
}

/// Record the checkout's setup state.
pub(super) fn write_status(worktree: &Path, status: &SetupStatus) {
    let file = agent_dir(worktree).join(STATUS_FILE);
    let written = serde_json::to_vec(status)
        .map_err(std::io::Error::other)
        .and_then(|body| {
            let scratch = file.with_extension("json.tmp");
            std::fs::write(&scratch, body)?;
            std::fs::rename(&scratch, &file)
        });
    if let Err(error) = written {
        tracing::warn!(%error, "node: could not record the setup state");
    }
}

/// One fresh checkout's progress and setup output: UTF-8 text (invalid
/// bytes replaced, a character split across chunks kept whole), at most
/// [`MAX_SCRIPT_OUTPUT_BYTES`] per `setupOutput`, `seq` from 0. Output the
/// link cannot take is dropped and reported by one marker once there is
/// room; the last [`OUTPUT_TAIL_BYTES`] are kept and resent once after a
/// reconnect.
pub(super) struct SetupOutput {
    owner: SetupOwner,
    head: HeadLink,
    state: Mutex<OutputState>,
    /// The log in the agent's storage directory, appended as output
    /// arrives so a crash keeps what ran.
    log: Mutex<Option<std::fs::File>>,
}

#[derive(Default)]
struct OutputState {
    seq: u64,
    carry: Vec<u8>,
    dropped: usize,
    tail: String,
    truncated: bool,
}

impl SetupOutput {
    pub(super) fn new(owner: impl Into<SetupOwner>, head: HeadLink) -> Arc<Self> {
        Arc::new(Self {
            owner: owner.into(),
            head,
            state: Mutex::new(OutputState::default()),
            log: Mutex::new(None),
        })
    }

    /// Start the log at `path` with the output so far.
    fn start_log(&self, path: &Path) {
        let (tail, _) = self.tail();
        let file = std::fs::File::create(path).and_then(|mut file| {
            use std::io::Write as _;
            file.write_all(tail.as_bytes())?;
            Ok(file)
        });
        match file {
            Ok(file) => *lock(&self.log) = Some(file),
            Err(error) => tracing::warn!(%error, "node: could not start the setup log"),
        }
    }

    /// Replace the log with the kept tail and stop appending.
    fn close_log(&self, path: &Path) {
        lock(&self.log).take();
        let (tail, _) = self.tail();
        if let Err(error) = std::fs::write(path, tail) {
            tracing::warn!(%error, "node: could not keep the setup output");
        }
    }

    /// One progress line of the checkout.
    pub(super) fn line(&self, text: &str) {
        let mut state = lock(&self.state);
        self.send_text(&mut state, &format!("{text}\n"));
    }

    fn push(&self, bytes: &[u8]) {
        let mut state = lock(&self.state);
        state.carry.extend_from_slice(bytes);
        let text = take_utf8(&mut state.carry);
        self.send_text(&mut state, &text);
    }

    /// After the last chunk: send an unfinished trailing character as
    /// U+FFFD and report dropped output if there is room.
    fn finish(&self) {
        let mut state = lock(&self.state);
        if !state.carry.is_empty() {
            let text = String::from_utf8_lossy(&std::mem::take(&mut state.carry)).into_owned();
            self.send_text(&mut state, &text);
        }
        self.report_dropped(&mut state);
    }

    /// The kept output, and whether earlier output was cut from it.
    pub(super) fn tail(&self) -> (String, bool) {
        let state = lock(&self.state);
        (state.tail.clone(), state.truncated)
    }

    /// Send the kept output again from `seq` 0 on `link` (a new link to
    /// head), so head replaces what it has.
    pub(super) fn resend(&self, link: &NodeLink) {
        let mut state = lock(&self.state);
        state.seq = 0;
        state.dropped = 0;
        let tail = state.tail.clone();
        for piece in split_utf8(&tail, MAX_SCRIPT_OUTPUT_BYTES) {
            if !self.try_send_on(link, &mut state, piece) {
                state.dropped += piece.len();
            }
        }
        self.report_dropped(&mut state);
    }

    fn send_text(&self, state: &mut OutputState, text: &str) {
        if text.is_empty() {
            return;
        }
        if let Some(file) = lock(&self.log).as_mut() {
            use std::io::Write as _;
            let _ = file.write_all(text.as_bytes());
        }
        state.tail.push_str(text);
        if state.tail.len() > OUTPUT_TAIL_BYTES {
            let mut cut = state.tail.len() - OUTPUT_TAIL_BYTES;
            while !state.tail.is_char_boundary(cut) {
                cut += 1;
            }
            state.tail.drain(..cut);
            state.truncated = true;
        }
        self.report_dropped(state);
        let Some(link) = self.link() else {
            // No link: the kept output is resent when one comes up.
            return;
        };
        for piece in split_utf8(text, MAX_SCRIPT_OUTPUT_BYTES) {
            if state.dropped > 0 || !self.try_send_on(&link, state, piece) {
                state.dropped += piece.len();
            }
        }
    }

    fn report_dropped(&self, state: &mut OutputState) {
        if state.dropped == 0 {
            return;
        }
        let Some(link) = self.link() else {
            return;
        };
        let marker = format!("[… {} bytes of output dropped]\n", state.dropped);
        if self.try_send_on(&link, state, &marker) {
            state.dropped = 0;
        }
    }

    fn try_send_on(&self, link: &NodeLink, state: &mut OutputState, data: &str) -> bool {
        let sent = link.try_send_setup_output(intent_core::node_link::SetupOutput {
            owner: self.owner.clone(),
            seq: state.seq,
            data: data.to_owned(),
        });
        let queued = matches!(sent, Ok(true));
        if queued {
            state.seq += 1;
        }
        queued
    }

    fn link(&self) -> Option<NodeLink> {
        let needs = match self.owner {
            SetupOwner::Agent(_) => PREPARED_CHECKOUT_PROTOCOL,
            SetupOwner::Home(_) => HOMES_PROTOCOL,
        };
        self.head.get().filter(|link| link.protocol() >= needs)
    }
}

/// What one setup run needs.
pub(super) struct SetupRun<'a> {
    pub command: &'a SetupCommand,
    pub worktree: &'a Path,
    /// The agent's private temp folder, the script's `TMPDIR` as the agent's.
    pub tmp: &'a Path,
    /// The machine-local stand-in for head's main checkout.
    pub main_checkout: &'a Path,
    pub pins: &'a [SubmodulePin],
}

/// How a setup run ended.
pub(super) enum Ran {
    Finished(SetupStatus),
    /// The agent was stopped during setup: its result stays unrecorded.
    Stopped,
}

/// Run the setup script in the agent's checkout, in its own process group,
/// bounded by its timeout. `started_at` is the journaled `setupStarted`.
pub(super) async fn run(
    pty: &PtyHost,
    run: SetupRun<'_>,
    output: &Arc<SetupOutput>,
    started_at: String,
    stop: &mut oneshot::Receiver<()>,
) -> Ran {
    let dir = agent_dir(run.worktree);
    let excludes = excludes_dir(run.worktree);
    // A fresh run's excludes replace any earlier run's.
    let _ = std::fs::remove_dir_all(&excludes);
    let before = untracked(run.worktree, run.pins).await;
    match serde_json::to_vec(&before) {
        Ok(body) => {
            if let Err(error) = std::fs::write(dir.join(BEFORE_FILE), body) {
                tracing::warn!(%error, "node: could not record the untracked paths before setup");
            }
        }
        Err(error) => tracing::warn!(%error, "node: could not encode the untracked paths"),
    }
    output.start_log(&dir.join(LOG_FILE));
    let script = dir.join(format!(
        "setup-{}.sh",
        intent_core::node_link::LinkId::new_v4()
    ));
    let ran = spawn_and_wait(pty, &run, &script, output, started_at, stop).await;
    let _ = std::fs::remove_file(&script);
    output.finish();
    let after = untracked(run.worktree, run.pins).await;
    write_excludes(run.worktree, &excludes, &before, after);
    output.close_log(&dir.join(LOG_FILE));
    ran
}

/// After a setup a stop or restart interrupted: exclude what it created
/// from `wip` pushes, as a finished run does.
pub(super) async fn exclude_interrupted(worktree: &Path, pins: &[SubmodulePin]) {
    let before: Vec<(Option<usize>, Vec<String>)> =
        std::fs::read(agent_dir(worktree).join(BEFORE_FILE))
            .ok()
            .and_then(|body| serde_json::from_slice(&body).ok())
            .unwrap_or_default();
    let after = untracked(worktree, pins).await;
    write_excludes(worktree, &excludes_dir(worktree), &before, after);
}

async fn spawn_and_wait(
    pty: &PtyHost,
    run: &SetupRun<'_>,
    script: &Path,
    output: &Arc<SetupOutput>,
    started_at: String,
    stop: &mut oneshot::Receiver<()>,
) -> Ran {
    let started = Some(started_at);
    if let Err(error) =
        intent_services::feature_api::write_private_script(script, &run.command.script).await
    {
        return Ran::Finished(SetupStatus::failed(
            started,
            codes::FAILED,
            &format!("could not write the script: {error}"),
        ));
    }
    let worktree = run.worktree.to_string_lossy().into_owned();
    let main = run.main_checkout.to_string_lossy().into_owned();
    let tmp = run.tmp.to_string_lossy().into_owned();
    let vars = [
        ("WORKTREE_PATH", worktree.as_str()),
        ("MAIN_CHECKOUT", main.as_str()),
        ("BRANCH_NAME", run.command.branch_name.as_str()),
        ("SOURCE_BRANCH", run.command.source_branch.as_str()),
        ("INTENT_REMOTE_MACHINE", "1"),
    ]
    .into_iter()
    .chain(intent_core::AGENT_TMP_ENV_KEYS.map(|key| (key, tmp.as_str())));
    let mut spec = SpawnSpec::new("remote-setup".to_owned(), "/bin/sh".to_owned());
    spec.args = vec![
        "-c".to_owned(),
        intent_services::feature_api::SETUP_SCRIPT_WRAPPER.to_owned(),
        "sh".to_owned(),
        script.to_string_lossy().into_owned(),
    ];
    spec.env = saved_script_env_overlay(Some(run.worktree), vars);
    spec.env_remove = scrubbed_env_vars_except(&spec.env);
    spec.env_remove
        .push(intent_git::node_git::SOCKET_ENV.to_owned());
    spec.cwd = Some(run.worktree.to_path_buf());
    spec.listed = false;
    spec.scrollback_bytes = SCROLLBACK_BYTES;
    let writer = output.clone();
    spec.output_sink = Some(OutputSink::new(move |bytes: &[u8]| writer.push(bytes)));
    let pty_id = match pty.spawn(spec) {
        Ok(id) => id,
        Err(error) => {
            return Ran::Finished(SetupStatus::failed(
                started,
                codes::FAILED,
                &format!("could not start the script: {error}"),
            ));
        }
    };
    let timeout = run
        .command
        .timeout_secs
        .map_or(DEFAULT_TIMEOUT, Duration::from_secs);
    let exit = tokio::select! {
        exit = pty.wait(pty_id) => Some(exit),
        () = tokio::time::sleep(timeout) => None,
        _ = &mut *stop => {
            pty.kill(pty_id).await;
            return Ran::Stopped;
        }
    };
    // Reap the group first so the output can reach EOF and nothing the
    // script started outlives the run.
    match exit {
        Some(_) => pty.reap_group_stragglers(pty_id).await,
        None => {
            pty.kill(pty_id).await;
        }
    }
    let _ = tokio::time::timeout(OUTPUT_EOF_WAIT, pty.wait_output_eof(pty_id)).await;
    pty.kill(pty_id).await;
    let finished_at = Some(intent_core::now_iso());
    Ran::Finished(match exit {
        None => SetupStatus::failed(
            started,
            codes::TIMEOUT,
            &format!(
                "the setup script ran past {}s and was killed",
                timeout.as_secs()
            ),
        ),
        Some(Ok(exit)) if exit.observed => {
            let code = i32::try_from(exit.exit_code).unwrap_or(-1);
            SetupStatus {
                state: if code == 0 {
                    Recorded::Completed
                } else {
                    Recorded::Failed
                },
                exit_code: Some(code),
                error: None,
                started_at: started,
                finished_at,
            }
        }
        Some(_) => SetupStatus::failed(
            started,
            codes::FAILED,
            "the setup script's exit status could not be read",
        ),
    })
}

/// The notice naming the submodules populated in the checkout that are not
/// the agent's pins, or `None` when there are none.
pub(super) fn unsent_notice(paths: &[String]) -> Option<String> {
    let verb = match paths {
        [] => return None,
        [_] => "was",
        _ => "were",
    };
    Some(format!(
        "[System: {} {verb} populated on this machine (by the setup script or an earlier \
         session), not carried from the backend; changes there won't be sent back to the \
         backend.]",
        paths.join(", ")
    ))
}

/// The first turn's notice for a setup that did not complete.
pub(super) fn failed_notice(status: &SetupStatus, worktree: &Path, tail: &str) -> Option<String> {
    if status.state != Recorded::Failed {
        return None;
    }
    let outcome = match (status.exit_code, status.error.as_deref()) {
        (Some(code), _) => format!("the setup script exited with code {code}"),
        (None, Some(error)) if error.starts_with(codes::TIMEOUT) => {
            "the setup script timed out and was killed".to_owned()
        }
        (None, Some(error)) if error.starts_with(codes::INTERRUPTED) => {
            "the setup script was stopped before it finished and did not run again".to_owned()
        }
        (None, error) => format!(
            "the setup script could not run ({})",
            error.unwrap_or("no detail")
        ),
    };
    let log = agent_dir(worktree).join(LOG_FILE);
    let mut cut = tail.len().saturating_sub(NOTICE_TAIL_BYTES);
    while !tail.is_char_boundary(cut) {
        cut += 1;
    }
    let last = tail[cut..].trim_end();
    let quoted = if last.is_empty() {
        String::new()
    } else {
        format!(" Its last output:\n```\n{last}\n```\n")
    };
    Some(format!(
        "[System: workspace setup failed — {outcome}. It ran on this machine before you started; \
         its output is in {}.{quoted} The checkout may be missing tooling the script installs: \
         read that output before diagnosing missing files, and tell the user setup needs \
         attention.]",
        log.display()
    ))
}

/// Each repository's untracked, not ignored paths (whole untracked
/// directories as one `dir/` entry): the superproject's under `None`, a
/// populated pin's under its index. A repository git cannot read lists
/// nothing.
async fn untracked(worktree: &Path, pins: &[SubmodulePin]) -> Vec<(Option<usize>, Vec<String>)> {
    let mut repos = vec![(None, worktree.to_path_buf())];
    repos.extend(
        pins.iter()
            .enumerate()
            .map(|(index, pin)| (Some(index), worktree.join(&pin.path)))
            .filter(|(_, dir)| dir.join(".git").exists()),
    );
    let mut listed = Vec::new();
    for (repo, dir) in repos {
        let out = tokio::process::Command::new("git")
            .arg("-C")
            .arg(&dir)
            .args([
                "ls-files",
                "-z",
                "--others",
                "--exclude-standard",
                "--directory",
            ])
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .await;
        let paths = match out {
            Ok(out) if out.status.success() => out
                .stdout
                .split(|b| *b == 0)
                .filter(|p| !p.is_empty())
                .filter_map(|p| std::str::from_utf8(p).ok().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        };
        listed.push((repo, paths));
    }
    listed
}

/// Write each repository's excludes: the paths `after` lists that `before`
/// did not, after the machine user's global excludes (the file replaces
/// git's global excludes file for the push).
fn write_excludes(
    worktree: &Path,
    dir: &Path,
    before: &[(Option<usize>, Vec<String>)],
    after: Vec<(Option<usize>, Vec<String>)>,
) {
    let global = global_excludes(worktree);
    for (repo, paths) in after {
        let old = before
            .iter()
            .find(|(r, _)| *r == repo)
            .map(|(_, paths)| paths.as_slice())
            .unwrap_or_default();
        let new: Vec<String> = paths
            .into_iter()
            .filter(|path| !old.contains(path) && !path.contains('\n'))
            .map(|path| format!("/{}", escape_pattern(&path)))
            .collect();
        if new.is_empty() {
            continue;
        }
        let mut body = global.clone();
        if !body.is_empty() && !body.ends_with('\n') {
            body.push('\n');
        }
        body.push_str("# Created by the workspace setup script on this machine\n");
        for line in new {
            body.push_str(&line);
            body.push('\n');
        }
        let file = intent_git::node_git::exclude_file(dir, repo);
        let written = std::fs::create_dir_all(dir).and_then(|()| std::fs::write(&file, body));
        if let Err(error) = written {
            tracing::warn!(%error, "node: could not record the setup files' excludes");
        }
    }
}

/// The patterns of the machine user's global excludes file: the one
/// `core.excludesFile` names, else git's default `git/ignore` under
/// `$XDG_CONFIG_HOME` (or `~/.config`).
fn global_excludes(worktree: &Path) -> String {
    let configured = std::process::Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["config", "--path", "--get", "core.excludesFile"])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);
    let file = configured.or_else(|| {
        std::env::var_os("XDG_CONFIG_HOME")
            .filter(|dir| !dir.is_empty())
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
            .map(|dir| dir.join("git").join("ignore"))
    });
    file.and_then(|file| std::fs::read_to_string(file).ok())
        .unwrap_or_default()
}

/// `path` as a literal gitignore pattern.
fn escape_pattern(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        if matches!(c, '\\' | '*' | '?' | '[' | '!' | '#') {
            out.push('\\');
        }
        out.push(c);
    }
    let trailing = out.len() - out.trim_end_matches(' ').len();
    if trailing > 0 {
        out.truncate(out.len() - trailing);
        for _ in 0..trailing {
            out.push_str("\\ ");
        }
    }
    out
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_unsent_notice_names_each_submodule_once() {
        assert_eq!(unsent_notice(&[]), None);
        let one = unsent_notice(&["packages/ios".to_owned()]).unwrap();
        assert!(
            one.contains("packages/ios was populated on this machine"),
            "{one}"
        );
        let two = unsent_notice(&["a".to_owned(), "b".to_owned()]).unwrap();
        assert!(two.contains("a, b were populated on this machine"), "{two}");
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .env_remove("GIT_DIR")
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    /// A repository at `<root>/work` (the agent dir is `<root>`).
    fn repo(root: &Path) -> PathBuf {
        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "--quiet"]);
        std::fs::write(work.join("tracked.txt"), "t\n").unwrap();
        git(&work, &["add", "tracked.txt"]);
        git(
            &work,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "commit",
                "--quiet",
                "-m",
                "t",
            ],
        );
        work
    }

    /// The excludes file replaces git's global one for a push, so it
    /// carries the patterns of the file `core.excludesFile` names.
    #[test]
    fn excludes_carry_the_configured_global_excludes_file() {
        let root = tempfile::tempdir().unwrap();
        let work = repo(root.path());
        let global = root.path().join("global-ignore");
        std::fs::write(&global, "secret\n").unwrap();
        git(
            &work,
            &["config", "core.excludesFile", global.to_str().unwrap()],
        );
        std::fs::write(work.join("made by setup"), "x").unwrap();
        let after = vec![(None, vec!["made by setup".to_owned()])];
        let dir = excludes_dir(&work);
        write_excludes(&work, &dir, &[], after);
        let body = std::fs::read_to_string(intent_git::node_git::exclude_file(&dir, None)).unwrap();
        assert!(body.lines().any(|line| line == "secret"), "{body}");
        assert!(body.lines().any(|line| line == "/made by setup"), "{body}");
    }

    /// An interrupted setup's files are excluded at the next launch from
    /// the untracked paths recorded before it ran.
    #[tokio::test]
    async fn an_interrupted_setups_files_are_excluded() {
        let root = tempfile::tempdir().unwrap();
        let work = repo(root.path());
        std::fs::write(work.join("before.txt"), "b").unwrap();
        let before = untracked(&work, &[]).await;
        std::fs::write(
            agent_dir(&work).join(BEFORE_FILE),
            serde_json::to_vec(&before).unwrap(),
        )
        .unwrap();
        std::fs::create_dir_all(work.join("node_modules/x")).unwrap();
        std::fs::write(work.join("node_modules/x/i.js"), "x").unwrap();
        exclude_interrupted(&work, &[]).await;
        let body = std::fs::read_to_string(intent_git::node_git::exclude_file(
            &excludes_dir(&work),
            None,
        ))
        .unwrap();
        assert!(body.lines().any(|line| line == "/node_modules/"), "{body}");
        assert!(!body.contains("before.txt"), "{body}");
    }

    #[test]
    fn patterns_are_literal() {
        assert_eq!(escape_pattern("a*b?[c]"), "a\\*b\\?\\[c]");
        assert_eq!(escape_pattern("#x!"), "\\#x\\!");
        assert_eq!(escape_pattern("trail  "), "trail\\ \\ ");
    }
}
