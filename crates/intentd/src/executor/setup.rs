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
    SetupCommand, SetupState, SubmodulePin, MAX_SCRIPT_OUTPUT_BYTES, PREPARED_CHECKOUT_PROTOCOL,
};
use intent_core::AgentId;
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
    agent_id: AgentId,
    head: HeadLink,
    state: Mutex<OutputState>,
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
    pub(super) fn new(agent_id: AgentId, head: HeadLink) -> Arc<Self> {
        Arc::new(Self {
            agent_id,
            head,
            state: Mutex::new(OutputState::default()),
        })
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
            agent_id: self.agent_id.clone(),
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
        self.head
            .get()
            .filter(|link| link.protocol() >= PREPARED_CHECKOUT_PROTOCOL)
    }
}

/// What one setup run needs.
pub(super) struct SetupRun<'a> {
    pub command: &'a SetupCommand,
    pub worktree: &'a Path,
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
    let before = untracked(run.worktree, run.pins).await;
    let script = dir.join(format!("setup-{}.sh", intent_core::node_link::LinkId::new_v4()));
    let ran = spawn_and_wait(pty, &run, &script, output, started_at, stop).await;
    let _ = std::fs::remove_file(&script);
    output.finish();
    let after = untracked(run.worktree, run.pins).await;
    write_excludes(&excludes_dir(run.worktree), &before, after);
    let (tail, _) = output.tail();
    if let Err(error) = std::fs::write(dir.join(LOG_FILE), tail) {
        tracing::warn!(%error, "node: could not keep the setup output");
    }
    ran
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
    let vars = [
        ("WORKTREE_PATH", worktree.as_str()),
        ("MAIN_CHECKOUT", main.as_str()),
        ("BRANCH_NAME", run.command.branch_name.as_str()),
        ("SOURCE_BRANCH", run.command.source_branch.as_str()),
        ("INTENT_REMOTE_MACHINE", "1"),
    ];
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
    dir: &Path,
    before: &[(Option<usize>, Vec<String>)],
    after: Vec<(Option<usize>, Vec<String>)>,
) {
    let global = global_excludes();
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

/// The machine user's default global excludes file's patterns, if any.
fn global_excludes() -> String {
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")));
    config
        .and_then(|dir| std::fs::read_to_string(dir.join("git").join("ignore")).ok())
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
