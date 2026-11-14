//! Machine runs of saved scripts (nodeProtocol 8): the backend keeps a remote
//! agent's script definitions, runtime state and output log, and only the
//! process runs here, in the agent's checkout. At nodeProtocol 19 a frame may
//! name a workspace home instead of an agent: that is the workspace's home
//! run, in its home checkout, otherwise the same. Each `scriptStart` is one
//! process, spawned through the same shell, process group, PTY capture and
//! environment as a local saved script (never the agent's provider
//! credentials), answered with `scriptStarted`, its output as `scriptOutput`
//! frames and one `scriptExited`. Every `scriptStop` is answered with a
//! `scriptExited`: the stopped process's own, or `node-script-not-running`
//! when the triple names no running process. The machine never restarts a
//! script; the backend's supervisor does, with a new `spawnId`.
//!
//! Processes belong to the link binding that started them: when that link
//! ends, or its agent is stopped, they are killed, so no script outlives
//! its link.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use intent_core::node_link::{
    CheckoutOwner, LinkFrame, ScriptExited, ScriptOutput, ScriptProcessRef, ScriptStartCommand,
    ScriptStarted, MAX_SCRIPT_OUTPUT_BYTES, NODE_HOME_UNKNOWN,
};
use intent_core::nodes::NodeCounter;
use intent_core::AgentId;
use intent_pty::{OutputSink, PtyHost, PtyId, SpawnSpec};
use intent_services::feature_api::shell::{
    default_shell, saved_script_env_overlay, scrubbed_env_vars_except, shell_args,
};
use intent_transport::node_link::NodeLink;

/// `scriptExited` error codes.
const AGENT_UNKNOWN: &str = "node-script-agent-unknown";
const INVALID_CWD: &str = "node-script-invalid-cwd";
const START_FAILED: &str = "node-script-start-failed";
const NOT_RUNNING: &str = "node-script-not-running";

/// In-memory scrollback of a machine script's PTY. The output is streamed
/// to the backend, which keeps the run's log; this only backs the PTY.
const SCROLLBACK_BYTES: usize = 64 * 1024;

/// How long the reaper waits for the terminal's output to end after the
/// process group is reaped.
const OUTPUT_EOF_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// Slack on top of the teardown and reap timings in [`reap_wait`].
const REAP_MARGIN: std::time::Duration = std::time::Duration::from_secs(2);

/// [`reap_wait`] outside tests, as a constant so the machine's shutdown
/// budget can be checked at compile time.
pub(super) const REAP_WAIT: std::time::Duration = std::time::Duration::from_secs(
    intent_pty::TERM_GRACE.as_secs() * 2 + OUTPUT_EOF_WAIT.as_secs() + REAP_MARGIN.as_secs(),
);

/// Longest a stop waits for the processes it kills to be reaped: the kill's
/// SIGTERM grace, the reaper's own group escalation, its output-EOF wait and
/// a margin. A process past it (a child stuck in the kernel) is logged and
/// left; the caller carries on.
pub(super) fn reap_wait() -> std::time::Duration {
    let bound = REAP_WAIT;
    #[cfg(test)]
    let bound = intentd_test_support::test_timeout(bound);
    bound
}

/// Agents whose script reapers never finish (test seam for a process whose
/// `wait` never returns).
#[cfg(test)]
static HELD_REAPS: std::sync::LazyLock<Mutex<std::collections::HashSet<CheckoutOwner>>> =
    std::sync::LazyLock::new(Mutex::default);

/// Make every reaper of `agent_id`'s scripts hang (test seam).
#[cfg(test)]
pub(super) fn hold_reaps_for_test(agent_id: &AgentId) {
    lock(&HELD_REAPS).insert(agent_id.clone().into());
}

/// Agents whose next script kill panics, and whose script reapers panic
/// (test seams).
#[cfg(test)]
static PANIC_KILLS: std::sync::LazyLock<Mutex<std::collections::HashSet<CheckoutOwner>>> =
    std::sync::LazyLock::new(Mutex::default);
#[cfg(test)]
static PANIC_REAPS: std::sync::LazyLock<Mutex<std::collections::HashSet<CheckoutOwner>>> =
    std::sync::LazyLock::new(Mutex::default);

/// Make the next kill of one of `agent_id`'s scripts panic (test seam).
#[cfg(test)]
pub(super) fn panic_next_kill_for_test(agent_id: &AgentId) {
    lock(&PANIC_KILLS).insert(agent_id.clone().into());
}

/// Make every reaper of `agent_id`'s scripts panic (test seam).
#[cfg(test)]
pub(super) fn panic_reaps_for_test(agent_id: &AgentId) {
    lock(&PANIC_REAPS).insert(agent_id.clone().into());
}

/// A process is named by its owner (an agent or a home), script and spawn.
type Key = (CheckoutOwner, String, String);

struct Process {
    pty_id: PtyId,
    generation: NodeCounter,
    /// Turns true once its reaper has seen the process exit and recorded
    /// it, however the kill was started (`scriptStop`, the binding's end or
    /// the agent's stop). A closed channel counts as exited.
    reaped: tokio::sync::watch::Receiver<bool>,
}

/// The script processes of one backend's agents and homes on this machine.
#[derive(Clone)]
pub(super) struct ScriptProcesses {
    pty: Arc<PtyHost>,
    running: Arc<Mutex<HashMap<Key, Process>>>,
    /// Kills under way, each until its whole process-group teardown is
    /// done (`true`). A later stop joins one instead of starting a kill
    /// that finds the terminal already taken and returns at once.
    kills: Arc<Mutex<HashMap<Key, Kill>>>,
    /// Set by [`Self::stop_all`]: the machine is shutting down, so no new
    /// process starts.
    shutting_down: Arc<std::sync::atomic::AtomicBool>,
    /// Starts waiting for their checkout (a home being reopened), each
    /// `true` once a `scriptStop` for it came: the start then answers that
    /// stop instead of spawning.
    pending: Arc<Mutex<HashMap<Key, bool>>>,
}

/// Removes a kill's entry from [`ScriptProcesses::kills`] when its task
/// ends, if the entry is still this kill's.
struct KillGuard {
    kills: Arc<Mutex<HashMap<Key, Kill>>>,
    key: Key,
    finished: tokio::sync::watch::Sender<bool>,
}

impl Drop for KillGuard {
    fn drop(&mut self) {
        let mut kills = lock(&self.kills);
        if kills
            .get(&self.key)
            .is_some_and(|kill| kill.done.same_channel(&self.finished.subscribe()))
        {
            kills.remove(&self.key);
        }
    }
}

/// One kill under way.
struct Kill {
    generation: NodeCounter,
    done: tokio::sync::watch::Receiver<bool>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn key(owner: &CheckoutOwner, script_id: &str, spawn_id: &str) -> Key {
    (owner.clone(), script_id.to_owned(), spawn_id.to_owned())
}

fn exited(key: &Key, exit_code: Option<i32>, error: Option<String>) -> LinkFrame {
    LinkFrame::ScriptExited(ScriptExited {
        owner: key.0.clone(),
        script_id: key.1.clone(),
        spawn_id: key.2.clone(),
        exit_code,
        error,
    })
}

/// `cwd` inside `checkout`: relative, with no `..` or root component.
fn resolve_cwd(checkout: &Path, cwd: Option<&str>) -> Option<PathBuf> {
    match cwd.filter(|cwd| !cwd.is_empty()) {
        None => Some(checkout.to_path_buf()),
        Some(rel) => {
            let rel = Path::new(rel);
            rel.components()
                .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
                .then(|| checkout.join(rel))
        }
    }
}

impl ScriptProcesses {
    pub(super) fn new(pty: Arc<PtyHost>) -> Self {
        Self {
            pty,
            running: Arc::default(),
            kills: Arc::default(),
            shutting_down: Arc::default(),
            pending: Arc::default(),
        }
    }

    /// Hold `command`'s start while its checkout is looked up off the
    /// link's frame gate: a `scriptStop` meanwhile cancels it.
    pub(super) fn reserve(&self, command: &ScriptStartCommand) {
        lock(&self.pending).insert(
            key(&command.owner, &command.script_id, &command.spawn_id),
            false,
        );
    }

    /// Start one process for `command` in `checkout` (its owner's checkout:
    /// the agent's, or the home checkout; `None` when the owner is unknown
    /// or its checkout is gone), started by the link binding `generation`.
    /// Every outcome is reported on `link`.
    pub(super) async fn start(
        &self,
        link: &NodeLink,
        generation: NodeCounter,
        checkout: Option<&Path>,
        command: ScriptStartCommand,
    ) {
        let key = key(&command.owner, &command.script_id, &command.spawn_id);
        let refuse =
            |code: &str, detail: &str| exited(&key, None, Some(format!("{code}: {detail}")));
        if lock(&self.pending).remove(&key) == Some(true) {
            let _ = link
                .send(&refuse(NOT_RUNNING, "stopped before it started"))
                .await;
            return;
        }
        if self.shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
            let _ = link
                .send(&refuse(START_FAILED, "the machine is shutting down"))
                .await;
            return;
        }
        let Some(checkout) = checkout else {
            let refusal = match &key.0 {
                CheckoutOwner::Agent(_) => {
                    refuse(AGENT_UNKNOWN, "the agent has no checkout on this machine")
                }
                CheckoutOwner::Home(home) => refuse(
                    NODE_HOME_UNKNOWN,
                    &format!("this machine holds no home {}", home.home_id),
                ),
            };
            let _ = link.send(&refusal).await;
            return;
        };
        let Some(cwd) = resolve_cwd(checkout, command.cwd.as_deref()) else {
            let _ = link
                .send(&refuse(INVALID_CWD, "cwd must stay inside the checkout"))
                .await;
            return;
        };
        if lock(&self.running).contains_key(&key) {
            tracing::warn!(owner = %key.0, "node: duplicate scriptStart for a running process; ignored");
            return;
        }
        let shell = default_shell();
        let scope = match &key.0 {
            CheckoutOwner::Agent(agent) => format!("remote-script:{agent}"),
            CheckoutOwner::Home(home) => format!("home-script:{}", home.home_id),
        };
        let mut spec = SpawnSpec::new(scope, shell.clone());
        spec.args = shell_args(&shell, &command.command);
        spec.env = saved_script_env_overlay(
            Some(&cwd),
            command
                .env
                .iter()
                .map(|var| (var.key.as_str(), var.value.as_str())),
        );
        spec.env_remove = scrubbed_env_vars_except(&spec.env);
        spec.cwd = Some(cwd);
        spec.listed = false;
        spec.scrollback_bytes = SCROLLBACK_BYTES;
        let (sink, stream) = output_sink(link.clone(), &key);
        spec.output_sink = Some(sink);
        let pty_id = match self.pty.spawn(spec) {
            Ok(id) => id,
            Err(error) => {
                let _ = link.send(&refuse(START_FAILED, &error.to_string())).await;
                return;
            }
        };
        let (reaped_tx, reaped) = tokio::sync::watch::channel(false);
        lock(&self.running).insert(
            key.clone(),
            Process {
                pty_id,
                generation,
                reaped,
            },
        );
        // A shutdown that began after the check above may have taken its
        // list before this process was recorded: end it here, so none
        // outlives the shutdown. Its reaper reports the exit.
        if self.shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
            drop(self.kill(&key, pty_id, generation));
        }
        let _ = link
            .send(&LinkFrame::ScriptStarted(ScriptStarted {
                owner: key.0.clone(),
                script_id: key.1.clone(),
                spawn_id: key.2.clone(),
                pid: self.pty.pid(pty_id).unwrap_or_default(),
            }))
            .await;
        let this = self.clone();
        let link = link.clone();
        // caller-binding: allow — node-local script process supervision; the node has no service layer
        tokio::spawn(async move { this.reap(link, key, pty_id, stream, reaped_tx).await });
    }

    /// Wait for the process to exit, then report it.
    async fn reap(
        &self,
        link: NodeLink,
        key: Key,
        pty_id: PtyId,
        stream: Arc<Mutex<OutputStream>>,
        reaped: tokio::sync::watch::Sender<bool>,
    ) {
        #[cfg(test)]
        if lock(&HELD_REAPS).contains(&key.0) {
            std::future::pending::<()>().await;
        }
        #[cfg(test)]
        assert!(
            !lock(&PANIC_REAPS).contains(&key.0),
            "test: the reaper panicked"
        );
        let exit = self.pty.wait(pty_id).await;
        // A descendant can outlive the direct child and keep the terminal
        // open: reap the group first, as a local saved script does, so the
        // output can reach EOF and no process outlives the run.
        self.pty.reap_group_stragglers(pty_id).await;
        // Let the reader deliver the last output before the exit, bounded:
        // a copy of the terminal that survives the reap must not hold up
        // the exit report.
        let _ = tokio::time::timeout(OUTPUT_EOF_WAIT, self.pty.wait_output_eof(pty_id)).await;
        // Release the session and reap any group member it left behind.
        self.pty.kill(pty_id).await;
        {
            let mut running = lock(&self.running);
            if running.get(&key).is_some_and(|p| p.pty_id == pty_id) {
                running.remove(&key);
            }
        }
        // The process is gone and recorded: a stop waiting on it goes ahead
        // without waiting for the output flush or the link.
        reaped.send_replace(true);
        // Output loss is always reported, and before the exit.
        finish_output(&link, &stream).await;
        let frame = match exit {
            Ok(exit) if exit.observed => exited(
                &key,
                Some(i32::try_from(exit.exit_code).unwrap_or(-1)),
                None,
            ),
            _ => exited(&key, Some(-1), Some("exit status unobservable".to_owned())),
        };
        let _ = link.send(&frame).await;
    }

    /// Stop one process. Its `scriptExited` acknowledges the stop; a triple
    /// naming no running process is answered at once with
    /// `node-script-not-running` and stops nothing.
    pub(super) fn stop(&self, link: &NodeLink, process: &ScriptProcessRef) {
        let key = key(&process.owner, &process.script_id, &process.spawn_id);
        // A start still waiting for its checkout answers this stop itself.
        if let Some(cancelled) = lock(&self.pending).get_mut(&key) {
            *cancelled = true;
            return;
        }
        let found = lock(&self.running)
            .get(&key)
            .map(|p| (p.pty_id, p.generation));
        if let Some((pty_id, generation)) = found {
            // The exit is reported by its reaper.
            drop(self.kill(&key, pty_id, generation));
        } else {
            let link = link.clone();
            // caller-binding: allow — node-local answer to a stop of no process
            tokio::spawn(async move {
                let _ = link
                    .send(&exited(
                        &key,
                        Some(-1),
                        Some(format!("{NOT_RUNNING}: no such process on this machine")),
                    ))
                    .await;
            });
        }
    }

    /// Kill `key`'s process group, or join the kill already under way.
    /// Resolves (turns `true`) once the whole group's teardown is done.
    fn kill(
        &self,
        key: &Key,
        pty_id: PtyId,
        generation: NodeCounter,
    ) -> tokio::sync::watch::Receiver<bool> {
        let mut kills = lock(&self.kills);
        if let Some(kill) = kills.get(key) {
            return kill.done.clone();
        }
        let (finished, done) = tokio::sync::watch::channel(false);
        kills.insert(
            key.clone(),
            Kill {
                generation,
                done: done.clone(),
            },
        );
        drop(kills);
        let pty = self.pty.clone();
        // Drops the kill's entry however its task ends (a panic included),
        // so the next stop kills again; joiners see `true` only when the
        // teardown finished, and a dropped sender otherwise.
        let guard = KillGuard {
            kills: self.kills.clone(),
            key: key.clone(),
            finished,
        };
        // caller-binding: allow — node-local script teardown; every stop of the process joins it
        tokio::spawn(async move {
            #[cfg(test)]
            assert!(
                !lock(&PANIC_KILLS).remove(&guard.key.0),
                "test: the kill panicked"
            );
            pty.kill(pty_id).await;
            guard.finished.send_replace(true);
        });
        done
    }

    /// Kill every process started on the link binding `generation`. While
    /// the machine is shutting down the kills are sent but not waited for:
    /// [`Self::stop_all`] already waited its one bounded reap wait for the
    /// same processes, and a link's end must not add another past the
    /// shutdown's budget (intent-hq/intent#8437).
    pub(super) async fn stop_binding(&self, generation: NodeCounter) {
        let waits = self.start_kills(|_, g| g == generation);
        if self.shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        Self::wait_reaped(waits).await;
    }

    /// Whether `process` is still recorded as running (until its reaper
    /// has recorded its exit).
    #[cfg(test)]
    pub(super) fn is_running(&self, process: &ScriptProcessRef) -> bool {
        lock(&self.running).contains_key(&key(
            &process.owner,
            &process.script_id,
            &process.spawn_id,
        ))
    }

    /// Kill every process of `agent_id`. Returns whether each was reaped
    /// within [`reap_wait`].
    pub(super) async fn stop_agent(&self, agent_id: &AgentId) -> bool {
        self.kill_where(|key, _| key.0.agent() == Some(agent_id))
            .await
    }

    /// Kill every process and start no more (the machine is shutting
    /// down).
    pub(super) async fn stop_all(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.kill_where(|_, _| true).await;
    }

    /// Kill every matching process (joining a kill already under way), then
    /// wait until each kill's whole process-group teardown is done and each
    /// process's reaper has recorded its exit. A kill started elsewhere
    /// (`scriptStop`) of a process already reaped is waited for too. The
    /// wait is bounded by [`reap_wait`]: past it, what is still not done is
    /// logged and left. A kill or reaper that panicked counts as not done.
    /// Returns whether everything finished.
    async fn kill_where(&self, matches: impl Fn(&Key, NodeCounter) -> bool) -> bool {
        Self::wait_reaped(self.start_kills(matches)).await
    }

    /// Kill every matching process (joining a kill already under way), and
    /// return what to wait on for each one's teardown and recorded exit.
    fn start_kills(
        &self,
        matches: impl Fn(&Key, NodeCounter) -> bool,
    ) -> Vec<tokio::sync::watch::Receiver<bool>> {
        let mut waits: Vec<tokio::sync::watch::Receiver<bool>> = Vec::new();
        let victims: Vec<(Key, PtyId, NodeCounter)> = {
            let running = lock(&self.running);
            for (key, p) in running.iter() {
                if matches(key, p.generation) {
                    waits.push(p.reaped.clone());
                }
            }
            running
                .iter()
                .filter(|(key, p)| matches(key, p.generation))
                .map(|(key, p)| (key.clone(), p.pty_id, p.generation))
                .collect()
        };
        for (key, pty_id, generation) in &victims {
            waits.push(self.kill(key, *pty_id, *generation));
        }
        for kill in lock(&self.kills)
            .iter()
            .filter(|(key, kill)| matches(key, kill.generation))
            .map(|(_, kill)| kill.done.clone())
        {
            waits.push(kill);
        }
        waits
    }

    /// Wait, bounded by [`reap_wait`], until every one of `waits` is done.
    /// Returns whether all were.
    async fn wait_reaped(waits: Vec<tokio::sync::watch::Receiver<bool>>) -> bool {
        // A kill or reaper whose task ended without finishing (a panic: its
        // sender dropped before `true`) is not done.
        let done = tokio::time::timeout(reap_wait(), async {
            let mut all = true;
            for mut wait in waits {
                all &= wait.wait_for(|done| *done).await.is_ok();
            }
            all
        })
        .await
        .unwrap_or(false);
        if !done {
            tracing::warn!("node: a script process was not reaped (its teardown failed or ran past its bound); leaving it");
        }
        done
    }
}

/// One process's output stream: UTF-8 text (invalid bytes replaced, a
/// character split across chunks kept whole), at most
/// [`MAX_SCRIPT_OUTPUT_BYTES`] per `scriptOutput`, `seq` from 0. When the
/// link's output queue is full the output is dropped and, once there is
/// room again, one marker frame says how much.
struct OutputStream {
    link: NodeLink,
    key: Key,
    seq: u64,
    carry: Vec<u8>,
    dropped: usize,
}

impl OutputStream {
    fn try_send(&mut self, data: String) -> bool {
        let sent = self.link.try_send_script_output(ScriptOutput {
            owner: self.key.0.clone(),
            script_id: self.key.1.clone(),
            spawn_id: self.key.2.clone(),
            seq: self.seq,
            data,
        });
        let queued = matches!(sent, Ok(true));
        if queued {
            self.seq += 1;
        }
        queued
    }

    /// Send the dropped-output marker if output was dropped; `true` once
    /// nothing dropped is unreported.
    fn report_dropped(&mut self) -> bool {
        if self.dropped == 0 {
            return true;
        }
        let marker = format!("[… {} bytes of output dropped]", self.dropped);
        if self.try_send(marker) {
            self.dropped = 0;
        }
        self.dropped == 0
    }

    fn send_text(&mut self, text: &str) {
        self.report_dropped();
        for piece in split_utf8(text, MAX_SCRIPT_OUTPUT_BYTES) {
            if self.dropped > 0 || !self.try_send(piece.to_owned()) {
                self.dropped += piece.len();
            }
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        self.carry.extend_from_slice(bytes);
        let text = take_utf8(&mut self.carry);
        self.send_text(&text);
    }

    /// After the last chunk: send an unfinished trailing character as
    /// U+FFFD and report any dropped output. `true` once everything is
    /// queued ahead of the exit; `false` while the queue is still full.
    fn finish(&mut self) -> bool {
        if !self.carry.is_empty() {
            let text = String::from_utf8_lossy(&std::mem::take(&mut self.carry)).into_owned();
            self.send_text(&text);
        }
        self.report_dropped()
    }
}

/// How long the reaper waits for room to report dropped output before the
/// exit; the link closing ends the wait at once.
const FINISH_OUTPUT_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

fn output_sink(link: NodeLink, key: &Key) -> (OutputSink, Arc<Mutex<OutputStream>>) {
    let stream = Arc::new(Mutex::new(OutputStream {
        link,
        key: key.clone(),
        seq: 0,
        carry: Vec::new(),
        dropped: 0,
    }));
    let writer = stream.clone();
    let sink = OutputSink::new(move |bytes: &[u8]| lock(&writer).push(bytes));
    (sink, stream)
}

/// Queue the stream's last text and dropped-output report ahead of the
/// exit, waiting (bounded) for room in the link's output queue.
async fn finish_output(link: &NodeLink, stream: &Mutex<OutputStream>) {
    let deadline = tokio::time::Instant::now() + FINISH_OUTPUT_WAIT;
    while !lock(stream).finish() {
        if link.is_closed() || tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Decode the complete UTF-8 prefix of `buf` (invalid bytes replaced),
/// leaving an incomplete trailing character for the next chunk.
pub(super) fn take_utf8(buf: &mut Vec<u8>) -> String {
    let keep = incomplete_tail(buf);
    let rest = buf.split_off(buf.len() - keep);
    let text = String::from_utf8_lossy(buf).into_owned();
    *buf = rest;
    text
}

/// The length of a trailing UTF-8 sequence that is started but not finished.
fn incomplete_tail(buf: &[u8]) -> usize {
    for back in 1..=buf.len().min(3) {
        let byte = buf[buf.len() - back];
        let width = match byte {
            0x80..=0xBF => continue,
            0xC0..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF7 => 4,
            _ => return 0,
        };
        return if width > back { back } else { 0 };
    }
    0
}

/// Split `text` into pieces of at most `max` bytes on character boundaries.
pub(super) fn split_utf8(text: &str, max: usize) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let mut end = rest.len().min(max);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        let (piece, tail) = rest.split_at(end);
        pieces.push(piece);
        rest = tail;
    }
    pieces
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn utf8_split_across_chunks_is_kept_whole() {
        let mut buf = "a€".as_bytes()[..2].to_vec();
        assert_eq!(take_utf8(&mut buf), "a");
        buf.extend_from_slice(&"€".as_bytes()[1..]);
        assert_eq!(take_utf8(&mut buf), "€");
        assert!(buf.is_empty());
        let mut bad = vec![b'x', 0xff, b'y'];
        assert_eq!(take_utf8(&mut bad), "x\u{fffd}y");
    }

    #[test]
    fn output_pieces_respect_the_frame_bound_and_char_boundaries() {
        let text = "é".repeat(5);
        let pieces = split_utf8(&text, 3);
        assert!(pieces.iter().all(|p| p.len() <= 3));
        assert_eq!(pieces.concat(), text);
    }

    #[test]
    fn cwd_must_stay_inside_the_checkout() {
        let root = Path::new("/co");
        assert_eq!(resolve_cwd(root, None), Some(root.to_path_buf()));
        assert_eq!(resolve_cwd(root, Some("")), Some(root.to_path_buf()));
        assert_eq!(resolve_cwd(root, Some("a/b")), Some(root.join("a/b")));
        assert_eq!(resolve_cwd(root, Some("../x")), None);
        assert_eq!(resolve_cwd(root, Some("/tmp")), None);
        assert_eq!(resolve_cwd(root, Some("a/../../x")), None);
    }
}
