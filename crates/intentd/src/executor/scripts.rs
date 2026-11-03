//! Machine runs of saved scripts (nodeProtocol 8): the backend keeps a remote
//! agent's script definitions, runtime state and output log, and only the
//! process runs here, in the agent's checkout. Each `scriptStart` is one
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
    LinkFrame, ScriptExited, ScriptOutput, ScriptProcessRef, ScriptStartCommand, ScriptStarted,
    MAX_SCRIPT_OUTPUT_BYTES,
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

/// Longest a stop waits for the processes it kills to be reaped: the kill's
/// SIGTERM grace, the reaper's own group escalation, its output-EOF wait and
/// a margin. A process past it (a child stuck in the kernel) is logged and
/// left; the caller carries on.
pub(super) fn reap_wait() -> std::time::Duration {
    let bound = intent_pty::TERM_GRACE * 2 + OUTPUT_EOF_WAIT + REAP_MARGIN;
    #[cfg(test)]
    let bound = intentd_test_support::test_timeout(bound);
    bound
}

/// Agents whose script reapers never finish (test seam for a process whose
/// `wait` never returns).
#[cfg(test)]
static HELD_REAPS: std::sync::LazyLock<Mutex<std::collections::HashSet<AgentId>>> =
    std::sync::LazyLock::new(Mutex::default);

/// Make every reaper of `agent_id`'s scripts hang (test seam).
#[cfg(test)]
pub(super) fn hold_reaps_for_test(agent_id: &AgentId) {
    lock(&HELD_REAPS).insert(agent_id.clone());
}

/// A process is named by its agent, script and spawn.
type Key = (AgentId, String, String);

struct Process {
    pty_id: PtyId,
    generation: NodeCounter,
    /// Turns true once its reaper has seen the process exit and recorded
    /// it, however the kill was started (`scriptStop`, the binding's end or
    /// the agent's stop). A closed channel counts as exited.
    reaped: tokio::sync::watch::Receiver<bool>,
}

/// The script processes of one backend's agents on this machine.
#[derive(Clone)]
pub(super) struct ScriptProcesses {
    pty: Arc<PtyHost>,
    running: Arc<Mutex<HashMap<Key, Process>>>,
    /// Kills under way, each until its whole process-group teardown is
    /// done (`true`). A later stop joins one instead of starting a kill
    /// that finds the terminal already taken and returns at once.
    kills: Arc<Mutex<HashMap<Key, Kill>>>,
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

fn key(agent_id: &AgentId, script_id: &str, spawn_id: &str) -> Key {
    (agent_id.clone(), script_id.to_owned(), spawn_id.to_owned())
}

fn exited(key: &Key, exit_code: Option<i32>, error: Option<String>) -> LinkFrame {
    LinkFrame::ScriptExited(ScriptExited {
        agent_id: key.0.clone(),
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
        }
    }

    /// Start one process for `command` in `checkout` (the agent's checkout,
    /// `None` when the agent is unknown or its checkout is gone), started by
    /// the link binding `generation`. Every outcome is reported on `link`.
    pub(super) async fn start(
        &self,
        link: &NodeLink,
        generation: NodeCounter,
        checkout: Option<&Path>,
        command: ScriptStartCommand,
    ) {
        let key = key(&command.agent_id, &command.script_id, &command.spawn_id);
        let refuse =
            |code: &str, detail: &str| exited(&key, None, Some(format!("{code}: {detail}")));
        let Some(checkout) = checkout else {
            let _ = link
                .send(&refuse(
                    AGENT_UNKNOWN,
                    "the agent has no checkout on this machine",
                ))
                .await;
            return;
        };
        let Some(cwd) = resolve_cwd(checkout, command.cwd.as_deref()) else {
            let _ = link
                .send(&refuse(
                    INVALID_CWD,
                    "cwd must stay inside the agent's checkout",
                ))
                .await;
            return;
        };
        if lock(&self.running).contains_key(&key) {
            tracing::warn!(agent = %key.0, "node: duplicate scriptStart for a running process; ignored");
            return;
        }
        let shell = default_shell();
        let mut spec = SpawnSpec::new(format!("remote-script:{}", key.0), shell.clone());
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
        let _ = link
            .send(&LinkFrame::ScriptStarted(ScriptStarted {
                agent_id: key.0.clone(),
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
        let key = key(&process.agent_id, &process.script_id, &process.spawn_id);
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
        let all = self.kills.clone();
        let key = key.clone();
        // caller-binding: allow — node-local script teardown; every stop of the process joins it
        tokio::spawn(async move {
            pty.kill(pty_id).await;
            {
                let mut kills = lock(&all);
                if kills
                    .get(&key)
                    .is_some_and(|kill| kill.done.same_channel(&finished.subscribe()))
                {
                    kills.remove(&key);
                }
            }
            finished.send_replace(true);
        });
        done
    }

    /// Kill every process started on the link binding `generation`.
    pub(super) async fn stop_binding(&self, generation: NodeCounter) {
        self.kill_where(|_, g| g == generation).await;
    }

    /// Whether `process` is still recorded as running (until its reaper
    /// has recorded its exit).
    #[cfg(test)]
    pub(super) fn is_running(&self, process: &ScriptProcessRef) -> bool {
        lock(&self.running).contains_key(&key(
            &process.agent_id,
            &process.script_id,
            &process.spawn_id,
        ))
    }

    /// Kill every process of `agent_id`. Returns whether each was reaped
    /// within [`reap_wait`].
    pub(super) async fn stop_agent(&self, agent_id: &AgentId) -> bool {
        self.kill_where(|key, _| &key.0 == agent_id).await
    }

    /// Kill every process (the machine is shutting down).
    pub(super) async fn stop_all(&self) {
        self.kill_where(|_, _| true).await;
    }

    /// Kill every matching process (joining a kill already under way), then
    /// wait until each kill's whole process-group teardown is done and each
    /// process's reaper has recorded its exit. A kill started elsewhere
    /// (`scriptStop`) of a process already reaped is waited for too. The
    /// wait is bounded by [`reap_wait`]: past it, what is still not done is
    /// logged and left. Returns whether everything finished.
    async fn kill_where(&self, matches: impl Fn(&Key, NodeCounter) -> bool) -> bool {
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
        let done = tokio::time::timeout(reap_wait(), async {
            for mut wait in waits {
                let _ = wait.wait_for(|done| *done).await;
            }
        })
        .await
        .is_ok();
        if !done {
            tracing::warn!("node: a script process was not reaped within its bound; leaving it");
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
            agent_id: self.key.0.clone(),
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
fn take_utf8(buf: &mut Vec<u8>) -> String {
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
fn split_utf8(text: &str, max: usize) -> Vec<&str> {
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
