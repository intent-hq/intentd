//! Terminals on a workspace home (nodeProtocol 19, node-link.md § Terminals
//! on the home). Each `ptyOpen` is one process in the home checkout: a
//! shell on a PTY, or (`exec`) a command on pipes. Its output goes back as
//! `ptyOutput`, numbered from 0, and stays here until the backend
//! acknowledges it with `ptyAck`: it is resent after the next bind, and
//! past [`PTY_UNACKED_BYTES`] the terminal's reader waits, so the program
//! blocks instead of losing output. A lost link leaves the processes
//! running for [`LINK_LOSS_GRACE`]; a bind from a new backend session ends
//! them at once.

use std::collections::{HashMap, VecDeque};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use base64::Engine as _;
use intent_core::node_link::{
    LinkFrame, PtyAck, PtyExited, PtyInput, PtyKill, PtyMode, PtyOpen, PtyOutput, PtyResize,
    PtyStream, MAX_PTY_OUTPUT_BYTES, NODE_HOME_UNKNOWN, PTY_UNACKED_BYTES,
};
use intent_core::nodes::NodeCounter;
use intent_pty::{OutputSink, PtyHost, PtyId, PtySize, SpawnSpec};
use intent_services::feature_api::shell::{default_shell, scrubbed_env_vars_except, shell_args};
use intent_transport::node_link::NodeLink;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// How long terminals outlive a lost link, waiting for the next bind.
pub(super) const LINK_LOSS_GRACE: Duration = Duration::from_secs(60);

/// `ptyExited.error` codes besides [`NODE_HOME_UNKNOWN`].
const START_FAILED: &str = "node-pty-start-failed";
const INVALID_CWD: &str = "node-pty-invalid-cwd";

/// In-memory scrollback of a home terminal's PTY on the machine. The
/// backend keeps the terminal's buffer; this only backs the PTY.
const SCROLLBACK_BYTES: usize = 64 * 1024;

/// How long an exit waits for the last output to be read.
const OUTPUT_EOF_WAIT: Duration = Duration::from_secs(2);

const TERM: &str = "xterm-256color";

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The home terminals of the bound backend.
#[derive(Clone)]
pub(super) struct HomeTerminals {
    shared: Arc<Shared>,
}

struct Shared {
    pty: Arc<PtyHost>,
    grace: Duration,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    terms: HashMap<String, Arc<Term>>,
    link: Option<NodeLink>,
    generation: Option<NodeCounter>,
    session: Option<String>,
    /// Ends every terminal when the grace after a lost link runs out.
    grace: Option<tokio::task::JoinHandle<()>>,
    closed: bool,
}

struct Term {
    id: String,
    out: Mutex<Outbox>,
    /// The PTY reader waits here while the unacknowledged output is full.
    room: Condvar,
    /// An exec reader waits here while the unacknowledged output is full.
    room_async: tokio::sync::Notify,
    /// Wakes the terminal's sender: new output, an ack, a bind or the exit.
    wake: tokio::sync::Notify,
    process: Mutex<Process>,
}

#[derive(Default)]
struct Outbox {
    next_seq: u64,
    unacked: VecDeque<PtyOutput>,
    unacked_bytes: usize,
    /// The next `seq` to send on the current link.
    cursor: u64,
    /// Bumped by each bind: a send on an older link moves no cursor.
    epoch: u64,
    exited: Option<PtyExited>,
    exited_sent: bool,
    /// The process ended or was killed: readers stop waiting for room.
    ended: bool,
}

enum Process {
    Pty(PtyId),
    Exec {
        stdin: Option<tokio::process::ChildStdin>,
        kill: Option<tokio::sync::oneshot::Sender<()>>,
    },
    Gone,
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

/// Login arguments for a default interactive shell, as a local terminal.
fn login_args(shell: &str) -> Vec<String> {
    if cfg!(windows) {
        return Vec::new();
    }
    let base = Path::new(shell)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(shell)
        .to_ascii_lowercase();
    if matches!(base.as_str(), "zsh" | "bash") {
        vec!["-l".to_owned()]
    } else {
        Vec::new()
    }
}

impl Term {
    fn new(id: String, process: Process) -> Arc<Self> {
        Arc::new(Self {
            id,
            out: Mutex::default(),
            room: Condvar::new(),
            room_async: tokio::sync::Notify::new(),
            wake: tokio::sync::Notify::new(),
            process: Mutex::new(process),
        })
    }

    /// Queue `bytes` as numbered frames. Returns whether the unacknowledged
    /// output is now full.
    fn push(&self, bytes: &[u8], stream: Option<PtyStream>) -> bool {
        let mut out = lock(&self.out);
        for chunk in bytes.chunks(MAX_PTY_OUTPUT_BYTES) {
            let seq = out.next_seq;
            out.next_seq += 1;
            out.unacked_bytes += chunk.len();
            out.unacked.push_back(PtyOutput {
                terminal_id: self.id.clone(),
                seq,
                data: base64::engine::general_purpose::STANDARD.encode(chunk),
                stream,
            });
        }
        let full = out.unacked_bytes >= PTY_UNACKED_BYTES && !out.ended;
        drop(out);
        self.wake.notify_one();
        full
    }

    /// Block the PTY reader thread until there is room again.
    fn wait_room_blocking(&self) {
        let out = lock(&self.out);
        let _out = self
            .room
            .wait_while(out, |out| {
                out.unacked_bytes >= PTY_UNACKED_BYTES && !out.ended
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }

    async fn wait_room(&self) {
        loop {
            let notified = self.room_async.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let out = lock(&self.out);
                if out.unacked_bytes < PTY_UNACKED_BYTES || out.ended {
                    return;
                }
            }
            notified.await;
        }
    }

    fn release_room(&self) {
        self.room.notify_all();
        self.room_async.notify_waiters();
    }

    fn ack(&self, seq: u64) {
        let mut out = lock(&self.out);
        while out.unacked.front().is_some_and(|frame| frame.seq <= seq) {
            if let Some(frame) = out.unacked.pop_front() {
                out.unacked_bytes = out.unacked_bytes.saturating_sub(decoded_len(&frame.data));
            }
        }
        drop(out);
        self.release_room();
        self.wake.notify_one();
    }

    fn finish(&self, exited: PtyExited) {
        let mut out = lock(&self.out);
        out.ended = true;
        if out.exited.is_none() {
            out.exited = Some(exited);
        }
        drop(out);
        *lock(&self.process) = Process::Gone;
        self.release_room();
        self.wake.notify_one();
    }

    /// Resend everything unacknowledged on the next link.
    fn rewind(&self) {
        let mut out = lock(&self.out);
        out.epoch += 1;
        out.cursor = out.unacked.front().map_or(out.next_seq, |frame| frame.seq);
        out.exited_sent = false;
        drop(out);
        self.wake.notify_one();
    }
}

fn decoded_len(data: &str) -> usize {
    data.len() / 4 * 3 - data.bytes().rev().take_while(|byte| *byte == b'=').count()
}

impl HomeTerminals {
    pub(super) fn new(pty: Arc<PtyHost>) -> Self {
        Self::with_grace(pty, LINK_LOSS_GRACE)
    }

    pub(super) fn with_grace(pty: Arc<PtyHost>, grace: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                pty,
                grace,
                state: Mutex::default(),
            }),
        }
    }

    fn term(&self, terminal_id: &str) -> Option<Arc<Term>> {
        lock(&self.shared.state).terms.get(terminal_id).cloned()
    }

    /// A link bound with `head_session`: a new backend session ends every
    /// terminal of the previous one; the same session gets the
    /// unacknowledged output again.
    pub(super) fn bound(
        &self,
        link: NodeLink,
        generation: NodeCounter,
        head_session: Option<&str>,
    ) {
        let mut state = lock(&self.shared.state);
        if let Some(grace) = state.grace.take() {
            grace.abort();
        }
        let restarted = state.session.as_deref() != head_session || head_session.is_none();
        state.session = head_session.map(str::to_owned);
        state.link = Some(link);
        state.generation = Some(generation);
        let ended: Vec<Arc<Term>> = if restarted {
            state.terms.drain().map(|(_, term)| term).collect()
        } else {
            Vec::new()
        };
        let kept: Vec<Arc<Term>> = state.terms.values().cloned().collect();
        drop(state);
        for term in kept {
            term.rewind();
        }
        for term in ended {
            self.end(&term);
        }
    }

    /// The link bound as `generation` ended: unless a newer one bound, the
    /// terminals end once the grace runs out.
    pub(super) fn link_ended(&self, generation: NodeCounter) {
        let mut state = lock(&self.shared.state);
        if state.generation != Some(generation) || state.closed {
            return;
        }
        state.link = None;
        if let Some(grace) = state.grace.take() {
            grace.abort();
        }
        let this = self.clone();
        let wait = self.shared.grace;
        // caller-binding: allow — node-local terminal grace after a lost link
        state.grace = Some(tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let ended: Vec<Arc<Term>> = {
                let mut state = lock(&this.shared.state);
                if state.link.is_some() {
                    return;
                }
                state.grace = None;
                state.terms.drain().map(|(_, term)| term).collect()
            };
            for term in ended {
                this.end(&term);
            }
        }));
    }

    /// End every terminal (the machine is shutting down).
    pub(super) fn close(&self) {
        let ended: Vec<Arc<Term>> = {
            let mut state = lock(&self.shared.state);
            state.closed = true;
            if let Some(grace) = state.grace.take() {
                grace.abort();
            }
            state.terms.drain().map(|(_, term)| term).collect()
        };
        for term in ended {
            self.end(&term);
        }
    }

    /// Kill a terminal's process; nothing more is sent for it.
    fn end(&self, term: &Arc<Term>) {
        let process = std::mem::replace(&mut *lock(&term.process), Process::Gone);
        {
            let mut out = lock(&term.out);
            out.ended = true;
            out.exited_sent = true;
            out.unacked.clear();
            out.unacked_bytes = 0;
        }
        term.release_room();
        term.wake.notify_one();
        match process {
            Process::Pty(pty_id) => {
                let pty = self.shared.pty.clone();
                // caller-binding: allow — node-local terminal teardown
                tokio::spawn(async move {
                    pty.kill(pty_id).await;
                });
            }
            Process::Exec { kill, .. } => {
                if let Some(kill) = kill {
                    let _ = kill.send(());
                }
            }
            Process::Gone => {}
        }
    }

    /// Start `frame`'s process in `checkout`, the home checkout (`None`
    /// when this machine does not hold the home). A refused start answers
    /// `ptyExited` with `error`.
    pub(super) fn open(&self, frame: &PtyOpen, checkout: Option<&Path>) {
        let mut state = lock(&self.shared.state);
        if state.closed || state.terms.contains_key(&frame.terminal_id) {
            return;
        }
        let refused = |code: &str, detail: String| {
            let term = Term::new(frame.terminal_id.clone(), Process::Gone);
            term.finish(PtyExited {
                terminal_id: frame.terminal_id.clone(),
                exit_code: None,
                error: Some(format!("{code}: {detail}")),
            });
            term
        };
        let term = match checkout {
            None => refused(
                NODE_HOME_UNKNOWN,
                format!("this machine holds no home {}", frame.home_id),
            ),
            Some(checkout) => match resolve_cwd(checkout, frame.cwd.as_deref()) {
                None => refused(
                    INVALID_CWD,
                    "cwd must stay inside the home checkout".to_owned(),
                ),
                Some(cwd) => {
                    let started = match frame.mode {
                        PtyMode::Terminal => self.start_pty(frame, cwd),
                        PtyMode::Exec => Self::start_exec(frame, &cwd),
                    };
                    match started {
                        Ok(term) => term,
                        Err(error) => refused(START_FAILED, error),
                    }
                }
            },
        };
        state.terms.insert(frame.terminal_id.clone(), term.clone());
        drop(state);
        let this = self.clone();
        // caller-binding: allow — node-local sender of one home terminal's frames
        tokio::spawn(async move { this.send_loop(term).await });
    }

    fn env(frame: &PtyOpen) -> Vec<(String, String)> {
        let mut env: Vec<(String, String)> = frame
            .env
            .iter()
            .map(|var| (var.key.clone(), var.value.clone()))
            .collect();
        if frame.mode == PtyMode::Terminal
            && !env
                .iter()
                .any(|(key, value)| key == "TERM" && !value.is_empty())
        {
            env.retain(|(key, _)| key != "TERM");
            env.push(("TERM".to_owned(), TERM.to_owned()));
        }
        env
    }

    fn start_pty(&self, frame: &PtyOpen, cwd: PathBuf) -> Result<Arc<Term>, String> {
        let shell = default_shell();
        let args = frame
            .command
            .as_deref()
            .filter(|command| !command.is_empty())
            .map_or_else(|| login_args(&shell), |command| shell_args(&shell, command));
        let command = shell;
        let mut spec = SpawnSpec::new(format!("home-terminal:{}", frame.home_id), command);
        spec.args = args;
        spec.env = Self::env(frame);
        spec.env_remove = scrubbed_env_vars_except(&spec.env);
        spec.cwd = Some(cwd);
        spec.listed = false;
        spec.scrollback_bytes = SCROLLBACK_BYTES;
        if let (Some(cols), Some(rows)) = (frame.cols, frame.rows) {
            spec.size = PtySize { rows, cols };
        }
        let term = Term::new(frame.terminal_id.clone(), Process::Gone);
        let sink_term = Arc::downgrade(&term);
        spec.output_sink = Some(OutputSink::new(move |bytes| {
            if let Some(term) = sink_term.upgrade() {
                if term.push(bytes, None) {
                    term.wait_room_blocking();
                }
            }
        }));
        let pty_id = self.shared.pty.spawn(spec).map_err(|e| e.to_string())?;
        *lock(&term.process) = Process::Pty(pty_id);
        let pty = self.shared.pty.clone();
        let reaped = term.clone();
        // caller-binding: allow — node-local home terminal process supervision
        tokio::spawn(async move {
            let exit = pty.wait(pty_id).await;
            pty.reap_group_stragglers(pty_id).await;
            let _ = tokio::time::timeout(OUTPUT_EOF_WAIT, pty.wait_output_eof(pty_id)).await;
            pty.kill(pty_id).await;
            let exit_code = match exit {
                Ok(exit) if exit.observed => i32::try_from(exit.exit_code).unwrap_or(-1),
                _ => -1,
            };
            reaped.finish(PtyExited {
                terminal_id: reaped.id.clone(),
                exit_code: Some(exit_code),
                error: None,
            });
        });
        Ok(term)
    }

    fn start_exec(frame: &PtyOpen, cwd: &Path) -> Result<Arc<Term>, String> {
        let shell = default_shell();
        let command = frame.command.as_deref().unwrap_or_default();
        let mut cmd = tokio::process::Command::new(&shell);
        cmd.args(shell_args(&shell, command))
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let env = Self::env(frame);
        for key in scrubbed_env_vars_except(&env) {
            cmd.env_remove(key);
        }
        cmd.envs(env);
        #[cfg(unix)]
        cmd.process_group(0);
        let mut child = cmd.spawn().map_err(|e| e.to_string())?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let (kill, killed) = tokio::sync::oneshot::channel();
        let term = Term::new(
            frame.terminal_id.clone(),
            Process::Exec {
                stdin,
                kill: Some(kill),
            },
        );
        let readers = [
            stdout.map(|pipe| read_pipe(term.clone(), pipe, PtyStream::Stdout)),
            stderr.map(|pipe| read_pipe(term.clone(), pipe, PtyStream::Stderr)),
        ];
        let reaped = term.clone();
        // caller-binding: allow — node-local home exec process supervision
        tokio::spawn(async move {
            let status = tokio::select! {
                status = child.wait() => status.ok(),
                _ = killed => {
                    kill_child(&mut child).await;
                    child.wait().await.ok()
                }
            };
            for reader in readers.into_iter().flatten() {
                let _ = tokio::time::timeout(OUTPUT_EOF_WAIT, reader).await;
            }
            reaped.finish(PtyExited {
                terminal_id: reaped.id.clone(),
                exit_code: Some(status.and_then(|s| s.code()).unwrap_or(-1)),
                error: None,
            });
        });
        Ok(term)
    }

    /// Send the terminal's frames on the current link, in order; resend
    /// after each bind; drop the terminal once its exit is sent and every
    /// output frame is acknowledged.
    async fn send_loop(self, term: Arc<Term>) {
        loop {
            let notified = term.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let link = lock(&self.shared.state).link.clone();
            if let Some(link) = link {
                let (epoch, frames, exited) = {
                    let out = lock(&term.out);
                    let frames: Vec<PtyOutput> = out
                        .unacked
                        .iter()
                        .filter(|frame| frame.seq >= out.cursor)
                        .cloned()
                        .collect();
                    let exited = (!out.exited_sent).then(|| out.exited.clone()).flatten();
                    (out.epoch, frames, exited)
                };
                let mut sent_all = true;
                for frame in frames {
                    let seq = frame.seq;
                    if link.send(&LinkFrame::PtyOutput(frame)).await.is_err() {
                        sent_all = false;
                        break;
                    }
                    let mut out = lock(&term.out);
                    if out.epoch == epoch && out.cursor <= seq {
                        out.cursor = seq + 1;
                    }
                }
                if sent_all {
                    if let Some(exited) = exited {
                        let caught_up = {
                            let out = lock(&term.out);
                            out.epoch == epoch && out.cursor >= out.next_seq
                        };
                        if caught_up && link.send(&LinkFrame::PtyExited(exited)).await.is_ok() {
                            let mut out = lock(&term.out);
                            if out.epoch == epoch {
                                out.exited_sent = true;
                            }
                        }
                    }
                }
            }
            let done = {
                let out = lock(&term.out);
                out.exited_sent && out.unacked.is_empty()
            };
            if done {
                let mut state = lock(&self.shared.state);
                if state
                    .terms
                    .get(&term.id)
                    .is_some_and(|current| Arc::ptr_eq(current, &term))
                {
                    state.terms.remove(&term.id);
                }
                return;
            }
            notified.await;
        }
    }

    pub(super) fn input(&self, frame: &PtyInput) {
        let Some(term) = self.term(&frame.terminal_id) else {
            return;
        };
        let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(&frame.data) else {
            tracing::debug!(terminal = %frame.terminal_id, "node: undecodable ptyInput dropped");
            return;
        };
        let mut process = lock(&term.process);
        match &mut *process {
            Process::Pty(pty_id) => {
                let _ = self.shared.pty.write(*pty_id, &bytes);
            }
            Process::Exec { stdin, .. } => {
                if let Some(mut pipe) = stdin.take() {
                    let term = term.clone();
                    drop(process);
                    // caller-binding: allow — node-local write to one exec process's stdin
                    tokio::spawn(async move {
                        let ok = pipe.write_all(&bytes).await.is_ok();
                        if ok {
                            if let Process::Exec { stdin, .. } = &mut *lock(&term.process) {
                                *stdin = Some(pipe);
                            }
                        }
                    });
                }
            }
            Process::Gone => {}
        }
    }

    pub(super) fn resize(&self, frame: &PtyResize) {
        let Some(term) = self.term(&frame.terminal_id) else {
            return;
        };
        let pty_id = match &*lock(&term.process) {
            Process::Pty(pty_id) => Some(*pty_id),
            _ => None,
        };
        if let Some(pty_id) = pty_id {
            let _ = self.shared.pty.resize(
                pty_id,
                PtySize {
                    rows: frame.rows,
                    cols: frame.cols,
                },
            );
        }
    }

    /// End a terminal's process; its `ptyExited` follows.
    pub(super) fn kill(&self, frame: &PtyKill) {
        let Some(term) = self.term(&frame.terminal_id) else {
            return;
        };
        let mut process = lock(&term.process);
        match &mut *process {
            Process::Pty(pty_id) => {
                let pty_id = *pty_id;
                drop(process);
                let pty = self.shared.pty.clone();
                // caller-binding: allow — node-local home terminal kill; its reaper reports the exit
                tokio::spawn(async move {
                    pty.kill(pty_id).await;
                });
            }
            Process::Exec { kill, .. } => {
                if let Some(kill) = kill.take() {
                    let _ = kill.send(());
                }
            }
            Process::Gone => {}
        }
    }

    pub(super) fn ack(&self, frame: &PtyAck) {
        if let Some(term) = self.term(&frame.terminal_id) {
            term.ack(frame.seq);
        }
    }
}

fn read_pipe(
    term: Arc<Term>,
    mut pipe: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    stream: PtyStream,
) -> tokio::task::JoinHandle<()> {
    // caller-binding: allow — node-local reader of one exec process pipe
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_PTY_OUTPUT_BYTES];
        loop {
            let read = match pipe.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(read) => read,
            };
            if term.push(&buf[..read], Some(stream)) {
                term.wait_room().await;
            }
        }
    })
}

async fn kill_child(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;
        let group = Pid::from_raw(pid.cast_signed());
        let _ = killpg(group, Signal::SIGTERM);
        if tokio::time::timeout(intent_pty::TERM_GRACE, child.wait())
            .await
            .is_ok()
        {
            return;
        }
        let _ = killpg(group, Signal::SIGKILL);
        return;
    }
    let _ = child.start_kill();
}
