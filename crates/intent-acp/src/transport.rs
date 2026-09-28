//! NDJSON JSON-RPC transport over piped stdio (§6.3).
//!
//! One writer task owns the child's stdin and is fed whole lines through an
//! `mpsc` channel, guaranteeing per-message atomicity (no interleaving even for
//! messages larger than `PIPE_BUF`). A reader task frames stdout on `\n`, parses
//! each line as JSON-RPC, and dispatches: responses → the pending `oneshot` map
//! (keyed per id), agent→client requests → a client-served handler hook, and
//! notifications → a streaming-router hook. A stderr task drains the child's
//! stderr into a bounded ring buffer, flags configured auth-error patterns,
//! and — when a capture dir is configured — forwards every line through a
//! bounded channel to a dedicated writer task that appends it to a
//! daily-rotated per-agent log file (STAB-53).

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, watch, Notify};
use tokio::task::JoinHandle;

use crate::callback_registration::{CallbackFailure, CallbackToolRoutes};
use crate::error::{AcpError, AcpResult, JsonRpcError};

/// Default per-request timeout (§6.4). `initialize` uses its own, more
/// generous timeout — see `handshake::initialize_timeout` (monorepo#616).
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of recent stderr entries retained (parity:
/// `MAX_RECENT_STDERR_ERRORS`).
const MAX_RECENT_STDERR_ERRORS: usize = 5;
/// Maximum characters retained per stderr entry (parity:
/// `MAX_RECENT_STDERR_ENTRY_CHARS`).
const MAX_RECENT_STDERR_ENTRY_CHARS: usize = 10_000;
/// Outbound writer channel capacity.
const WRITER_CHANNEL_CAPACITY: usize = 256;

type PendingMap = Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, JsonRpcError>>>>>;

/// Optional prompt admission gets one second AFTER the original writer slot is
/// reserved. The ordinary response timeout still begins only after queueing.
pub const PROMPT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(1);

const PROMPT_OPEN: u8 = 0;
const PROMPT_QUEUED: u8 = 1;
const PROMPT_RETIRED: u8 = 2;

/// Allocation identity for one original prompt transfer, never an RPC/session ID.
pub struct AcpPromptBoundary(Arc<()>);

/// Created only by consuming the original queue slot. It proves queue ownership
/// transfer, not a socket write, model receipt, or reusable approval.
pub struct AcpPromptReceipt(Arc<()>);

/// Choose one of the two already encoded forms of the SAME session prompt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptVariant {
    Base,
    WithGuidance,
}

/// Optional refusal does not authorize replacement of an already queued effect.
pub enum AcpPromptAdmissionOutcome {
    Transferred(AcpPromptReceipt),
    ConsumerClosed,
    ForeignBoundary,
    OmitOptional,
}

/// A trusted owner of the original prompt capture. This neutral transport does
/// not manufacture that capture or validate repository/provider authority.
pub trait AcpPromptAdmission: Send + Sync {
    /// Validate the original capture, then consume the borrowed packet while
    /// retaining the required guards. Release guards before returning. Never
    /// detach, retry, rebind, or reconstruct authority from public identifiers.
    fn admit<'a>(
        &'a self,
        original: &'a AcpPromptBoundary,
        packet: PreparedAcpPromptTransfer<'a>,
    ) -> intent_core::BoxFuture<'a, AcpPromptAdmissionOutcome>;
}

struct PromptSlot {
    permit: Option<mpsc::OwnedPermit<String>>,
    base: Option<String>,
    enriched: Option<String>,
    queued: bool,
}

/// No public constructor, payload accessor, Clone, or reusable approval. All
/// opaque owners, response state and unchosen bytes remain outside the action.
#[must_use]
pub struct PreparedAcpPromptTransfer<'a> {
    original: &'a AcpPromptBoundary,
    state: &'a AtomicU8,
    writer: &'a mpsc::Sender<String>,
    slot: &'a mut PromptSlot,
}

impl PreparedAcpPromptTransfer<'_> {
    /// Atomically claim the still-original pending transfer and move one
    /// pre-encoded String into its reserved slot. No lock, serialization, queue
    /// reservation, await, I/O, spawn, or authority callback occurs here.
    ///
    /// # Panics
    ///
    /// Panics only if the private packet's encoded-variant invariant is broken.
    #[must_use]
    pub fn transfer(
        self,
        original: &AcpPromptBoundary,
        variant: PromptVariant,
    ) -> AcpPromptAdmissionOutcome {
        if !Arc::ptr_eq(&self.original.0, &original.0) {
            return AcpPromptAdmissionOutcome::ForeignBoundary;
        }
        if self.writer.is_closed()
            || self.slot.permit.is_none()
            || self
                .state
                .compare_exchange(
                    PROMPT_OPEN,
                    PROMPT_QUEUED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
        {
            return AcpPromptAdmissionOutcome::ConsumerClosed;
        }
        // This claim linearizes against original pending retirement. Once it
        // wins, even a later auth/reader failure cannot erase the queue effect.
        let line = match variant {
            PromptVariant::Base => self.slot.base.take(),
            PromptVariant::WithGuidance => self.slot.enriched.take(),
        };
        let permit = self
            .slot
            .permit
            .take()
            .expect("original unused prompt slot");
        self.slot.queued = true;
        permit.send(line.expect("original encoded prompt variant"));
        AcpPromptAdmissionOutcome::Transferred(AcpPromptReceipt(Arc::clone(&original.0)))
    }
}

/// Only qualified prompts register here. Ordinary pending-map behavior stays
/// unchanged. Reader retirement precedes removal/draining of the original sender.
#[derive(Default)]
struct PromptPending {
    closed: bool,
    entries: HashMap<i64, Arc<AtomicU8>>,
}

impl PromptPending {
    fn insert(&mut self, id: i64) -> Arc<AtomicU8> {
        let state = Arc::new(AtomicU8::new(if self.closed {
            PROMPT_RETIRED
        } else {
            PROMPT_OPEN
        }));
        self.entries.insert(id, Arc::clone(&state));
        state
    }

    fn retire(&mut self, id: i64) {
        if let Some(state) = self.entries.remove(&id) {
            Self::retire_state(&state);
        }
    }

    fn retire_state(state: &AtomicU8) {
        let _ = state.compare_exchange(
            PROMPT_OPEN,
            PROMPT_RETIRED,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn close(&mut self) {
        self.closed = true;
        for (_, state) in self.entries.drain() {
            Self::retire_state(&state);
        }
    }
}

struct PromptPendingGuard<'a> {
    original: PendingEntryGuard,
    prompts: &'a Mutex<PromptPending>,
    state: Arc<AtomicU8>,
}

impl Drop for PromptPendingGuard<'_> {
    fn drop(&mut self) {
        PromptPending::retire_state(&self.state);
        self.prompts.lock().unwrap().retire(self.original.id);
    }
}

struct CaughtPromptAdmission<'a> {
    future: Option<intent_core::BoxFuture<'a, AcpPromptAdmissionOutcome>>,
    state: &'a AtomicU8,
}

impl Future for CaughtPromptAdmission<'_> {
    type Output = Option<AcpPromptAdmissionOutcome>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match catch_unwind(AssertUnwindSafe(|| {
            self.future
                .as_mut()
                .expect("live admission future")
                .as_mut()
                .poll(cx)
        })) {
            Ok(Poll::Ready(result)) => Poll::Ready(Some(result)),
            // A policy yielding after transfer cannot postpone the ordinary
            // response wait, spend a second admission budget, or cause replay.
            Ok(Poll::Pending) if self.state.load(Ordering::SeqCst) == PROMPT_QUEUED => {
                Poll::Ready(None)
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(None),
        }
    }
}

impl Drop for CaughtPromptAdmission<'_> {
    fn drop(&mut self) {
        // Cleanup stays outside the consuming action, including cancellation
        // before the policy future's first poll.
        let future = self.future.take();
        let _ = catch_unwind(AssertUnwindSafe(|| drop(future)));
    }
}

fn authentication_required() -> JsonRpcError {
    JsonRpcError {
        code: -32000,
        message: "Authentication required; run intentd provider login antigravity".into(),
        data: None,
    }
}

fn prompt_response(
    response: Result<Result<Value, JsonRpcError>, oneshot::error::RecvError>,
) -> AcpResult<Value> {
    match response {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(AcpError::Rpc(error)),
        Err(_) => Err(AcpError::Transport("response channel dropped".into())),
    }
}

/// Removes a request's pending-map entry when the request future completes or
/// is dropped, making [`Connection::request_timeout`] cancel-safe with respect
/// to the correlation map: a caller that abandons the future mid-flight (e.g.
/// `session::prompt`'s idle-timeout early return) no longer leaks the entry
/// until the agent closes stdout. Removal after the reader task already
/// dispatched the response is a harmless no-op.
struct PendingEntryGuard {
    pending: PendingMap,
    id: i64,
}

impl Drop for PendingEntryGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

pub(crate) enum CallbackRequestOutcome {
    Response(Value),
    NotSent(CallbackFailure),
    Unknown(CallbackFailure),
}

// Unlike ordinary pending cleanup, an abandoned callback request also attempts
// peer cancellation. No await, task, or retry is allowed in this guard.
struct CallbackPendingGuard<'a> {
    connection: &'a Connection,
    id: i64,
    queued: bool,
    completed: bool,
}

impl Drop for CallbackPendingGuard<'_> {
    fn drop(&mut self) {
        self.connection.pending.lock().unwrap().remove(&self.id);
        if self.queued && !self.completed {
            let _ = self.connection.try_notify(
                "$/cancel_request",
                &serde_json::json!({"requestId":self.id}),
            );
        }
    }
}

/// An agent→client request that must be served by a client-side handler
/// (`fs/*`, `terminal/*`, `session/request_permission`). For M3.3 this is a
/// plumbing hook; the handlers themselves land in M3.5.
#[derive(Debug, Clone)]
pub struct IncomingRequest {
    /// The JSON-RPC id to respond against.
    pub id: Value,
    /// The request method name.
    pub method: String,
    /// The request params (`Null` when absent).
    pub params: Value,
}

/// A notification from the agent (`session/update`, …). For M3.3 this is a
/// plumbing hook; the streaming router lands in M3.4.
#[derive(Debug, Clone)]
pub struct IncomingNotification {
    /// The notification method name.
    pub method: String,
    /// The notification params (`Null` when absent).
    pub params: Value,
}

/// Hooks the reader forwards inbound traffic to, plus auth-error patterns the
/// stderr drain matches against.
#[derive(Default)]
pub struct ConnectionHooks {
    /// Exact out-of-band stdout signal from a configured browser guard.
    /// Disabled by default. A match fails requests without exposing a URL.
    pub auth_required_stdout_marker: Option<&'static str>,
    /// Sink for agent→client requests (client-served handlers).
    pub requests: Option<mpsc::UnboundedSender<IncomingRequest>>,
    /// Sink for agent notifications (streaming router).
    pub notifications: Option<mpsc::UnboundedSender<IncomingNotification>>,
    /// Case-insensitive substrings that mark an auth failure on stderr.
    pub auth_error_patterns: Vec<String>,
    /// When set, every stderr line is also appended to
    /// `<dir>/<YYYY-MM-DD>.log` (the per-agent capture dir, STAB-53). Writes
    /// are best-effort in a dedicated writer task behind a bounded channel:
    /// lines are dropped when the writer stalls or fails, so capture never
    /// backpressures the stderr drain or the agent runtime.
    pub stderr_log_dir: Option<PathBuf>,
    /// Diagnostics-only owner of this connection: the agent id the reader
    /// attributes an unparseable stdout line to. `None` for connections with
    /// no agent (e.g. ephemeral adapter runs); never affects behavior.
    pub agent_id: Option<String>,
}

/// Lines buffered between the stderr drain and the log writer task before
/// drop-on-full kicks in (STAB-53).
const STDERR_LOG_CHANNEL_CAPACITY: usize = 256;

/// Best-effort daily-rotated file sink for the stderr capture (STAB-53).
///
/// A bounded channel decouples the stderr drain loop from file I/O: the drain
/// side `try_send`s lines and drops them when the channel is full or the
/// writer has exited, so a stalled disk can never backpressure the child's
/// stderr pipe. A dedicated writer task owns the file handle — it opens
/// `<dir>/<YYYY-MM-DD>.log` lazily in append mode, reopens when the (UTC)
/// date rolls over, and exits on the first write failure so a bad disk never
/// loops warnings per line.
struct StderrLogSink {
    tx: mpsc::Sender<String>,
    writer: JoinHandle<()>,
    drop_warned: bool,
}

impl StderrLogSink {
    fn new(dir: PathBuf) -> Self {
        let (tx, rx) = mpsc::channel::<String>(STDERR_LOG_CHANNEL_CAPACITY);
        let writer = tokio::spawn(stderr_log_writer(dir, rx));
        Self {
            tx,
            writer,
            drop_warned: false,
        }
    }

    /// Hand a line to the writer task without ever awaiting: on a full
    /// channel (stalled disk) or a closed one (writer exited after an I/O
    /// error) the line is dropped — capture is best-effort by design.
    /// Returns whether the writer accepted the line.
    fn send_line(&mut self, line: &str) -> bool {
        match self.tx.try_send(line.to_string()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.drop_warned {
                    self.drop_warned = true;
                    tracing::warn!("agent stderr log capture dropping lines (writer backlogged)");
                }
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// Close the channel and wait for the writer to drain the backlog and
    /// flush the capture file (monorepo#3570): called at stderr EOF so
    /// "settled" means every captured line is durably on disk.
    async fn finish(self) {
        drop(self.tx);
        let _ = self.writer.await;
    }
}

/// Writer task behind [`StderrLogSink`]: owns the daily-rotated file and
/// exits on the first write failure (subsequent sends then see a closed
/// channel and drop silently). When the sink is dropped (child exited /
/// connection closed) it drains the remaining lines and flushes.
async fn stderr_log_writer(dir: PathBuf, mut rx: mpsc::Receiver<String>) {
    let mut current: Option<(String, tokio::fs::File)> = None;
    while let Some(line) = rx.recv().await {
        let flush = rx.is_empty();
        if let Err(e) = write_stderr_log_line(&dir, &mut current, &line, flush).await {
            tracing::warn!(dir = %dir.display(), error = %e, "agent stderr log capture disabled (write failed)");
            return;
        }
    }
    if let Some((_, mut file)) = current {
        let _ = file.flush().await;
    }
}

/// Append one line to the daily capture file, opening/rolling it as needed.
/// Flushes only when the writer's channel drained empty, batching flushes
/// under bursts.
async fn write_stderr_log_line(
    dir: &Path,
    current: &mut Option<(String, tokio::fs::File)>,
    line: &str,
    flush: bool,
) -> std::io::Result<()> {
    let name = intent_core::current_agent_log_file_name();
    if current.as_ref().map(|(n, _)| n.as_str()) != Some(name.as_str()) {
        // Hardened creation (STAB-56): dir `0700` / file `0600` on Unix via
        // the shared intent-core helpers, applied at creation time so there
        // is no world-readable window. `spawn_blocking` keeps the rare sync
        // open (once per day/connection) off the async runtime; failures
        // surface exactly like the previous create/open errors — the writer
        // exits and capture is disabled — and the bounded channel still
        // shields the stderr drain loop (STAB-53).
        let dir_owned = dir.to_path_buf();
        let path = dir.join(&name);
        let file = tokio::task::spawn_blocking(move || {
            intent_core::create_agent_log_dir(&dir_owned)?;
            intent_core::open_agent_log_file(&path)
        })
        .await
        .map_err(std::io::Error::other)??;
        *current = Some((name, tokio::fs::File::from_std(file)));
    }
    let (_, file) = current.as_mut().expect("sink file just opened");
    file.write_all(line.as_bytes()).await?;
    file.write_all(b"\n").await?;
    if flush {
        file.flush().await?;
    }
    Ok(())
}

/// Bounded ring buffer of recent stderr lines (parity: `recentStderrErrors`).
#[derive(Default)]
struct StderrBuffer {
    entries: VecDeque<String>,
}

impl StderrBuffer {
    fn push(&mut self, line: String) {
        let bounded = if line.len() > MAX_RECENT_STDERR_ENTRY_CHARS {
            truncate_middle(&line, MAX_RECENT_STDERR_ENTRY_CHARS)
        } else {
            line
        };
        self.entries.push_back(bounded);
        while self.entries.len() > MAX_RECENT_STDERR_ERRORS {
            self.entries.pop_front();
        }
    }

    fn recent(&self) -> Vec<String> {
        self.entries.iter().cloned().collect()
    }
}

/// Truncate a string in the middle, keeping head and tail (parity:
/// `truncateMiddleContent`).
fn truncate_middle(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let marker = "…[truncated]…";
    let keep = max.saturating_sub(marker.len());
    let head = keep / 2;
    let tail = keep - head;
    let head_str: String = s.chars().take(head).collect();
    let tail_str: String = s
        .chars()
        .rev()
        .take(tail)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head_str}{marker}{tail_str}")
}

/// Route one parsed JSON-RPC message to the pending map / request hook /
/// notification hook (§6.3 reader dispatch).
///
/// Every response line (a message with an `id` and no `method`) bumps the
/// response watermark BEFORE the pending-map lookup, so a response whose
/// pending entry was already removed (the caller dropped its request future —
/// see [`PendingEntryGuard`]) still advances the watermark. This is
/// client-side bookkeeping only; nothing changes on the wire.
fn dispatch(
    value: &Value,
    pending: &PendingMap,
    requests: Option<&mpsc::UnboundedSender<IncomingRequest>>,
    notifications: Option<&mpsc::UnboundedSender<IncomingNotification>>,
    response_seq: &AtomicU64,
    response_notify: &Notify,
    client_request_seq: &AtomicU64,
) {
    let Some(obj) = value.as_object() else { return };
    let method = obj.get("method").and_then(|m| m.as_str());
    let id = obj.get("id").cloned().filter(|v| !v.is_null());

    if let Some(method) = method {
        let method = method.to_string();
        let params = obj.get("params").cloned().unwrap_or(Value::Null);
        match id {
            Some(id) => {
                // Count BEFORE forwarding: the watermark must never read
                // lower than the number of requests already handed to a
                // handler that may side-effect (fs writes, terminal exec).
                client_request_seq.fetch_add(1, Ordering::SeqCst);
                if let Some(tx) = requests {
                    let _ = tx.send(IncomingRequest { id, method, params });
                }
            }
            None => {
                if let Some(tx) = notifications {
                    let _ = tx.send(IncomingNotification { method, params });
                }
            }
        }
        return;
    }

    let Some(id) = id else { return };
    response_seq.fetch_add(1, Ordering::SeqCst);
    response_notify.notify_waiters();
    let Some(key) = id.as_i64() else { return };
    let Some(sender) = pending.lock().unwrap().remove(&key) else {
        return;
    };
    if let Some(err) = obj.get("error") {
        let _ = sender.send(Err(JsonRpcError {
            code: err.get("code").and_then(Value::as_i64).unwrap_or(0),
            message: err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            data: err.get("data").cloned(),
        }));
    } else {
        let result = obj.get("result").cloned().unwrap_or(Value::Null);
        let _ = sender.send(Ok(result));
    }
}

/// A live JSON-RPC connection to a spawned agent over piped stdio.
///
/// Owns the writer/reader tasks and the pending-request correlation map.
/// All outbound traffic is serialized through a single writer task; inbound
/// traffic is routed by [`dispatch`]. Dropping the connection aborts the
/// writer/reader tasks — but NOT the stderr drain task (monorepo#3570): the
/// drain runs to the child's stderr EOF so the dying words a crashing child
/// writes right as teardown drops the connection still reach the capture
/// file. Every teardown path kills the child's whole process group, which
/// normally closes the pipe promptly — but a descendant that re-`setsid`
/// into its OWN group survives the `killpg` and can hold the write end open,
/// so the detached drain (and its capture file) may outlive the connection
/// until that process exits or the daemon's shutdown sweep reaps it.
pub struct Connection {
    callback_routes: Arc<CallbackToolRoutes>,
    writer_tx: mpsc::Sender<String>,
    pending: PendingMap,
    prompt_pending: Arc<Mutex<PromptPending>>,
    next_id: AtomicI64,
    response_seq: Arc<AtomicU64>,
    response_notify: Arc<Notify>,
    client_request_seq: Arc<AtomicU64>,
    stderr: Arc<Mutex<StderrBuffer>>,
    auth_error: Arc<AtomicBool>,
    stderr_settled: watch::Receiver<bool>,
    stderr_captured: Arc<AtomicBool>,
    auth_required: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Connection {
    /// Wire up the writer/reader/stderr tasks around a child's piped stdio.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn new<W, R>(
        stdin: W,
        stdout: R,
        stderr: Option<Box<dyn AsyncRead + Unpin + Send>>,
        hooks: ConnectionHooks,
    ) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let prompt_pending = Arc::new(Mutex::new(PromptPending::default()));
        let response_seq = Arc::new(AtomicU64::new(0));
        let response_notify = Arc::new(Notify::new());
        let client_request_seq = Arc::new(AtomicU64::new(0));
        let stderr_buf = Arc::new(Mutex::new(StderrBuffer::default()));
        let auth_error = Arc::new(AtomicBool::new(false));
        let auth_required = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();

        // Writer task: drain whole lines to stdin, one at a time.
        let (writer_tx, mut writer_rx) = mpsc::channel::<String>(WRITER_CHANNEL_CAPACITY);
        tasks.push(tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(line) = writer_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if stdin.flush().await.is_err() {
                    break;
                }
            }
        }));

        // Reader task: frame on `\n`, parse, dispatch.
        let pending_reader = Arc::clone(&pending);
        let prompt_pending_reader = Arc::clone(&prompt_pending);
        let seq_reader = Arc::clone(&response_seq);
        let notify_reader = Arc::clone(&response_notify);
        let client_req_seq_reader = Arc::clone(&client_request_seq);
        let requests = hooks.requests;
        let notifications = hooks.notifications;
        let auth_marker = hooks.auth_required_stdout_marker;
        let auth_required_reader = Arc::clone(&auth_required);
        let auth_error_reader = Arc::clone(&auth_error);
        let agent_id_reader = hooks.agent_id;
        tasks.push(tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                if auth_marker.is_some_and(|marker| line.trim() == marker) {
                    auth_required_reader.store(true, Ordering::SeqCst);
                    auth_error_reader.store(true, Ordering::SeqCst);
                    prompt_pending_reader.lock().unwrap().close();
                    for (_, sender) in pending_reader.lock().unwrap().drain() {
                        let _ = sender.send(Err(authentication_required()));
                    }
                    // Continue draining stdout, but never parse OAuth banners
                    // printed after the browser guard. The caller reaps the child.
                    continue;
                }
                if auth_required_reader.load(Ordering::SeqCst) {
                    continue;
                }
                match serde_json::from_str::<Value>(&line) {
                    Ok(value) => {
                        if value.get("method").and_then(Value::as_str).is_none() {
                            if let Some(id) = value.get("id").and_then(Value::as_i64) {
                                prompt_pending_reader.lock().unwrap().retire(id);
                            }
                        }
                        dispatch(
                            &value,
                            &pending_reader,
                            requests.as_ref(),
                            notifications.as_ref(),
                            &seq_reader,
                            &notify_reader,
                            &client_req_seq_reader,
                        );
                    }
                    // Attribution without content: the agent id and the line
                    // length locate the offending child, the line itself is
                    // never logged.
                    Err(e) => tracing::warn!(
                        agent = agent_id_reader.as_deref().unwrap_or("unknown"),
                        line_len = line.len(),
                        error = %e,
                        "failed to parse ACP stdout line"
                    ),
                }
            }
            // stdout closed: fail every still-pending request.
            prompt_pending_reader.lock().unwrap().close();
            {
                let mut map = pending_reader.lock().unwrap();
                for (_, sender) in map.drain() {
                    let _ = sender.send(Err(JsonRpcError {
                        code: 0,
                        message: "agent stdout closed".to_string(),
                        data: None,
                    }));
                }
            }
            // Wake watermark waiters so they recheck instead of sleeping out
            // their full timeout against a dead child; no response arrived,
            // so the seq is NOT bumped and `await_response_after`'s timeout
            // remains the backstop.
            notify_reader.notify_waiters();
        }));

        // Stderr drain task: ring-buffer recent lines, flag auth-error
        // patterns, and append every raw line to the per-agent capture file
        // when configured. NOT abort-on-drop (monorepo#3570): the drain runs
        // to stderr EOF so the dying words a crashing child writes while the
        // terminal-failure teardown drops the connection are still captured.
        // Reads bytes + lossy-decodes so one invalid-UTF-8 blob cannot kill
        // capture for the rest of the child's life. At EOF it finishes the
        // sink (drain backlog + flush) and then flips the settled watch.
        let (settled_tx, stderr_settled) = watch::channel(stderr.is_none());
        let stderr_captured = Arc::new(AtomicBool::new(false));
        if let Some(stderr) = stderr {
            let stderr_buf_task = Arc::clone(&stderr_buf);
            let auth_flag = Arc::clone(&auth_error);
            let captured_flag = Arc::clone(&stderr_captured);
            let patterns: Vec<String> = hooks
                .auth_error_patterns
                .iter()
                .map(|p| p.to_lowercase())
                .collect();
            let mut log_sink = hooks.stderr_log_dir.map(StderrLogSink::new);
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut buf = Vec::new();
                loop {
                    buf.clear();
                    match reader.read_until(b'\n', &mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                    if buf.last() == Some(&b'\n') {
                        buf.pop();
                        if buf.last() == Some(&b'\r') {
                            buf.pop();
                        }
                    }
                    let line = String::from_utf8_lossy(&buf);
                    if let Some(sink) = log_sink.as_mut() {
                        if sink.send_line(&line) {
                            captured_flag.store(true, Ordering::SeqCst);
                        }
                    }
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    if !patterns.is_empty() {
                        let lower = trimmed.to_lowercase();
                        if patterns.iter().any(|p| lower.contains(p)) {
                            auth_flag.store(true, Ordering::SeqCst);
                        }
                    }
                    stderr_buf_task.lock().unwrap().push(trimmed.to_string());
                }
                if let Some(sink) = log_sink.take() {
                    sink.finish().await;
                }
                let _ = settled_tx.send(true);
            });
        }

        Self {
            callback_routes: Arc::new(CallbackToolRoutes::default()),
            writer_tx,
            pending,
            prompt_pending,
            next_id: AtomicI64::new(1),
            response_seq,
            response_notify,
            client_request_seq,
            stderr: stderr_buf,
            auth_error,
            stderr_settled,
            stderr_captured,
            auth_required,
            tasks,
        }
    }

    /// Historical callback attribution belonging only to this connection.
    #[must_use]
    pub fn callback_tool_routes(&self) -> Arc<CallbackToolRoutes> {
        Arc::clone(&self.callback_routes)
    }

    pub(crate) async fn request_callback(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
        cancelled: impl Future<Output = ()>,
    ) -> CallbackRequestOutcome {
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline, cancelled);
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let mut guard = CallbackPendingGuard {
            connection: self,
            id,
            queued: false,
            completed: false,
        };
        if self.auth_required.load(Ordering::SeqCst) {
            return CallbackRequestOutcome::NotSent(CallbackFailure::Connection(AcpError::Rpc(
                authentication_required(),
            )));
        }
        let line = match encode_message(Some(id), method, &params) {
            Ok(line) => line,
            Err(error) => {
                return CallbackRequestOutcome::NotSent(CallbackFailure::Connection(error))
            }
        };
        // Reserving is cancellation-safe. The queued flag and send occur in the
        // same poll, with no await between them; NotSent therefore means no send.
        let permit = tokio::select! {
            biased;
            () = &mut cancelled => return CallbackRequestOutcome::NotSent(CallbackFailure::Cancelled),
            () = &mut deadline => return CallbackRequestOutcome::NotSent(CallbackFailure::Deadline),
            permit = self.writer_tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => return CallbackRequestOutcome::NotSent(CallbackFailure::Connection(AcpError::Transport("writer task closed".into()))),
            },
        };
        guard.queued = true;
        permit.send(line);
        tokio::select! {
            biased;
            () = &mut cancelled => CallbackRequestOutcome::Unknown(CallbackFailure::Cancelled),
            () = &mut deadline => CallbackRequestOutcome::Unknown(CallbackFailure::Deadline),
            response = rx => match response {
                Ok(Ok(value)) => {
                    guard.completed = true;
                    CallbackRequestOutcome::Response(value)
                }
                Ok(Err(error)) => CallbackRequestOutcome::Unknown(CallbackFailure::Connection(AcpError::Rpc(error))),
                Err(_) => CallbackRequestOutcome::Unknown(CallbackFailure::Connection(AcpError::Transport("response channel dropped".into()))),
            },
        }
    }

    /// Send a request and await its response with the default timeout (§6.4).
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Transport`] if the connection is closed or the write fails; [`AcpError::Rpc`] if the agent answers with a JSON-RPC error; [`AcpError::Timeout`] if no response arrives in time.
    pub async fn request(&self, method: &str, params: Value) -> AcpResult<Value> {
        self.request_timeout(method, params, DEFAULT_REQUEST_TIMEOUT)
            .await
    }

    /// Send a request and await its response with an explicit timeout.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Transport`] if the connection is closed or the write fails; [`AcpError::Rpc`] if the agent answers with a JSON-RPC error; [`AcpError::Timeout`] if no response arrives in time.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn request_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> AcpResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.request_with_id(id, method, params, timeout).await
    }

    /// [`Connection::request_timeout`] that also tells the peer about an
    /// abandoned request: when the request times out, the notification
    /// `cancel(id)` yields (`(method, params)`, e.g. MCP's
    /// `notifications/cancelled { requestId, reason }`) is sent for the
    /// abandoned request id. The pending slot is already gone by then, so a
    /// late reply to that id is discarded rather than delivered. The cancel
    /// is best-effort and never waits: it is queued only if the writer has
    /// room right now (a full queue means the peer has stopped reading its
    /// stdin, and the timed-out caller must not block behind it). A cancel
    /// that is dropped or fails to send is logged, never surfaced — the
    /// timeout is the caller-facing outcome either way, on the timer's
    /// schedule.
    ///
    /// # Errors
    ///
    /// Same as [`Connection::request_timeout`].
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn request_timeout_with_cancel(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
        cancel: impl FnOnce(i64) -> (String, Value),
    ) -> AcpResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let result = self.request_with_id(id, method, params, timeout).await;
        if matches!(result, Err(AcpError::Timeout(_))) {
            let (cancel_method, cancel_params) = cancel(id);
            if let Err(e) = self.try_notify(&cancel_method, &cancel_params) {
                tracing::debug!(
                    method,
                    id,
                    error = %e,
                    "cancel notification for timed-out request not sent"
                );
            }
        }
        result
    }

    /// [`Connection::notify`] that never waits for writer capacity: the line
    /// is queued if the outbound channel has room right now, else dropped.
    fn try_notify(&self, method: &str, params: &Value) -> AcpResult<()> {
        let line = encode_message(None, method, params)?;
        self.writer_tx.try_send(line).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                AcpError::Transport("writer queue full".to_string())
            }
            mpsc::error::TrySendError::Closed(_) => {
                AcpError::Transport("writer task closed".to_string())
            }
        })
    }

    /// The request body shared by the `request_timeout*` entry points. The
    /// pending slot for `id` is dropped with the guard on return, so a caller
    /// that names the request to the peer afterwards does so with no slot
    /// left for a late reply to land in.
    async fn request_with_id(
        &self,
        id: i64,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> AcpResult<Value> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        // Drop-guard cleanup: covers the error/timeout arms below AND the
        // caller dropping this future mid-flight (cancel-safety for the
        // pending map — see `PendingEntryGuard`).
        let _guard = PendingEntryGuard {
            pending: Arc::clone(&self.pending),
            id,
        };

        // Check after insertion so a signal racing this request either drains
        // its sender or is observed here. No request can miss the auth failure.
        if self.auth_required.load(Ordering::SeqCst) {
            return Err(AcpError::Rpc(authentication_required()));
        }

        let line = encode_message(Some(id), method, &params)?;
        if self.writer_tx.send(line).await.is_err() {
            return Err(AcpError::Transport("writer task closed".to_string()));
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(err))) => Err(AcpError::Rpc(err)),
            Ok(Err(_)) => Err(AcpError::Transport("response channel dropped".to_string())),
            Err(_) => Err(AcpError::Timeout(method.to_string())),
        }
    }

    /// Only `session::prompt_with_guidance` constructs these matched parameters.
    /// No public arbitrary-method or alternate-request admission entry exists.
    pub(crate) async fn request_prompt_with_admission(
        &self,
        base_params: Value,
        enriched_params: Value,
        admission: Box<dyn AcpPromptAdmission>,
        timeout: Duration,
    ) -> AcpResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, mut rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let state = self.prompt_pending.lock().unwrap().insert(id);
        let _guard = PromptPendingGuard {
            original: PendingEntryGuard {
                pending: Arc::clone(&self.pending),
                id,
            },
            prompts: &self.prompt_pending,
            state: Arc::clone(&state),
        };
        if self.auth_required.load(Ordering::SeqCst) {
            return Err(AcpError::Rpc(authentication_required()));
        }
        if state.load(Ordering::SeqCst) != PROMPT_OPEN {
            return Err(AcpError::Transport("agent stdout closed".into()));
        }
        let base = encode_message(Some(id), "session/prompt", &base_params)?;
        let enriched = encode_message(Some(id), "session/prompt", &enriched_params)?;
        let permit = tokio::select! {
            biased;
            response = &mut rx => return prompt_response(response),
            permit = self.writer_tx.clone().reserve_owned() => {
                permit.map_err(|_| AcpError::Transport("writer task closed".into()))?
            }
        };
        let boundary = AcpPromptBoundary(Arc::new(()));
        let mut slot = PromptSlot {
            permit: Some(permit),
            base: Some(base),
            enriched: Some(enriched),
            queued: false,
        };
        // Construction, polling and cleanup may execute opaque owner code, so
        // all occur outside the consuming action. The packet only borrows slot.
        {
            let packet = PreparedAcpPromptTransfer {
                original: &boundary,
                state: &state,
                writer: &self.writer_tx,
                slot: &mut slot,
            };
            let future = catch_unwind(AssertUnwindSafe(|| admission.admit(&boundary, packet)));
            if let Ok(future) = future {
                let future = CaughtPromptAdmission {
                    future: Some(future),
                    state: &state,
                };
                let outcome = tokio::select! {
                    biased;
                    response = &mut rx => return prompt_response(response),
                    () = self.writer_tx.closed() => {
                        return Err(AcpError::Transport("writer task closed".into()));
                    }
                    outcome = tokio::time::timeout(PROMPT_ADMISSION_TIMEOUT, future) => outcome,
                };
                if let Ok(Some(AcpPromptAdmissionOutcome::Transferred(receipt))) = outcome {
                    // A foreign claim is never authority. Slot consumption below
                    // remains factual even after a bad receipt or policy panic.
                    let _original_receipt = Arc::ptr_eq(&boundary.0, &receipt.0);
                }
            }
        }
        if !slot.queued {
            if self.auth_required.load(Ordering::SeqCst) {
                return Err(AcpError::Rpc(authentication_required()));
            }
            match rx.try_recv() {
                Ok(response) => return prompt_response(Ok(response)),
                Err(oneshot::error::TryRecvError::Closed) => {
                    return Err(AcpError::Transport("response channel dropped".into()));
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
            let packet = PreparedAcpPromptTransfer {
                original: &boundary,
                state: &state,
                writer: &self.writer_tx,
                slot: &mut slot,
            };
            if !matches!(
                packet.transfer(&boundary, PromptVariant::Base),
                AcpPromptAdmissionOutcome::Transferred(_)
            ) {
                if self.auth_required.load(Ordering::SeqCst) {
                    return Err(AcpError::Rpc(authentication_required()));
                }
                if state.load(Ordering::SeqCst) != PROMPT_RETIRED {
                    return Err(AcpError::Transport(
                        "original prompt transfer closed".into(),
                    ));
                }
                // Reader retirement precedes dispatch/drain. Preserve that
                // original receiver's exact result if it has not arrived yet.
            }
        }
        drop(slot);
        // Same post-send response timeout as request_with_id. Optional admission
        // never spends this budget; the session's outer idle loop is unchanged.
        match tokio::time::timeout(timeout, rx).await {
            Ok(response) => prompt_response(response),
            Err(_) => Err(AcpError::Timeout("session/prompt".into())),
        }
    }

    /// Number of in-flight request correlation entries (test observability
    /// for the pending-map cancel-safety guarantee).
    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Current response watermark: the number of response lines the reader
    /// has dispatched so far. The counter is bumped for EVERY response line
    /// (a message with an `id` and no `method`) before the pending-map
    /// lookup, so it also counts responses to abandoned requests whose
    /// pending entry the drop-guard already removed. Client-side transport
    /// bookkeeping only — nothing changes on the wire.
    pub fn response_seq(&self) -> u64 {
        self.response_seq.load(Ordering::SeqCst)
    }

    /// Current agent→client request watermark: the number of agent-initiated
    /// requests (`fs/*`, `terminal/*`, `session/request_permission`, …) the
    /// reader has forwarded to the client-served handler so far. Bumped
    /// BEFORE the forward, so a caller comparing watermarks across a
    /// `session/prompt` attempt sees every request that may have
    /// side-effected (file writes, terminal commands) even if its handler is
    /// still running. Client-side transport bookkeeping only.
    pub fn client_request_seq(&self) -> u64 {
        self.client_request_seq.load(Ordering::SeqCst)
    }

    /// Wait until the response watermark advances past `since` (i.e.
    /// `response_seq() > since`), returning `true` when it does and `false`
    /// on timeout. Lets a caller that abandoned a request (dropped its
    /// future) wait — bounded — for the straggling response to actually
    /// arrive. Cancel-safe: dropping this future mid-wait mutates nothing.
    pub async fn await_response_after(&self, since: u64, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Arm the waiter BEFORE the recheck so a bump+notify landing
            // between the load and the await cannot be missed.
            let notified = self.response_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.response_seq.load(Ordering::SeqCst) > since {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.response_seq.load(Ordering::SeqCst) > since;
            }
        }
    }

    /// Whether any request correlation entries are still in flight (their
    /// futures are live and awaiting a response).
    #[cfg(test)]
    pub(crate) fn has_pending_requests(&self) -> bool {
        !self.pending.lock().unwrap().is_empty()
    }

    /// Send a notification (no id, no response).
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Transport`] if the connection is closed or the write fails.
    pub async fn notify(&self, method: &str, params: Value) -> AcpResult<()> {
        let line = encode_message(None, method, &params)?;
        self.writer_tx
            .send(line)
            .await
            .map_err(|_| AcpError::Transport("writer task closed".to_string()))
    }

    /// Send a successful response to an agent→client request.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Transport`] if the connection is closed or the write fails.
    pub async fn respond_result(&self, id: Value, result: Value) -> AcpResult<()> {
        let msg = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result });
        let line = format!("{}\n", serde_json::to_string(&msg)?);
        self.writer_tx
            .send(line)
            .await
            .map_err(|_| AcpError::Transport("writer task closed".to_string()))
    }

    /// Send an error response to an agent→client request.
    ///
    /// # Errors
    ///
    /// Returns [`AcpError::Transport`] if the connection is closed or the write fails.
    pub async fn respond_error(&self, id: Value, error: JsonRpcError) -> AcpResult<()> {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": error.code, "message": error.message, "data": error.data },
        });
        let line = format!("{}\n", serde_json::to_string(&msg)?);
        self.writer_tx
            .send(line)
            .await
            .map_err(|_| AcpError::Transport("writer task closed".to_string()))
    }

    /// Recent stderr lines captured from the agent (newest last).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn recent_stderr(&self) -> Vec<String> {
        self.stderr.lock().unwrap().recent()
    }

    /// Whether a configured auth-error pattern has been seen on stderr.
    pub(crate) fn auth_error_detected(&self) -> bool {
        self.auth_error.load(Ordering::SeqCst)
    }

    /// Cheap transport liveness probe: `false` once the writer task has
    /// exited (its exit drops `writer_rx`, closing the channel) — e.g. after
    /// a broken-pipe write to a dead child's stdin. NOTE this signal is lazy:
    /// the writer only notices the dead pipe on its NEXT write, so a child
    /// that died with no traffic since still reports `true` here — pair with
    /// `Child::try_wait` for a strong dead-child check (monorepo#764).
    pub fn is_alive(&self) -> bool {
        !self.writer_tx.is_closed()
    }

    /// Wait — bounded by `timeout` — until the stderr drain has hit EOF and
    /// the capture sink has flushed to disk (monorepo#3570). Returns `true`
    /// when settled (immediately when the connection had no stderr pipe),
    /// `false` on timeout. Callers about to log "stderr captured at <path>"
    /// after killing the child should await this first so the claim is true.
    pub async fn await_stderr_settled(&self, timeout: Duration) -> bool {
        let mut rx = self.stderr_settled.clone();
        tokio::time::timeout(timeout, rx.wait_for(|settled| *settled))
            .await
            .is_ok_and(|r| r.is_ok())
    }

    /// Whether THIS connection's child wrote at least one stderr line that
    /// the capture sink accepted (monorepo#3570). Distinguishes a fresh
    /// capture from stale daily files an earlier child left in the same
    /// per-agent dir, so the "stderr captured at <dir>" WARN never points at
    /// a directory this child contributed nothing to.
    pub fn stderr_captured(&self) -> bool {
        self.stderr_captured.load(Ordering::SeqCst)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Aborts the writer/reader tasks only; the stderr drain task is
        // deliberately not owned here — it runs to stderr EOF so the child's
        // dying words reach the capture file (monorepo#3570).
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Serialize a JSON-RPC request/notification frame with a trailing newline.
fn encode_message(id: Option<i64>, method: &str, params: &Value) -> AcpResult<String> {
    let msg = match id {
        Some(id) => serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params,
        }),
        None => serde_json::json!({
            "jsonrpc": "2.0", "method": method, "params": params,
        }),
    };
    Ok(format!("{}\n", serde_json::to_string(&msg)?))
}

#[cfg(test)]
mod prompt_transfer_tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;

    // Explicit neutral authority fixtures, not repository/Store/P validation.
    #[derive(Clone, Copy)]
    enum Mode {
        Accept,
        Refuse,
        ConstructorPanic,
        PollPanic,
        Hang,
        ForeignBoundary,
        ForeignReceipt,
        AfterPanic,
        AfterHang,
        AfterBadReceipt,
        DropPanic,
        Hold,
    }

    #[derive(Default)]
    struct Observation {
        calls: AtomicUsize,
        polls: AtomicUsize,
        future_drops: AtomicUsize,
        owner_drops: AtomicUsize,
        currency: AtomicBool,
        boundaries: Mutex<Vec<Arc<()>>>,
        entered: Notify,
        release: Notify,
    }

    struct Policy {
        mode: Mode,
        seen: Arc<Observation>,
    }

    struct FutureDrop {
        seen: Arc<Observation>,
        panic: bool,
    }
    impl Drop for FutureDrop {
        fn drop(&mut self) {
            self.seen.future_drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic, "injected future cleanup panic");
        }
    }
    impl Drop for Policy {
        fn drop(&mut self) {
            self.seen.owner_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl AcpPromptAdmission for Policy {
        fn admit<'a>(
            &'a self,
            original: &'a AcpPromptBoundary,
            packet: PreparedAcpPromptTransfer<'a>,
        ) -> intent_core::BoxFuture<'a, AcpPromptAdmissionOutcome> {
            self.seen.calls.fetch_add(1, Ordering::SeqCst);
            self.seen
                .boundaries
                .lock()
                .unwrap()
                .push(Arc::clone(&original.0));
            assert!(
                !matches!(self.mode, Mode::ConstructorPanic),
                "injected constructor panic"
            );
            let cleanup = FutureDrop {
                seen: Arc::clone(&self.seen),
                panic: matches!(self.mode, Mode::DropPanic),
            };
            Box::pin(async move {
                let _cleanup = cleanup;
                self.seen.polls.fetch_add(1, Ordering::SeqCst);
                self.seen.entered.notify_one();
                match self.mode {
                    Mode::Refuse | Mode::DropPanic => AcpPromptAdmissionOutcome::OmitOptional,
                    Mode::PollPanic => panic!("injected poll panic"),
                    Mode::Hang => std::future::pending().await,
                    Mode::ForeignBoundary => packet.transfer(
                        &AcpPromptBoundary(Arc::new(())),
                        PromptVariant::WithGuidance,
                    ),
                    Mode::ForeignReceipt => {
                        AcpPromptAdmissionOutcome::Transferred(AcpPromptReceipt(Arc::new(())))
                    }
                    Mode::AfterPanic => {
                        let _ = packet.transfer(original, PromptVariant::WithGuidance);
                        panic!("injected panic after actual queue transfer");
                    }
                    Mode::AfterHang => {
                        let _ = packet.transfer(original, PromptVariant::WithGuidance);
                        std::future::pending().await
                    }
                    Mode::AfterBadReceipt => {
                        let _ = packet.transfer(original, PromptVariant::WithGuidance);
                        AcpPromptAdmissionOutcome::Transferred(AcpPromptReceipt(Arc::new(())))
                    }
                    Mode::Hold => {
                        self.seen.release.notified().await;
                        let variant = if self.seen.currency.load(Ordering::SeqCst) {
                            PromptVariant::WithGuidance
                        } else {
                            PromptVariant::Base
                        };
                        packet.transfer(original, variant)
                    }
                    Mode::Accept | Mode::ConstructorPanic => {
                        let variant = if self.seen.currency.load(Ordering::SeqCst) {
                            PromptVariant::WithGuidance
                        } else {
                            PromptVariant::Base
                        };
                        packet.transfer(original, variant)
                    }
                }
            })
        }
    }

    fn policy(mode: Mode) -> (Box<dyn AcpPromptAdmission>, Arc<Observation>) {
        let seen = Arc::new(Observation::default());
        seen.currency.store(true, Ordering::SeqCst);
        (
            Box::new(Policy {
                mode,
                seen: Arc::clone(&seen),
            }),
            seen,
        )
    }

    fn fixture() -> (
        Connection,
        BufReader<tokio::io::DuplexStream>,
        tokio::io::DuplexStream,
    ) {
        let (input, output) = tokio::io::duplex(4096);
        let (reply, read) = tokio::io::duplex(4096);
        let conn = Connection::new(
            input,
            read,
            None,
            ConnectionHooks {
                auth_required_stdout_marker: Some("TEST_AUTH_REQUIRED"),
                ..ConnectionHooks::default()
            },
        );
        (conn, BufReader::new(output), reply)
    }

    fn request(
        conn: &Connection,
        admission: Box<dyn AcpPromptAdmission>,
        timeout: Duration,
    ) -> impl Future<Output = AcpResult<Value>> + '_ {
        conn.request_prompt_with_admission(
            json!({"sessionId":"same-public-session","prompt":[{"type":"text","text":"original"}]}),
            json!({"sessionId":"same-public-session","prompt":[{"type":"text","text":"original"},{"type":"text","text":"optional"}]}),
            admission, timeout,
        )
    }

    async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    async fn read_frame<F: Future>(
        reader: &mut BufReader<tokio::io::DuplexStream>,
        mut future: Pin<&mut F>,
    ) -> Value {
        let mut line = String::new();
        tokio::select! {
            biased;
            read = reader.read_line(&mut line) => { assert!(read.unwrap() > 0); }
            _ = &mut future => panic!("request settled before the test replied"),
            () = tokio::time::sleep(Duration::from_secs(3)) => panic!("no outbound frame"),
        }
        serde_json::from_str(&line).unwrap()
    }

    async fn reply(writer: &mut tokio::io::DuplexStream, id: &Value) {
        writer
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":id,"result":{"originalResult":true}})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }

    async fn no_frame(reader: &mut BufReader<tokio::io::DuplexStream>) {
        let mut extra = String::new();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), reader.read_line(&mut extra))
                .await
                .is_err(),
            "unexpected frame: {extra}"
        );
    }

    async fn until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn reserve_all(conn: &Connection) -> Vec<mpsc::OwnedPermit<String>> {
        (0..WRITER_CHANNEL_CAPACITY)
            .map(|_| conn.writer_tx.clone().try_reserve_owned().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn prompt_variants_preserve_one_id_slot_result_and_post_effect_truth() {
        for mode in [
            Mode::Accept,
            Mode::Refuse,
            Mode::ConstructorPanic,
            Mode::PollPanic,
            Mode::ForeignBoundary,
            Mode::ForeignReceipt,
            Mode::AfterPanic,
            Mode::AfterHang,
            Mode::AfterBadReceipt,
            Mode::DropPanic,
        ] {
            let (conn, mut reader, mut writer) = fixture();
            let (admission, seen) = policy(mode);
            let mut future = Box::pin(request(&conn, admission, Duration::from_secs(2)));
            let frame = read_frame(&mut reader, future.as_mut()).await;
            assert_eq!(frame["id"], 1);
            assert_eq!(frame["method"], "session/prompt");
            assert_eq!(frame["params"]["prompt"][0]["text"], "original");
            let enriched = matches!(
                mode,
                Mode::Accept | Mode::AfterPanic | Mode::AfterHang | Mode::AfterBadReceipt
            );
            assert_eq!(
                frame["params"]["prompt"].as_array().unwrap().len(),
                if enriched { 2 } else { 1 }
            );
            assert_eq!(conn.pending_len(), 1);
            assert_eq!(conn.next_id.load(Ordering::SeqCst), 2);
            reply(&mut writer, &frame["id"]).await;
            assert_eq!(future.await.unwrap(), json!({"originalResult":true}));
            assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
            assert_eq!(seen.owner_drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                seen.future_drops.load(Ordering::SeqCst),
                usize::from(!matches!(mode, Mode::ConstructorPanic))
            );
            assert_eq!(conn.pending_len(), 0);
            assert!(conn.prompt_pending.lock().unwrap().entries.is_empty());
            no_frame(&mut reader).await;
        }
    }

    #[tokio::test]
    async fn prompt_backpressure_rechecks_optional_currency_using_original_slot() {
        let (conn, mut reader, mut writer) = fixture();
        let mut reserved = reserve_all(&conn);
        let (admission, seen) = policy(Mode::Accept);
        let mut future = Box::pin(request(&conn, admission, Duration::from_secs(2)));
        poll_pending(future.as_mut()).await;
        assert_eq!(conn.pending_len(), 1);
        assert_eq!(seen.calls.load(Ordering::SeqCst), 0);
        assert_eq!(conn.writer_tx.capacity(), 0);
        seen.currency.store(false, Ordering::SeqCst);
        drop(reserved.pop());
        let frame = read_frame(&mut reader, future.as_mut()).await;
        assert_eq!(frame["params"]["prompt"].as_array().unwrap().len(), 1);
        reply(&mut writer, &frame["id"]).await;
        assert!(future.await.is_ok());
        assert_eq!(conn.next_id.load(Ordering::SeqCst), 2);
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
        assert_eq!(conn.writer_tx.capacity(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn prompt_optional_timeout_uses_base_without_spending_response_timeout() {
        let (conn, mut reader, mut writer) = fixture();
        let (admission, seen) = policy(Mode::Hang);
        let mut future = Box::pin(request(&conn, admission, Duration::from_millis(50)));
        poll_pending(future.as_mut()).await;
        tokio::time::advance(PROMPT_ADMISSION_TIMEOUT).await;
        let frame = read_frame(&mut reader, future.as_mut()).await;
        assert_eq!(frame["params"]["prompt"].as_array().unwrap().len(), 1);
        assert_eq!(seen.future_drops.load(Ordering::SeqCst), 1);
        reply(&mut writer, &frame["id"]).await;
        assert!(future.await.is_ok());
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn prompt_cancellation_unpolled_queue_and_admission_leave_no_send() {
        for stage in 0..3 {
            let (conn, mut reader, _writer) = fixture();
            let reserved = if stage == 1 {
                reserve_all(&conn)
            } else {
                Vec::new()
            };
            let (admission, seen) = policy(Mode::Hold);
            let mut future = Box::pin(request(&conn, admission, Duration::from_secs(2)));
            if stage > 0 {
                poll_pending(future.as_mut()).await;
            }
            assert_eq!(conn.pending_len(), usize::from(stage > 0));
            drop(future);
            assert_eq!(conn.pending_len(), 0);
            assert!(conn.prompt_pending.lock().unwrap().entries.is_empty());
            assert_eq!(seen.owner_drops.load(Ordering::SeqCst), 1);
            assert_eq!(seen.calls.load(Ordering::SeqCst), usize::from(stage == 2));
            assert_eq!(
                seen.future_drops.load(Ordering::SeqCst),
                usize::from(stage == 2)
            );
            drop(reserved);
            no_frame(&mut reader).await;
        }
    }

    #[tokio::test]
    async fn prompt_auth_drain_during_queue_or_admission_prevents_transfer() {
        for during_queue in [true, false] {
            let (conn, mut reader, mut writer) = fixture();
            let reserved = if during_queue {
                reserve_all(&conn)
            } else {
                Vec::new()
            };
            let (admission, seen) = policy(Mode::Hold);
            let mut future = Box::pin(request(&conn, admission, Duration::from_secs(2)));
            poll_pending(future.as_mut()).await;
            writer.write_all(b"TEST_AUTH_REQUIRED\n").await.unwrap();
            until(|| conn.auth_required.load(Ordering::SeqCst)).await;
            seen.release.notify_one();
            drop(reserved);
            assert!(
                matches!(future.await,Err(AcpError::Rpc(ref e)) if e.message.contains("Authentication required"))
            );
            assert_eq!(conn.pending_len(), 0);
            no_frame(&mut reader).await;
            let (admission, later) = policy(Mode::Accept);
            assert!(matches!(
                request(&conn, admission, Duration::from_secs(2)).await,
                Err(AcpError::Rpc(_))
            ));
            assert_eq!(later.calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn prompt_stdout_close_and_writer_close_are_original_failures() {
        for close_writer in [false, true] {
            let (conn, reader, writer) = fixture();
            let (admission, seen) = policy(Mode::Hold);
            let mut future = Box::pin(request(&conn, admission, Duration::from_secs(2)));
            poll_pending(future.as_mut()).await;
            if close_writer {
                drop(reader);
                conn.notify("fixture", json!({})).await.unwrap();
                until(|| !conn.is_alive()).await;
            } else {
                drop(writer);
                until(|| conn.prompt_pending.lock().unwrap().closed).await;
            }
            assert!(future.await.is_err());
            assert_eq!(conn.pending_len(), 0);
            assert_eq!(seen.future_drops.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn prompt_response_timeout_late_reply_and_new_request_keep_correlation() {
        let (conn, mut reader, mut writer) = fixture();
        let (admission, _) = policy(Mode::AfterHang);
        let mut future = Box::pin(request(&conn, admission, Duration::from_millis(50)));
        let first = read_frame(&mut reader, future.as_mut()).await;
        tokio::time::advance(Duration::from_millis(51)).await;
        assert!(matches!(future.await, Err(AcpError::Timeout(_))));
        assert_eq!(conn.pending_len(), 0);
        let (admission, _) = policy(Mode::Accept);
        let mut next = Box::pin(request(&conn, admission, Duration::from_secs(2)));
        let second = read_frame(&mut reader, next.as_mut()).await;
        assert_eq!(second["id"], 2);
        reply(&mut writer, &first["id"]).await;
        assert!(conn.await_response_after(0, Duration::from_secs(2)).await);
        poll_pending(next.as_mut()).await;
        assert_eq!(conn.pending_len(), 1);
        reply(&mut writer, &second["id"]).await;
        assert!(next.await.is_ok());
        assert_eq!(conn.response_seq(), 2);
    }

    #[tokio::test]
    async fn prompt_cancel_after_transfer_preserves_effect_without_duplicate() {
        let (conn, mut reader, mut writer) = fixture();
        let (admission, seen) = policy(Mode::AfterHang);
        let mut future = Box::pin(request(&conn, admission, Duration::from_secs(2)));
        let frame = read_frame(&mut reader, future.as_mut()).await;
        drop(future);
        assert_eq!(conn.pending_len(), 0);
        assert_eq!(seen.future_drops.load(Ordering::SeqCst), 1);
        reply(&mut writer, &frame["id"]).await;
        assert!(conn.await_response_after(0, Duration::from_secs(2)).await);
        no_frame(&mut reader).await;
    }

    #[tokio::test]
    async fn prompt_equal_public_ids_on_other_connection_do_not_replace_boundary() {
        struct Foreign(AcpPromptBoundary);
        impl AcpPromptAdmission for Foreign {
            fn admit<'a>(
                &'a self,
                _: &'a AcpPromptBoundary,
                packet: PreparedAcpPromptTransfer<'a>,
            ) -> intent_core::BoxFuture<'a, AcpPromptAdmissionOutcome> {
                Box::pin(async move { packet.transfer(&self.0, PromptVariant::WithGuidance) })
            }
        }
        let (first, mut first_reader, mut first_writer) = fixture();
        let (second, mut second_reader, mut second_writer) = fixture();
        let (admission, seen) = policy(Mode::Hold);
        let mut first_request = Box::pin(request(&first, admission, Duration::from_secs(2)));
        poll_pending(first_request.as_mut()).await;
        let foreign = AcpPromptBoundary(Arc::clone(&seen.boundaries.lock().unwrap()[0]));
        let mut second_request = Box::pin(request(
            &second,
            Box::new(Foreign(foreign)),
            Duration::from_secs(2),
        ));
        let second_frame = read_frame(&mut second_reader, second_request.as_mut()).await;
        assert_eq!(second_frame["id"], 1);
        assert_eq!(
            second_frame["params"]["prompt"].as_array().unwrap().len(),
            1
        );
        reply(&mut second_writer, &second_frame["id"]).await;
        assert!(second_request.await.is_ok());
        assert_eq!(first.pending_len(), 1);
        seen.release.notify_one();
        let first_frame = read_frame(&mut first_reader, first_request.as_mut()).await;
        assert_eq!(first_frame["id"], second_frame["id"]);
        assert_eq!(
            first_frame["params"]["sessionId"],
            second_frame["params"]["sessionId"]
        );
        assert_eq!(first_frame["params"]["prompt"].as_array().unwrap().len(), 2);
        reply(&mut first_writer, &first_frame["id"]).await;
        assert!(first_request.await.is_ok());
    }

    #[tokio::test]
    async fn prompt_auth_after_queue_preserves_one_effect_and_original_auth_failure() {
        let (conn, mut reader, mut writer) = fixture();
        let (admission, _) = policy(Mode::AfterHang);
        let mut future = Box::pin(request(&conn, admission, Duration::from_secs(2)));
        let frame = read_frame(&mut reader, future.as_mut()).await;
        assert_eq!(frame["params"]["prompt"].as_array().unwrap().len(), 2);
        writer.write_all(b"TEST_AUTH_REQUIRED\n").await.unwrap();
        assert!(
            matches!(future.await, Err(AcpError::Rpc(ref e)) if e.message.contains("Authentication required"))
        );
        assert_eq!(conn.pending_len(), 0);
        no_frame(&mut reader).await;
    }

    #[tokio::test]
    async fn prompt_drained_original_pending_drops_unpolled_admission_without_send() {
        struct Drain {
            connection: Arc<Connection>,
            seen: Arc<Observation>,
        }
        impl AcpPromptAdmission for Drain {
            fn admit<'a>(
                &'a self,
                original: &'a AcpPromptBoundary,
                packet: PreparedAcpPromptTransfer<'a>,
            ) -> intent_core::BoxFuture<'a, AcpPromptAdmissionOutcome> {
                self.connection.prompt_pending.lock().unwrap().retire(1);
                self.connection
                    .pending
                    .lock()
                    .unwrap()
                    .remove(&1)
                    .unwrap()
                    .send(Err(authentication_required()))
                    .unwrap();
                let cleanup = FutureDrop {
                    seen: Arc::clone(&self.seen),
                    panic: false,
                };
                Box::pin(async move {
                    let _cleanup = cleanup;
                    self.seen.polls.fetch_add(1, Ordering::SeqCst);
                    packet.transfer(original, PromptVariant::WithGuidance)
                })
            }
        }
        let (connection, mut reader, _writer) = fixture();
        let connection = Arc::new(connection);
        let seen = Arc::new(Observation::default());
        let admission = Box::new(Drain {
            connection: Arc::clone(&connection),
            seen: Arc::clone(&seen),
        });
        assert!(matches!(
            request(&connection, admission, Duration::from_secs(2)).await,
            Err(AcpError::Rpc(_))
        ));
        assert_eq!(seen.polls.load(Ordering::SeqCst), 0);
        assert_eq!(seen.future_drops.load(Ordering::SeqCst), 1);
        assert_eq!(connection.pending_len(), 0);
        no_frame(&mut reader).await;
    }
}

#[cfg(test)]
mod prompt_retirement_tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn prompt_retirement_before_sender_settlement_preserves_original_error() {
        struct Retire(Arc<Connection>);
        impl AcpPromptAdmission for Retire {
            fn admit<'a>(
                &'a self,
                _: &'a AcpPromptBoundary,
                _: PreparedAcpPromptTransfer<'a>,
            ) -> intent_core::BoxFuture<'a, AcpPromptAdmissionOutcome> {
                self.0.prompt_pending.lock().unwrap().retire(1);
                let sender = self.0.pending.lock().unwrap().remove(&1).unwrap();
                // Explicitly schedule the reader's retire-before-settle gap.
                tokio::spawn(async move {
                    tokio::task::yield_now().await;
                    let _ = sender.send(Err(JsonRpcError {
                        code: -123,
                        message: "original terminal error".into(),
                        data: Some(json!({"original":true})),
                    }));
                });
                Box::pin(async { AcpPromptAdmissionOutcome::OmitOptional })
            }
        }
        let (conn, mut reader, _writer) = super::watermark_tests::silent_connection();
        let conn = Arc::new(conn);
        let error = conn
            .request_prompt_with_admission(
                json!({}),
                json!({"optional":true}),
                Box::new(Retire(Arc::clone(&conn))),
                Duration::from_secs(2),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error,AcpError::Rpc(ref e) if e.code == -123 && e.data == Some(json!({"original":true})))
        );
        assert_eq!(conn.pending_len(), 0);
        let mut line = String::new();
        assert!(tokio::time::timeout(
            Duration::from_millis(30),
            BufReader::new(&mut reader).read_line(&mut line)
        )
        .await
        .is_err());
    }
}

#[cfg(test)]
mod watermark_tests {
    use super::*;
    use serde_json::json;

    /// A duplex-backed `Connection` whose "agent" never responds on its own:
    /// the test holds both remote ends and writes response lines by hand.
    pub(super) fn silent_connection(
    ) -> (Connection, tokio::io::DuplexStream, tokio::io::DuplexStream) {
        let (c2a_client, c2a_agent) = tokio::io::duplex(4096);
        let (a2c_agent, a2c_client) = tokio::io::duplex(4096);
        let conn = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());
        (conn, c2a_agent, a2c_agent)
    }

    /// The response to an abandoned request (future dropped, pending entry
    /// removed by the drop guard) still bumps the watermark — the bump
    /// happens before the pending-map lookup.
    #[tokio::test]
    async fn abandoned_request_response_still_bumps_watermark() {
        let (conn, _c2a_agent, mut a2c_agent) = silent_connection();
        assert_eq!(conn.response_seq(), 0);

        let mut fut =
            Box::pin(conn.request_timeout("session/prompt", json!({}), Duration::from_secs(60)));
        tokio::select! {
            _ = &mut fut => panic!("request must still be pending"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        drop(fut);
        assert!(!conn.has_pending_requests(), "drop guard removed the entry");

        // The straggling response for the abandoned id (first id is 1).
        a2c_agent
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n")
            .await
            .unwrap();
        a2c_agent.flush().await.unwrap();

        assert!(
            conn.await_response_after(0, Duration::from_secs(2)).await,
            "watermark advances for the abandoned request's response"
        );
        assert_eq!(conn.response_seq(), 1);
        assert!(!conn.has_pending_requests());
    }

    /// `await_response_after` resolves `true` when a later response lands and
    /// `false` on timeout when none does.
    #[tokio::test]
    async fn await_response_after_resolves_and_times_out() {
        let (conn, _c2a_agent, mut a2c_agent) = silent_connection();

        assert!(
            !conn
                .await_response_after(conn.response_seq(), Duration::from_millis(50))
                .await,
            "no response → false on timeout"
        );

        // Arm a waiter first, then land a response (an id with no pending
        // entry still counts — the bump precedes the lookup).
        let conn = Arc::new(conn);
        let waiter = {
            let conn = Arc::clone(&conn);
            tokio::spawn(async move { conn.await_response_after(0, Duration::from_secs(2)).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        a2c_agent
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":99,\"result\":null}\n")
            .await
            .unwrap();
        a2c_agent.flush().await.unwrap();
        assert!(waiter.await.unwrap(), "later response → true");
        assert_eq!(conn.response_seq(), 1);
    }

    /// `has_pending_requests` tracks the in-flight vs settled state of the
    /// correlation map.
    #[tokio::test]
    async fn has_pending_requests_reflects_in_flight_state() {
        let (conn, _c2a_agent, mut a2c_agent) = silent_connection();
        assert!(!conn.has_pending_requests(), "fresh connection: none");

        let mut fut =
            Box::pin(conn.request_timeout("session/ping", json!({}), Duration::from_secs(60)));
        tokio::select! {
            _ = &mut fut => panic!("request must still be pending"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        assert!(conn.has_pending_requests(), "in-flight request: pending");

        a2c_agent
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n")
            .await
            .unwrap();
        a2c_agent.flush().await.unwrap();
        fut.await.expect("request resolves");
        assert!(!conn.has_pending_requests(), "settled request: none");
    }

    /// Agent→client requests bump the client-request watermark; responses
    /// and notifications do not. The bump happens even with no request sink
    /// wired (`ConnectionHooks::default()`), so the watermark is trustworthy
    /// regardless of handler wiring.
    #[tokio::test]
    async fn client_request_seq_counts_only_agent_requests() {
        let (conn, _c2a_agent, mut a2c_agent) = silent_connection();
        assert_eq!(conn.client_request_seq(), 0);

        // A notification (method, no id) does NOT bump it.
        a2c_agent
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{}}\n")
            .await
            .unwrap();
        // A response (id, no method) does NOT bump it.
        a2c_agent
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":42,\"result\":{}}\n")
            .await
            .unwrap();
        // Agent→client requests (id + method) DO bump it.
        a2c_agent
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"fs/write_text_file\",\"params\":{}}\n",
            )
            .await
            .unwrap();
        a2c_agent
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"terminal/create\",\"params\":{}}\n",
            )
            .await
            .unwrap();
        a2c_agent.flush().await.unwrap();

        // Wait for the reader to process all four lines (the response line
        // bumps the response watermark, giving us an ordering fence past
        // line 2; poll briefly for the request lines behind it).
        assert!(conn.await_response_after(0, Duration::from_secs(2)).await);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while conn.client_request_seq() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(conn.client_request_seq(), 2, "exactly the two requests");
        assert_eq!(
            conn.response_seq(),
            1,
            "response watermark untouched by requests"
        );
    }
}

#[cfg(test)]
mod cancel_on_timeout_tests {
    use super::watermark_tests::silent_connection;
    use super::*;
    use serde_json::json;

    fn cancelled(id: i64) -> (String, Value) {
        (
            "notifications/cancelled".to_string(),
            json!({ "requestId": id, "reason": "test" }),
        )
    }

    async fn read_json(reader: &mut BufReader<tokio::io::DuplexStream>) -> Value {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }

    /// A timed-out request is followed on the wire by the cancel notification
    /// naming its id, sent once the pending slot is gone.
    #[tokio::test]
    async fn timed_out_request_sends_cancel_for_its_id() {
        let (conn, c2a_agent, _a2c_agent) = silent_connection();
        let err = conn
            .request_timeout_with_cancel(
                "tools/call",
                json!({ "name": "slow" }),
                Duration::from_millis(50),
                cancelled,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, AcpError::Timeout(_)), "got: {err}");
        assert!(!conn.has_pending_requests(), "slot dropped on timeout");

        let mut reader = BufReader::new(c2a_agent);
        let request = read_json(&mut reader).await;
        assert_eq!(request["method"], json!("tools/call"));
        let id = request["id"].as_i64().unwrap();
        let cancel = read_json(&mut reader).await;
        assert_eq!(cancel["method"], json!("notifications/cancelled"));
        assert!(cancel.get("id").is_none(), "notification: {cancel}");
        assert_eq!(cancel["params"]["requestId"], json!(id));
        assert_eq!(cancel["params"]["reason"], json!("test"));
    }

    /// A peer that has stopped reading its stdin with the writer queue full
    /// must not hold the timed-out caller hostage: the cancel is dropped and
    /// the request returns `Timeout` on the timer's schedule.
    ///
    /// Paused time: the clock only advances when the runtime is idle, so the
    /// 20ms settle below always fires before the request's 200ms timer no
    /// matter how the host schedules the test, and the 2s guard measures the
    /// wedged send by advancing past it rather than by waiting it out.
    #[tokio::test(start_paused = true)]
    async fn timed_out_request_with_full_writer_queue_still_returns_on_time() {
        let (conn, _c2a_agent_unread, _a2c_agent) = silent_connection();
        let mut fut = Box::pin(conn.request_timeout_with_cancel(
            "tools/call",
            json!({}),
            Duration::from_millis(200),
            cancelled,
        ));
        // Let the request line reach the writer before the flood.
        tokio::select! {
            _ = &mut fut => panic!("request must still be pending"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        // Wedge the writer: one line larger than the duplex buffer (which the
        // peer never drains) parks it mid-write, then the bounded channel
        // behind it is filled to capacity. Top off after yielding, since the
        // writer takes one item off the channel before it parks.
        let noise = json!({ "pad": "x".repeat(8192) });
        for _ in 0..8 {
            loop {
                match conn.try_notify("noise", &noise) {
                    Ok(()) => {}
                    Err(AcpError::Transport(msg)) => {
                        assert_eq!(msg, "writer queue full");
                        break;
                    }
                    Err(e) => panic!("unexpected: {e}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            if conn.writer_tx.capacity() == 0 {
                break;
            }
        }
        assert_eq!(conn.writer_tx.capacity(), 0, "writer queue wedged full");

        let outcome = tokio::time::timeout(Duration::from_secs(2), fut).await;
        let err = outcome
            .expect("timed-out call returned within budget")
            .unwrap_err();
        assert!(matches!(err, AcpError::Timeout(_)), "got: {err}");
        assert!(!conn.has_pending_requests(), "slot dropped on timeout");
    }

    /// A request settled by the peer (here with a JSON-RPC error) sends no
    /// cancel: only the request itself is on the wire.
    #[tokio::test]
    async fn answered_request_sends_no_cancel() {
        let (conn, c2a_agent, mut a2c_agent) = silent_connection();
        let mut fut = Box::pin(conn.request_timeout_with_cancel(
            "tools/call",
            json!({}),
            Duration::from_secs(60),
            cancelled,
        ));
        tokio::select! {
            _ = &mut fut => panic!("request must still be pending"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        a2c_agent
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32602,\"message\":\"nope\"}}\n")
            .await
            .unwrap();
        a2c_agent.flush().await.unwrap();
        let err = fut.await.unwrap_err();
        assert!(matches!(err, AcpError::Rpc(_)), "got: {err}");

        let mut reader = BufReader::new(c2a_agent);
        let request = read_json(&mut reader).await;
        assert_eq!(request["id"], json!(1));
        let mut extra = String::new();
        let quiet = tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut extra))
            .await
            .is_err();
        assert!(quiet, "unexpected line after the request: {extra}");
    }
}
