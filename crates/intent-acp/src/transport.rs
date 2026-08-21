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
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;

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
    drop_warned: bool,
}

impl StderrLogSink {
    fn new(dir: PathBuf) -> Self {
        let (tx, rx) = mpsc::channel::<String>(STDERR_LOG_CHANNEL_CAPACITY);
        tokio::spawn(stderr_log_writer(dir, rx));
        Self {
            tx,
            drop_warned: false,
        }
    }

    /// Hand a line to the writer task without ever awaiting: on a full
    /// channel (stalled disk) or a closed one (writer exited after an I/O
    /// error) the line is dropped — capture is best-effort by design.
    fn send_line(&mut self, line: &str) {
        match self.tx.try_send(line.to_string()) {
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.drop_warned {
                    self.drop_warned = true;
                    tracing::warn!("agent stderr log capture dropping lines (writer backlogged)");
                }
            }
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
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
    requests: &Option<mpsc::UnboundedSender<IncomingRequest>>,
    notifications: &Option<mpsc::UnboundedSender<IncomingNotification>>,
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
/// Owns the writer/reader/stderr tasks and the pending-request correlation map.
/// All outbound traffic is serialized through a single writer task; inbound
/// traffic is routed by [`dispatch`]. Dropping the connection aborts its tasks.
pub struct Connection {
    writer_tx: mpsc::Sender<String>,
    pending: PendingMap,
    next_id: AtomicI64,
    response_seq: Arc<AtomicU64>,
    response_notify: Arc<Notify>,
    client_request_seq: Arc<AtomicU64>,
    stderr: Arc<Mutex<StderrBuffer>>,
    auth_error: Arc<AtomicBool>,
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
        let response_seq = Arc::new(AtomicU64::new(0));
        let response_notify = Arc::new(Notify::new());
        let client_request_seq = Arc::new(AtomicU64::new(0));
        let stderr_buf = Arc::new(Mutex::new(StderrBuffer::default()));
        let auth_error = Arc::new(AtomicBool::new(false));
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
        let seq_reader = Arc::clone(&response_seq);
        let notify_reader = Arc::clone(&response_notify);
        let client_req_seq_reader = Arc::clone(&client_request_seq);
        let requests = hooks.requests;
        let notifications = hooks.notifications;
        tasks.push(tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(&line) {
                    Ok(value) => dispatch(
                        &value,
                        &pending_reader,
                        &requests,
                        &notifications,
                        &seq_reader,
                        &notify_reader,
                        &client_req_seq_reader,
                    ),
                    Err(e) => tracing::warn!(error = %e, "failed to parse ACP stdout line"),
                }
            }
            // stdout closed: fail every still-pending request.
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

        // Stderr task: ring-buffer recent lines, flag auth-error patterns, and
        // append every raw line to the per-agent capture file when configured.
        if let Some(stderr) = stderr {
            let stderr_buf_task = Arc::clone(&stderr_buf);
            let auth_flag = Arc::clone(&auth_error);
            let patterns: Vec<String> = hooks
                .auth_error_patterns
                .iter()
                .map(|p| p.to_lowercase())
                .collect();
            let mut log_sink = hooks.stderr_log_dir.map(StderrLogSink::new);
            tasks.push(tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(sink) = log_sink.as_mut() {
                        sink.send_line(&line);
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
            }));
        }

        Self {
            writer_tx,
            pending,
            next_id: AtomicI64::new(1),
            response_seq,
            response_notify,
            client_request_seq,
            stderr: stderr_buf,
            auth_error,
            tasks,
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
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        // Drop-guard cleanup: covers the error/timeout arms below AND the
        // caller dropping this future mid-flight (cancel-safety for the
        // pending map — see `PendingEntryGuard`).
        let _guard = PendingEntryGuard {
            pending: Arc::clone(&self.pending),
            id,
        };

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
}

impl Drop for Connection {
    fn drop(&mut self) {
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
mod watermark_tests {
    use super::*;
    use serde_json::json;

    /// A duplex-backed `Connection` whose "agent" never responds on its own:
    /// the test holds both remote ends and writes response lines by hand.
    fn silent_connection() -> (Connection, tokio::io::DuplexStream, tokio::io::DuplexStream) {
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
