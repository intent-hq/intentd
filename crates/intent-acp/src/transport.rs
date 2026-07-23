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
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::error::{AcpError, AcpResult, JsonRpcError};

/// Default per-request timeout (mirrors the TS initialize timeout, §6.4).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of recent stderr entries retained (parity:
/// `MAX_RECENT_STDERR_ERRORS`).
const MAX_RECENT_STDERR_ERRORS: usize = 5;
/// Maximum characters retained per stderr entry (parity:
/// `MAX_RECENT_STDERR_ENTRY_CHARS`).
const MAX_RECENT_STDERR_ENTRY_CHARS: usize = 10_000;
/// Outbound writer channel capacity.
const WRITER_CHANNEL_CAPACITY: usize = 256;

type PendingMap = Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, JsonRpcError>>>>>;

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
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.drop_warned {
                    self.drop_warned = true;
                    tracing::warn!("agent stderr log capture dropping lines (writer backlogged)");
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
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
fn dispatch(
    value: Value,
    pending: &PendingMap,
    requests: &Option<mpsc::UnboundedSender<IncomingRequest>>,
    notifications: &Option<mpsc::UnboundedSender<IncomingNotification>>,
) {
    let Some(obj) = value.as_object() else { return };
    let method = obj.get("method").and_then(|m| m.as_str());
    let id = obj.get("id").cloned().filter(|v| !v.is_null());

    if let Some(method) = method {
        let method = method.to_string();
        let params = obj.get("params").cloned().unwrap_or(Value::Null);
        match id {
            Some(id) => {
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
    stderr: Arc<Mutex<StderrBuffer>>,
    auth_error: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Connection {
    /// Wire up the writer/reader/stderr tasks around a child's piped stdio.
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
        let requests = hooks.requests;
        let notifications = hooks.notifications;
        tasks.push(tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(&line) {
                    Ok(value) => dispatch(value, &pending_reader, &requests, &notifications),
                    Err(e) => tracing::warn!(error = %e, "failed to parse ACP stdout line"),
                }
            }
            // stdout closed: fail every still-pending request.
            let mut map = pending_reader.lock().unwrap();
            for (_, sender) in map.drain() {
                let _ = sender.send(Err(JsonRpcError {
                    code: 0,
                    message: "agent stdout closed".to_string(),
                    data: None,
                }));
            }
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
            stderr: stderr_buf,
            auth_error,
            tasks,
        }
    }

    /// Send a request and await its response with the default timeout (§6.4).
    pub async fn request(&self, method: &str, params: Value) -> AcpResult<Value> {
        self.request_timeout(method, params, DEFAULT_REQUEST_TIMEOUT)
            .await
    }

    /// Send a request and await its response with an explicit timeout.
    pub async fn request_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> AcpResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);

        let line = encode_message(Some(id), method, &params)?;
        if self.writer_tx.send(line).await.is_err() {
            self.pending.lock().unwrap().remove(&id);
            return Err(AcpError::Transport("writer task closed".to_string()));
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(err))) => Err(AcpError::Rpc(err)),
            Ok(Err(_)) => Err(AcpError::Transport("response channel dropped".to_string())),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(AcpError::Timeout(method.to_string()))
            }
        }
    }

    /// Send a notification (no id, no response).
    pub async fn notify(&self, method: &str, params: Value) -> AcpResult<()> {
        let line = encode_message(None, method, &params)?;
        self.writer_tx
            .send(line)
            .await
            .map_err(|_| AcpError::Transport("writer task closed".to_string()))
    }

    /// Send a successful response to an agent→client request.
    pub async fn respond_result(&self, id: Value, result: Value) -> AcpResult<()> {
        let msg = serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result });
        let line = format!("{}\n", serde_json::to_string(&msg)?);
        self.writer_tx
            .send(line)
            .await
            .map_err(|_| AcpError::Transport("writer task closed".to_string()))
    }

    /// Send an error response to an agent→client request.
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
    pub fn recent_stderr(&self) -> Vec<String> {
        self.stderr.lock().unwrap().recent()
    }

    /// Whether a configured auth-error pattern has been seen on stderr.
    pub fn auth_error_detected(&self) -> bool {
        self.auth_error.load(Ordering::SeqCst)
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
