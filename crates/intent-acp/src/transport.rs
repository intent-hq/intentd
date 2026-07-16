//! NDJSON JSON-RPC transport over piped stdio (§6.3).
//!
//! One writer task owns the child's stdin and is fed whole lines through an
//! `mpsc` channel, guaranteeing per-message atomicity (no interleaving even for
//! messages larger than `PIPE_BUF`). A reader task frames stdout on `\n`, parses
//! each line as JSON-RPC, and dispatches: responses → the pending `oneshot` map
//! (keyed per id), agent→client requests → a client-served handler hook, and
//! notifications → a streaming-router hook. A stderr task drains the child's
//! stderr into a bounded ring buffer, flags configured auth-error patterns,
//! and — when a capture dir is configured — appends every line to a
//! daily-rotated per-agent log file (STAB-53).

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
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
    /// are best-effort inside the dedicated stderr task: a failure disables
    /// the file sink but never affects the agent runtime.
    pub stderr_log_dir: Option<PathBuf>,
}

/// Best-effort daily-rotated file sink for the stderr capture (STAB-53).
/// Opens `<dir>/<YYYY-MM-DD>.log` lazily in append mode and reopens when the
/// (UTC) date rolls over. The first write failure disables the sink for the
/// connection's lifetime so a bad disk never loops warnings per line.
struct StderrLogSink {
    dir: PathBuf,
    current: Option<(String, tokio::fs::File)>,
    disabled: bool,
}

impl StderrLogSink {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            current: None,
            disabled: false,
        }
    }

    async fn write_line(&mut self, line: &str) {
        if self.disabled {
            return;
        }
        if let Err(e) = self.try_write_line(line).await {
            tracing::warn!(dir = %self.dir.display(), error = %e, "agent stderr log capture disabled (write failed)");
            self.disabled = true;
        }
    }

    async fn try_write_line(&mut self, line: &str) -> std::io::Result<()> {
        let name = intent_core::current_agent_log_file_name();
        if self.current.as_ref().map(|(n, _)| n.as_str()) != Some(name.as_str()) {
            tokio::fs::create_dir_all(&self.dir).await?;
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.dir.join(&name))
                .await?;
            self.current = Some((name, file));
        }
        let (_, file) = self.current.as_mut().expect("sink file just opened");
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
        Ok(())
    }
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
                        sink.write_line(&line).await;
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
