//! The spawn-time bridge that lets a real spawned child reach the in-process
//! agent→BE MCP server (§6.8).
//!
//! [`serve_workspace_mcp_tcp`] binds a loopback TCP listener and serves a
//! [`WorkspaceMcpServer`] over newline-delimited JSON-RPC (one
//! [`WorkspaceMcpServer::handle_message`] per request line, dispatched
//! concurrently per connection so a long call never blocks the requests
//! behind it). The generated
//! `--mcp-config` points a provider's MCP client at the `intentd mcp-bridge`
//! subcommand, whose body is [`run_stdio_bridge`]: a stdio↔TCP proxy that
//! forwards the child's MCP frames to this listener. This is the Rust analog of
//! the TS `http-mcp-bridge` + `mcp-stdio-server` proxy pair — a real transport,
//! not an in-process `handle_message` shortcut.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::mcp_server::WorkspaceMcpServer;

/// Per-connection cap on concurrently dispatched requests. Small on purpose:
/// enough that a long `tools/call` never blocks a liveness ping behind it
/// (monorepo#871), bounded so a misbehaving client cannot spawn unboundedly.
const MAX_IN_FLIGHT_REQUESTS: usize = 16;

/// Capacity of the per-connection response channel feeding the writer task.
const RESPONSE_CHANNEL_CAPACITY: usize = 32;

/// Dispatch seam for the bridge listener: production is [`WorkspaceMcpServer`];
/// tests inject controllable handlers to exercise concurrency semantics.
pub(crate) trait BridgeDispatch: Send + Sync + 'static {
    fn dispatch(
        self: Arc<Self>,
        message: Value,
    ) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>>;
}

impl BridgeDispatch for WorkspaceMcpServer {
    fn dispatch(
        self: Arc<Self>,
        message: Value,
    ) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>> {
        Box::pin(async move { self.handle_message(&message).await })
    }
}

/// A running per-agent MCP TCP endpoint. Dropping the handle aborts the accept
/// loop, so the listener is torn down with the agent that owns it.
pub struct McpBridge {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl McpBridge {
    /// The bound loopback address (`127.0.0.1:<ephemeral-port>`).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The address as a `host:port` string for the bridge subcommand args.
    pub fn connect_addr(&self) -> String {
        format!("127.0.0.1:{}", self.addr.port())
    }

    /// Test-only: handle to the accept-loop task so tests can await abort
    /// completion deterministically instead of probing the (reusable) port.
    #[cfg(test)]
    pub(crate) fn accept_loop_handle(&self) -> tokio::task::AbortHandle {
        self.task.abort_handle()
    }
}

impl Drop for McpBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Bind a loopback TCP listener and serve `server` over newline-delimited
/// JSON-RPC. Each accepted connection is handled concurrently; a request line
/// yields one response line, a notification (no `id`) yields nothing.
pub async fn serve_workspace_mcp_tcp(
    server: Arc<WorkspaceMcpServer>,
) -> std::io::Result<McpBridge> {
    serve_mcp_tcp(server).await
}

/// Generic body of [`serve_workspace_mcp_tcp`], parameterized over the dispatch
/// seam so tests can serve a mock handler over a real loopback socket.
pub(crate) async fn serve_mcp_tcp<S: BridgeDispatch>(server: Arc<S>) -> std::io::Result<McpBridge> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let server = server.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve_connection(server, stream).await {
                            tracing::debug!(error = %e, "mcp bridge connection ended");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!(error = %e, "mcp bridge accept failed");
                    break;
                }
            }
        }
    });
    Ok(McpBridge { addr, task })
}

/// Serve one accepted MCP connection: read request lines and dispatch each one
/// on its own task so a long `tools/call` never head-of-line blocks a liveness
/// ping behind it (monorepo#871). Responses are funneled through an mpsc
/// channel to a single writer task, so lines are written whole and never
/// interleave; completion order may differ from request order, which is valid
/// id-correlated JSON-RPC. Notifications (no `id`) still produce no response
/// and malformed lines are still skipped.
///
/// Teardown semantics: when the read side ends (EOF or error), all in-flight
/// request tasks are aborted at whatever await point they have reached. This
/// is intentional — the peer can no longer receive the responses, and running
/// orphaned calls to completion would hold their resources with no consumer.
/// The trade-off: combined with the stdio proxy's synthesized retryable
/// [`BRIDGE_DISCONNECTED_CODE`] errors, a provider retry after a TCP blip can
/// re-run a call whose first attempt partially executed before the abort, so
/// non-idempotent tool calls (e.g. mutation scripts) may double-apply the
/// steps that completed before the drop.
async fn serve_connection<S: BridgeDispatch>(
    server: Arc<S>,
    stream: TcpStream,
) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let (response_tx, mut response_rx) = mpsc::channel::<String>(RESPONSE_CHANNEL_CAPACITY);
    let writer = tokio::spawn(async move {
        while let Some(line) = response_rx.recv().await {
            if write.write_all(line.as_bytes()).await.is_err() || write.flush().await.is_err() {
                break;
            }
        }
    });

    let limiter = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS));
    let mut in_flight = JoinSet::new();
    let mut lines = BufReader::new(read).lines();
    let result = loop {
        tokio::select! {
            // Reap finished request tasks as they complete — not only when
            // the next line arrives — so an idle connection does not
            // accumulate finished handles after a burst.
            joined = in_flight.join_next(), if !in_flight.is_empty() => {
                let _ = joined;
            }
            line = lines.next_line() => match line {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let Ok(message) = serde_json::from_str::<Value>(&line) else {
                        continue;
                    };
                    let permit = limiter
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("bridge semaphore is never closed");
                    let server = server.clone();
                    let response_tx = response_tx.clone();
                    in_flight.spawn(async move {
                        let _permit = permit;
                        if let Some(response) = server.dispatch(message).await {
                            let _ = response_tx.send(format!("{response}\n")).await;
                        }
                    });
                }
                Ok(None) => break Ok(()),
                Err(e) => break Err(e),
            },
        }
    };
    // Connection teardown: abort in-flight request tasks, then let the writer
    // drain and exit once every sender is gone.
    in_flight.shutdown().await;
    drop(response_tx);
    let _ = writer.await;
    result
}

/// JSON-RPC error code (implementation-defined `-32000`-range server error)
/// synthesized by the bridge for requests it cannot deliver while the TCP side
/// is disconnected. The message marks it clearly transient so a provider's MCP
/// client can retry instead of treating the tool as broken.
pub(crate) const BRIDGE_DISCONNECTED_CODE: i64 = -32001;

/// Human-readable companion to [`BRIDGE_DISCONNECTED_CODE`].
pub(crate) const BRIDGE_DISCONNECTED_MESSAGE: &str =
    "workspace-mcp bridge temporarily disconnected; retry";

/// Max stdin lines buffered during the initial connect window (monorepo#908).
/// The window is ~5s and a well-behaved client sends a handful of lines; the
/// cap only guards against a flooding client growing memory unboundedly.
/// Overflowing id-carrying requests fall back to the retryable disconnected
/// error.
pub(crate) const INITIAL_BUFFER_MAX_LINES: usize = 1024;

/// Companion byte cap for the initial-window stdin buffer.
const INITIAL_BUFFER_MAX_BYTES: usize = 1024 * 1024;

/// Retry/backoff knobs for [`run_stdio_bridge`]. Defaults give the initial
/// connect ~10 attempts over ~5s and mid-session reconnects a ~30s total
/// window; tests shrink these to keep runs fast.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BridgeRetryConfig {
    /// Max attempts for the initial connect before giving up with an error.
    pub initial_attempts: u32,
    /// Total time budget for mid-session reconnects. A restarted daemon binds
    /// a new (unknowable) port, so past this window the bridge exits cleanly.
    pub reconnect_window: Duration,
    /// First backoff delay; doubled after each failed attempt.
    pub backoff_start: Duration,
    /// Upper bound on the (doubling) backoff delay.
    pub backoff_cap: Duration,
}

impl Default for BridgeRetryConfig {
    fn default() -> Self {
        Self {
            initial_attempts: 10,
            reconnect_window: Duration::from_secs(30),
            backoff_start: Duration::from_millis(50),
            backoff_cap: Duration::from_secs(1),
        }
    }
}

/// How one connected pump session ended.
enum SessionEnd {
    /// The stdio side reached EOF: the provider is gone, exit cleanly.
    StdinEof,
    /// The TCP side dropped: attempt a reconnect.
    TcpDropped,
}

/// Body of the `intentd mcp-bridge --connect <addr>` subcommand: connect to a
/// per-agent listener (see [`serve_workspace_mcp_tcp`]) and pump stdin lines to
/// the socket and socket lines to stdout, giving a spawned provider a real stdio
/// MCP server that proxies to the in-process workspace tools.
///
/// Resilience (monorepo#871, monorepo#908): the initial connect is retried
/// with bounded backoff while stdin lines (notably the MCP `initialize`
/// handshake) are buffered and forwarded in order once the connect succeeds,
/// so a startup race never surfaces an error to the provider; on give-up the
/// buffered requests are never answered and the bridge exits with the connect
/// error instead. A mid-session TCP drop keeps the stdio side alive while the
/// bridge reconnects to the same address. While reconnecting, each stdin
/// request that carries an `id` is answered with a retryable JSON-RPC error
/// ([`BRIDGE_DISCONNECTED_CODE`]) instead of being dropped, and requests that
/// were in flight when the connection died get the same synthesized error so
/// the provider's MCP client never has to time out.
pub async fn run_stdio_bridge(addr: &str) -> std::io::Result<()> {
    run_bridge(
        addr,
        tokio::io::stdin(),
        tokio::io::stdout(),
        BridgeRetryConfig::default(),
    )
    .await
}

/// Generic body of [`run_stdio_bridge`], parameterized over the stdio pair so
/// tests can drive the bridge with in-memory duplex streams.
pub(crate) async fn run_bridge<R, W>(
    addr: &str,
    input: R,
    mut output: W,
    cfg: BridgeRetryConfig,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut input = BufReader::new(input).lines();
    let mut initial = true;
    let mut buffered: Vec<String> = Vec::new();
    loop {
        let stream =
            match connect_with_retry(addr, cfg, initial, &mut buffered, &mut input, &mut output)
                .await?
            {
                Some(stream) => stream,
                None => return Ok(()),
            };
        initial = false;
        match pump_session(
            stream,
            std::mem::take(&mut buffered),
            &mut input,
            &mut output,
        )
        .await?
        {
            SessionEnd::StdinEof => return Ok(()),
            SessionEnd::TcpDropped => {
                tracing::warn!(%addr, "mcp bridge connection dropped; reconnecting");
            }
        }
    }
}

/// Connect to `addr` with bounded backoff. While waiting between attempts,
/// keep servicing stdin. During the initial window (`initial == true`) lines
/// are buffered into `buffer` for forwarding once the connect succeeds
/// (monorepo#908), bounded by [`INITIAL_BUFFER_MAX_LINES`] /
/// [`INITIAL_BUFFER_MAX_BYTES`]; past the caps — or during a mid-session
/// reconnect — requests with an `id` are answered with the retryable
/// disconnected error and notifications are dropped.
///
/// Returns `Ok(Some(stream))` on success. On give-up, the initial connect
/// surfaces the last error (the daemon was never reachable; buffered requests
/// are never answered — the caller exits non-zero instead), while a reconnect
/// returns `Ok(None)` so the bridge exits cleanly — a restarted daemon listens
/// on a new port this bridge can never learn. `Ok(None)` is also returned when
/// stdin reaches EOF while disconnected.
async fn connect_with_retry<R, W>(
    addr: &str,
    cfg: BridgeRetryConfig,
    initial: bool,
    buffer: &mut Vec<String>,
    input: &mut Lines<BufReader<R>>,
    output: &mut W,
) -> std::io::Result<Option<TcpStream>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let deadline = Instant::now() + cfg.reconnect_window;
    let mut delay = cfg.backoff_start;
    let mut attempts: u32 = 0;
    let mut buffered_bytes: usize = buffer.iter().map(String::len).sum();
    loop {
        attempts += 1;
        // Service stdin while the connect itself is pending (not just during
        // the backoff sleep) so a blackholed address cannot leave requests
        // unserviced for the OS connect timeout. `biased` polls the connect
        // first: a request racing with a completed connect is forwarded on
        // the fresh connection instead of being spuriously buffered/rejected.
        let connect = TcpStream::connect(addr);
        tokio::pin!(connect);
        let connected = loop {
            tokio::select! {
                biased;
                result = &mut connect => break result,
                line = input.next_line() => match line? {
                    Some(line) => {
                        buffer_or_reject(line, initial, buffer, &mut buffered_bytes, output)
                            .await?
                    }
                    None => return Ok(None),
                },
            }
        };
        match connected {
            Ok(stream) => return Ok(Some(stream)),
            Err(e) => {
                if initial {
                    if attempts >= cfg.initial_attempts {
                        tracing::warn!(%addr, error = %e, attempts, "mcp bridge initial connect failed; giving up");
                        return Err(e);
                    }
                } else if Instant::now() >= deadline {
                    tracing::warn!(%addr, error = %e, "mcp bridge reconnect window exhausted; exiting");
                    return Ok(None);
                }
                tracing::debug!(%addr, error = %e, attempts, "mcp bridge connect failed; backing off");
            }
        }
        // Back off before the next attempt, servicing stdin in the meantime:
        // initial-window lines keep buffering, mid-session requests get an
        // immediate retryable error.
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                line = input.next_line() => match line? {
                    Some(line) => {
                        buffer_or_reject(line, initial, buffer, &mut buffered_bytes, output)
                            .await?
                    }
                    None => return Ok(None),
                },
            }
        }
        delay = (delay * 2).min(cfg.backoff_cap);
    }
}

/// Handle one stdin line received while disconnected: during the initial
/// connect window the line is buffered for forwarding after the connect
/// succeeds (monorepo#908); past the defensive caps — or during a mid-session
/// reconnect — id-carrying requests fall back to the retryable disconnected
/// error.
async fn buffer_or_reject<W: AsyncWrite + Unpin>(
    line: String,
    initial: bool,
    buffer: &mut Vec<String>,
    buffered_bytes: &mut usize,
    output: &mut W,
) -> std::io::Result<()> {
    if initial
        && buffer.len() < INITIAL_BUFFER_MAX_LINES
        && *buffered_bytes + line.len() <= INITIAL_BUFFER_MAX_BYTES
    {
        *buffered_bytes += line.len();
        buffer.push(line);
        return Ok(());
    }
    reject_if_request(&line, output).await
}

/// Pump one connected session: stdin lines → socket, socket lines → stdout.
/// Lines buffered during the initial connect window are flushed to the socket
/// first, in order, before live traffic is pumped (monorepo#908). Tracks the
/// `id` of every forwarded request until its response comes back; when the
/// TCP side drops, every still-pending id gets the synthesized retryable
/// error so the provider client is never left waiting.
async fn pump_session<R, W>(
    stream: TcpStream,
    buffered: Vec<String>,
    input: &mut Lines<BufReader<R>>,
    output: &mut W,
) -> std::io::Result<SessionEnd>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (tcp_read, mut tcp_write) = stream.into_split();
    let mut tcp_lines = BufReader::new(tcp_read).lines();
    // Requests forwarded but not yet answered, keyed by the id's canonical
    // JSON so numeric and string ids never collide.
    let mut pending: HashMap<String, Value> = HashMap::new();
    // Buffered ids are registered for the whole batch up front: once the
    // flush begins the session owns these requests, so a TCP drop mid-flush
    // synthesizes the retryable error for the not-yet-written ones too.
    for line in &buffered {
        if let Some(id) = request_id(line) {
            pending.insert(id.to_string(), id);
        }
    }
    let mut flush_dropped = false;
    for line in &buffered {
        if line.trim().is_empty() {
            continue;
        }
        if write_line(&mut tcp_write, line).await.is_err() {
            flush_dropped = true;
            break;
        }
    }
    let end = if flush_dropped {
        SessionEnd::TcpDropped
    } else {
        loop {
            tokio::select! {
                line = input.next_line() => match line? {
                    Some(line) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        if let Some(id) = request_id(&line) {
                            pending.insert(id.to_string(), id);
                        }
                        if write_line(&mut tcp_write, &line).await.is_err() {
                            break SessionEnd::TcpDropped;
                        }
                    }
                    None => break SessionEnd::StdinEof,
                },
                line = tcp_lines.next_line() => match line {
                    Ok(Some(line)) => {
                        if let Some(id) = response_id(&line) {
                            pending.remove(&id.to_string());
                        }
                        write_line(output, &line).await?;
                    }
                    Ok(None) | Err(_) => break SessionEnd::TcpDropped,
                },
            }
        }
    };
    if matches!(end, SessionEnd::TcpDropped) {
        for id in pending.into_values() {
            write_disconnected_error(output, &id).await?;
        }
    }
    Ok(end)
}

/// The `id` of a stdin line that is a JSON-RPC *request* (has `method` + `id`).
/// Notifications, client→server responses, and malformed lines yield `None`.
fn request_id(line: &str) -> Option<Value> {
    let msg: Value = serde_json::from_str(line).ok()?;
    if msg.get("method").is_some() {
        msg.get("id").cloned()
    } else {
        None
    }
}

/// The `id` of a socket line that is a JSON-RPC *response* (has `id`, no
/// `method`). Server-initiated requests and malformed lines yield `None`.
fn response_id(line: &str) -> Option<Value> {
    let msg: Value = serde_json::from_str(line).ok()?;
    if msg.get("method").is_none() {
        msg.get("id").cloned()
    } else {
        None
    }
}

/// If `line` is a request with an id, answer it on `output` with the retryable
/// disconnected error; anything else (notification, response, malformed) is
/// dropped, matching connected-path semantics for unanswerable input.
async fn reject_if_request<W: AsyncWrite + Unpin>(
    line: &str,
    output: &mut W,
) -> std::io::Result<()> {
    match request_id(line) {
        Some(id) => write_disconnected_error(output, &id).await,
        None => Ok(()),
    }
}

/// Write the synthesized retryable JSON-RPC error for `id` to `output`.
async fn write_disconnected_error<W: AsyncWrite + Unpin>(
    output: &mut W,
    id: &Value,
) -> std::io::Result<()> {
    let response = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": BRIDGE_DISCONNECTED_CODE,
            "message": BRIDGE_DISCONNECTED_MESSAGE,
            "data": { "retryable": true },
        },
    });
    write_line(output, &response.to_string()).await
}

/// Write one newline-terminated line and flush.
async fn write_line<W: AsyncWrite + Unpin>(writer: &mut W, line: &str) -> std::io::Result<()> {
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}
