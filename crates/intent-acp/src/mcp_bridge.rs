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

/// Watchdog deadline for a single dispatched request (monorepo#2709). The
/// dispatch future runs in its own task and the deadline is enforced from the
/// per-request task via `select!`, so it fires even when the dispatch is
/// wedged inside a single synchronous poll — the state an in-dispatch
/// `tokio::time::timeout` (like the 30s `intent-js` eval budget) can never
/// escape, because the wedged task never yields to let its timer fire.
/// Generously above that 30s eval budget plus the post-eval awaits, so the
/// watchdog only fires when the normal timeout machinery has already failed.
const DISPATCH_WATCHDOG_TIMEOUT: Duration = Duration::from_secs(120);

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
    serve_mcp_tcp_with_timeout(server, DISPATCH_WATCHDOG_TIMEOUT).await
}

/// [`serve_mcp_tcp`] with an injectable dispatch watchdog deadline, so tests
/// can shorten [`DISPATCH_WATCHDOG_TIMEOUT`] and exercise the timeout path
/// without waiting minutes.
pub(crate) async fn serve_mcp_tcp_with_timeout<S: BridgeDispatch>(
    server: Arc<S>,
    dispatch_timeout: Duration,
) -> std::io::Result<McpBridge> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let server = server.clone();
                    tokio::spawn(async move {
                        if let Err(e) = serve_connection(server, stream, dispatch_timeout).await {
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
/// An aborted call may thus have partially executed, which is why the stdio
/// proxy answers requests it already delivered with the non-retryable
/// [`BRIDGE_OUTCOME_UNKNOWN_CODE`] instead of the retryable
/// [`BRIDGE_DISCONNECTED_CODE`] (monorepo#1530): a blind provider retry after
/// a TCP blip would re-run a call whose first attempt partially executed
/// before the abort, double-applying the steps that completed before the
/// drop.
///
/// Dispatch watchdog (monorepo#2709): each dispatch is spawned into its own
/// task and the per-request task `select!`s its `JoinHandle` against
/// `dispatch_timeout`. Because the watchdog polls in a different task than
/// the dispatch, it fires even when the dispatch future is wedged inside a
/// single synchronous poll. On deadline the dispatch task is aborted and, for
/// requests carrying an `id`, a [`BRIDGE_DISPATCH_TIMEOUT_CODE`] error line
/// is synthesized so the peer never waits out its own client timeout;
/// notifications get only the abort.
async fn serve_connection<S: BridgeDispatch>(
    server: Arc<S>,
    stream: TcpStream,
    dispatch_timeout: Duration,
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
                        let id = message.get("id").cloned().filter(|id| !id.is_null());
                        let method = message
                            .get("method")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let started = Instant::now();
                        // The dispatch runs in its own task so the watchdog
                        // below still gets polled when the dispatch future
                        // wedges inside a synchronous poll; the guard aborts
                        // it if this request task is itself torn down.
                        let mut dispatch = AbortOnDrop(tokio::spawn(server.dispatch(message)));
                        tokio::select! {
                            joined = &mut dispatch.0 => match joined {
                                Ok(Some(response)) => {
                                    let _ = response_tx.send(format!("{response}\n")).await;
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    tracing::warn!(%method, error = %e, "mcp bridge dispatch task failed");
                                }
                            },
                            _ = tokio::time::sleep(dispatch_timeout) => {
                                dispatch.0.abort();
                                tracing::warn!(
                                    %method,
                                    elapsed_ms = started.elapsed().as_millis() as u64,
                                    "mcp bridge dispatch exceeded watchdog deadline; aborted and synthesized timeout error"
                                );
                                if let Some(id) = id {
                                    let response = json!({
                                        "jsonrpc": "2.0",
                                        "id": id,
                                        "error": {
                                            "code": BRIDGE_DISPATCH_TIMEOUT_CODE,
                                            "message": BRIDGE_DISPATCH_TIMEOUT_MESSAGE,
                                            "data": { "retryable": false },
                                        },
                                    });
                                    let _ = response_tx.send(format!("{response}\n")).await;
                                }
                            }
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

/// Aborts the wrapped dispatch task on drop, so a per-request task torn down
/// at connection teardown (or by the watchdog path exiting) never leaks a
/// still-running dispatch: dropping a bare `JoinHandle` would detach it.
struct AbortOnDrop(JoinHandle<Option<Value>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// JSON-RPC error code (implementation-defined `-32000`-range server error)
/// synthesized by the bridge for requests it cannot deliver while the TCP side
/// is disconnected. The message marks it clearly transient so a provider's MCP
/// client can retry instead of treating the tool as broken.
pub(crate) const BRIDGE_DISCONNECTED_CODE: i64 = -32001;

/// Human-readable companion to [`BRIDGE_DISCONNECTED_CODE`].
pub(crate) const BRIDGE_DISCONNECTED_MESSAGE: &str =
    "workspace-mcp bridge temporarily disconnected; retry";

/// JSON-RPC error code (implementation-defined `-32000`-range server error)
/// synthesized by the bridge for requests that had already been delivered to
/// the listener when the TCP side dropped (monorepo#1530). The listener may
/// have executed the call — partially or fully — before the drop, so the
/// error is explicitly non-retryable: blindly re-running a non-idempotent
/// `workspace_api` call could double-apply its side effects.
pub(crate) const BRIDGE_OUTCOME_UNKNOWN_CODE: i64 = -32002;

/// Human-readable companion to [`BRIDGE_OUTCOME_UNKNOWN_CODE`].
pub(crate) const BRIDGE_OUTCOME_UNKNOWN_MESSAGE: &str =
    "workspace-mcp call was delivered but its outcome is unknown after a disconnect; do not blindly retry";

/// JSON-RPC error code (implementation-defined `-32000`-range server error)
/// synthesized by the listener when a dispatch exceeds the watchdog deadline
/// (monorepo#2709, see [`DISPATCH_WATCHDOG_TIMEOUT`]). The dispatch task is
/// aborted mid-execution, so like [`BRIDGE_OUTCOME_UNKNOWN_CODE`] the call
/// may have partially executed and the error is non-retryable.
pub(crate) const BRIDGE_DISPATCH_TIMEOUT_CODE: i64 = -32003;

/// Human-readable companion to [`BRIDGE_DISPATCH_TIMEOUT_CODE`].
pub(crate) const BRIDGE_DISPATCH_TIMEOUT_MESSAGE: &str =
    "workspace-mcp dispatch timed out daemon-side; the call may have partially executed — do not blindly retry";

/// Max stdin lines buffered during the initial connect window (monorepo#908).
/// The window is ~5s and a well-behaved client sends a handful of lines; the
/// cap only guards against a flooding client growing memory unboundedly.
/// Overflowing id-carrying requests fall back to the retryable disconnected
/// error.
pub(crate) const INITIAL_BUFFER_MAX_LINES: usize = 1024;

/// Companion byte cap for the initial-window stdin buffer.
pub(crate) const INITIAL_BUFFER_MAX_BYTES: usize = 1024 * 1024;

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
    /// Per-attempt cap on how long a single connect may stay pending. The
    /// bridge only ever dials loopback, where connects resolve immediately,
    /// so this is a defensive bound: it keeps a pathologically hanging
    /// attempt from stalling held stdin lines (monorepo#906) or the
    /// `reconnect_window` deadline for the OS connect timeout.
    pub connect_timeout: Duration,
}

impl Default for BridgeRetryConfig {
    fn default() -> Self {
        Self {
            initial_attempts: 10,
            reconnect_window: Duration::from_secs(30),
            backoff_start: Duration::from_millis(50),
            backoff_cap: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(1),
        }
    }
}

/// How one connected pump session ended.
pub(crate) enum SessionEnd {
    /// The stdio side reached EOF: the provider is gone, exit cleanly.
    StdinEof,
    /// The TCP side dropped: attempt a reconnect.
    TcpDropped,
}

/// A forwarded-but-unanswered request id, tagged with whether its line was
/// successfully written to the TCP socket — i.e. delivered to the listener,
/// which may have executed it (monorepo#1530).
struct PendingRequest {
    id: Value,
    delivered: bool,
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
/// ([`BRIDGE_DISCONNECTED_CODE`]) instead of being dropped — except for lines
/// that race a still-pending connect attempt, which are held until that
/// attempt's outcome is known (monorepo#906). Requests that were pending when
/// the connection died are classified by delivery (monorepo#1530): ids never
/// written to the socket get the same retryable error, while ids already
/// delivered to the listener get the non-retryable
/// [`BRIDGE_OUTCOME_UNKNOWN_CODE`] — the listener may have executed them
/// before the drop, so a blind retry could double-apply. Either way the
/// provider's MCP client never has to time out.
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
    output: W,
    cfg: BridgeRetryConfig,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let target = addr.to_string();
    run_bridge_with(addr, input, output, cfg, move || {
        let target = target.clone();
        async move { TcpStream::connect(&target).await }
    })
    .await
}

/// [`run_bridge`] with an injectable connector, so tests can control exactly
/// when a connect attempt resolves and deterministically exercise the
/// line-races-pending-connect window (monorepo#906).
pub(crate) async fn run_bridge_with<R, W, C, Fut>(
    addr: &str,
    input: R,
    mut output: W,
    cfg: BridgeRetryConfig,
    mut connect: C,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    C: FnMut() -> Fut,
    Fut: Future<Output = std::io::Result<TcpStream>>,
{
    let mut input = BufReader::new(input).lines();
    let mut initial = true;
    let mut buffered: Vec<String> = Vec::new();
    loop {
        let stream = match connect_with_retry(
            addr,
            cfg,
            initial,
            &mut buffered,
            &mut input,
            &mut output,
            &mut connect,
        )
        .await?
        {
            Some(stream) => stream,
            None => return Ok(()),
        };
        initial = false;
        let (tcp_read, tcp_write) = stream.into_split();
        match pump_session(
            tcp_read,
            tcp_write,
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

/// Connect via `connect` with bounded backoff. While waiting between
/// attempts, keep servicing stdin. During the initial window
/// (`initial == true`) lines are buffered into `buffer` for forwarding once
/// the connect succeeds (monorepo#908), bounded by
/// [`INITIAL_BUFFER_MAX_LINES`] / [`INITIAL_BUFFER_MAX_BYTES`]; past the caps
/// — or during a mid-session reconnect backoff — requests with an `id` are
/// answered with the retryable disconnected error and notifications are
/// dropped.
///
/// Mid-session lines that race a *pending* connect attempt are held until
/// that attempt's outcome is known (monorepo#906): tokio only refreshes IO
/// readiness on a driver turn, so a stdin wake can observe the connect as
/// stale-`Pending` even though the kernel already established it — rejecting
/// on that observation would spuriously `-32001` a request the fresh
/// connection could serve. On success held lines join `buffer` (flushed in
/// order by the session); on failure they get the retryable error then.
///
/// Returns `Ok(Some(stream))` on success. On give-up, the initial connect
/// surfaces the last error (the daemon was never reachable; buffered requests
/// are never answered — the caller exits non-zero instead), while a reconnect
/// returns `Ok(None)` so the bridge exits cleanly — a restarted daemon listens
/// on a new port this bridge can never learn. `Ok(None)` is also returned when
/// stdin reaches EOF while disconnected; any buffered or held lines are
/// dropped unanswered — the provider closed stdin, so nothing is awaiting
/// responses.
async fn connect_with_retry<R, W, C, Fut>(
    addr: &str,
    cfg: BridgeRetryConfig,
    initial: bool,
    buffer: &mut Vec<String>,
    input: &mut Lines<BufReader<R>>,
    output: &mut W,
    connect: &mut C,
) -> std::io::Result<Option<TcpStream>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    C: FnMut() -> Fut,
    Fut: Future<Output = std::io::Result<TcpStream>>,
{
    let deadline = Instant::now() + cfg.reconnect_window;
    let mut delay = cfg.backoff_start;
    let mut attempts: u32 = 0;
    let mut buffered_bytes: usize = buffer.iter().map(String::len).sum();
    let mut overflowed = false;
    loop {
        attempts += 1;
        // Service stdin while the connect itself is pending, holding lines
        // for the attempt's outcome (see above) instead of rejecting them
        // against possibly stale readiness (monorepo#906). `connect_timeout`
        // bounds the attempt so a blackholed address cannot stall held lines
        // (or the reconnect deadline) for the OS connect timeout.
        let mut held: Vec<String> = Vec::new();
        let mut held_bytes: usize = 0;
        let attempt = tokio::time::timeout(cfg.connect_timeout, connect());
        tokio::pin!(attempt);
        let connected = loop {
            tokio::select! {
                biased;
                result = &mut attempt => break result.unwrap_or_else(|_| {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "mcp bridge connect attempt timed out",
                    ))
                }),
                line = input.next_line() => match line? {
                    Some(line) => {
                        if initial {
                            buffer_or_reject(
                                line,
                                initial,
                                buffer,
                                &mut buffered_bytes,
                                &mut overflowed,
                                output,
                            )
                            .await?
                        } else if !overflowed
                            && held.len() < INITIAL_BUFFER_MAX_LINES
                            && held_bytes + line.len() <= INITIAL_BUFFER_MAX_BYTES
                        {
                            held_bytes += line.len();
                            held.push(line);
                        } else {
                            overflowed = true;
                            reject_if_request(&line, output).await?;
                        }
                    }
                    None => return Ok(None),
                },
            }
        };
        match connected {
            Ok(stream) => {
                buffer.append(&mut held);
                return Ok(Some(stream));
            }
            Err(e) => {
                for line in held.drain(..) {
                    reject_if_request(&line, output).await?;
                }
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
                        buffer_or_reject(
                            line,
                            initial,
                            buffer,
                            &mut buffered_bytes,
                            &mut overflowed,
                            output,
                        )
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
/// error. Overflow is sticky: once any line is rejected, every later line is
/// rejected too, so a later request can never be served after an earlier one
/// failed (and buffered notifications are never delivered out of order
/// relative to dropped ones).
async fn buffer_or_reject<W: AsyncWrite + Unpin>(
    line: String,
    initial: bool,
    buffer: &mut Vec<String>,
    buffered_bytes: &mut usize,
    overflowed: &mut bool,
    output: &mut W,
) -> std::io::Result<()> {
    if initial
        && !*overflowed
        && buffer.len() < INITIAL_BUFFER_MAX_LINES
        && *buffered_bytes + line.len() <= INITIAL_BUFFER_MAX_BYTES
    {
        *buffered_bytes += line.len();
        buffer.push(line);
        return Ok(());
    }
    if initial {
        *overflowed = true;
    }
    reject_if_request(&line, output).await
}

/// Pump one connected session: stdin lines → socket, socket lines → stdout.
/// Lines buffered during the initial connect window are flushed to the socket
/// first, in order, before live traffic is pumped (monorepo#908). Tracks the
/// `id` of every forwarded request until its response comes back, along with
/// whether its line was successfully written to the socket; when the TCP side
/// drops, every still-pending id is answered so the provider client is never
/// left waiting — delivered ids with the non-retryable
/// [`BRIDGE_OUTCOME_UNKNOWN_CODE`] (the listener may have executed them,
/// monorepo#1530), undelivered ids with the retryable
/// [`BRIDGE_DISCONNECTED_CODE`].
pub(crate) async fn pump_session<TR, TW, R, W>(
    tcp_read: TR,
    mut tcp_write: TW,
    buffered: Vec<String>,
    input: &mut Lines<BufReader<R>>,
    output: &mut W,
) -> std::io::Result<SessionEnd>
where
    TR: AsyncRead + Unpin,
    TW: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut tcp_lines = BufReader::new(tcp_read).lines();
    // Requests forwarded but not yet answered, keyed by the id's canonical
    // JSON so numeric and string ids never collide.
    let mut pending: HashMap<String, PendingRequest> = HashMap::new();
    // Buffered ids are registered for the whole batch up front: once the
    // flush begins the session owns these requests, so a TCP drop mid-flush
    // answers the not-yet-written ones too — as undelivered (retryable),
    // while the already-written prefix is delivered (outcome unknown).
    for line in &buffered {
        if let Some(id) = request_id(line) {
            pending.insert(
                id.to_string(),
                PendingRequest {
                    id,
                    delivered: false,
                },
            );
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
        if let Some(id) = request_id(line) {
            if let Some(entry) = pending.get_mut(&id.to_string()) {
                entry.delivered = true;
            }
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
                        // Two-phase transition: register the request as
                        // undelivered, then mark it delivered only once its
                        // line was written to the socket.
                        let mut key = None;
                        if let Some(id) = request_id(&line) {
                            let k = id.to_string();
                            pending.insert(k.clone(), PendingRequest {
                                id,
                                delivered: false,
                            });
                            key = Some(k);
                        }
                        if write_line(&mut tcp_write, &line).await.is_err() {
                            break SessionEnd::TcpDropped;
                        }
                        if let Some(key) = key {
                            if let Some(entry) = pending.get_mut(&key) {
                                entry.delivered = true;
                            }
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
        for entry in pending.into_values() {
            if entry.delivered {
                write_outcome_unknown_error(output, &entry.id).await?;
            } else {
                write_disconnected_error(output, &entry.id).await?;
            }
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

/// Write the synthesized non-retryable outcome-unknown JSON-RPC error for
/// `id` to `output` — the request was delivered to the listener before the
/// drop, so it may have (partially) executed (monorepo#1530).
async fn write_outcome_unknown_error<W: AsyncWrite + Unpin>(
    output: &mut W,
    id: &Value,
) -> std::io::Result<()> {
    let response = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": BRIDGE_OUTCOME_UNKNOWN_CODE,
            "message": BRIDGE_OUTCOME_UNKNOWN_MESSAGE,
            "data": { "retryable": false },
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
