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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

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
/// and malformed lines are still skipped. When the connection tears down, all
/// in-flight request tasks are aborted.
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
        match lines.next_line().await {
            Ok(Some(line)) => {
                // Reap finished request tasks so the set does not grow
                // unboundedly over a long-lived connection.
                while in_flight.try_join_next().is_some() {}
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
        }
    };
    // Connection teardown: abort in-flight request tasks, then let the writer
    // drain and exit once every sender is gone.
    in_flight.shutdown().await;
    drop(response_tx);
    let _ = writer.await;
    result
}

/// Body of the `intentd mcp-bridge --connect <addr>` subcommand: connect to a
/// per-agent listener (see [`serve_workspace_mcp_tcp`]) and pump stdin lines to
/// the socket and socket lines to stdout, giving a spawned provider a real stdio
/// MCP server that proxies to the in-process workspace tools.
pub async fn run_stdio_bridge(addr: &str) -> std::io::Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (tcp_read, mut tcp_write) = stream.into_split();
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();

    // stdin → socket
    let up = tokio::spawn(async move {
        let mut lines = BufReader::new(stdin).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if tcp_write.write_all(line.as_bytes()).await.is_err()
                || tcp_write.write_all(b"\n").await.is_err()
                || tcp_write.flush().await.is_err()
            {
                break;
            }
        }
    });

    // socket → stdout
    let mut tcp_lines = BufReader::new(tcp_read).lines();
    while let Some(line) = tcp_lines.next_line().await? {
        stdout.write_all(line.as_bytes()).await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    up.abort();
    Ok(())
}
