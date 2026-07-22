//! The spawn-time bridge that lets a real spawned child reach the in-process
//! agent→BE MCP server (§6.8).
//!
//! [`serve_workspace_mcp_tcp`] binds a loopback TCP listener and serves a
//! [`WorkspaceMcpServer`] over newline-delimited JSON-RPC (one
//! [`WorkspaceMcpServer::handle_message`] per request line). The generated
//! `--mcp-config` points a provider's MCP client at the `intentd mcp-bridge`
//! subcommand, whose body is [`run_stdio_bridge`]: a stdio↔TCP proxy that
//! forwards the child's MCP frames to this listener. This is the Rust analog of
//! the TS `http-mcp-bridge` + `mcp-stdio-server` proxy pair — a real transport,
//! not an in-process `handle_message` shortcut.

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::mcp_server::WorkspaceMcpServer;

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

/// Serve one accepted MCP connection: read request lines, dispatch through the
/// shared server, and write each response back as a single line.
async fn serve_connection(
    server: Arc<WorkspaceMcpServer>,
    stream: TcpStream,
) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if let Some(response) = server.handle_message(&message).await {
            write.write_all(format!("{response}\n").as_bytes()).await?;
            write.flush().await?;
        }
    }
    Ok(())
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
