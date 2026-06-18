//! Unix-domain-socket listener (§5.1).
//!
//! Binds a `tokio::net::UnixListener` at the resolved socket path with mode
//! `0600`, removes a stale socket before bind, and serves newline-delimited
//! JSON-RPC frames. One task is spawned per connection and many requests may be
//! handled on a single connection. Runs until the `shutdown` future resolves,
//! then removes the socket file.
//!
//! UDS is Unix-only. The listener is gated behind `#[cfg(unix)]`; on other
//! platforms (e.g. Windows) the crate still builds and `serve_uds` returns an
//! `Unsupported` error at runtime. A Windows transport (TCP/TLS) is deferred to
//! M5 and intentionally not provided here.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use intent_core::WorkspaceApi;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

#[cfg(unix)]
use crate::router::handle_message;

/// Serve JSON-RPC over a UDS until `shutdown` resolves.
#[cfg(unix)]
pub async fn serve_uds<F>(
    api: Arc<dyn WorkspaceApi>,
    socket_path: &Path,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()>,
{
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Remove a stale socket file before binding (§5.1).
    if socket_path.exists() {
        let _ = std::fs::remove_file(socket_path);
    }

    let listener = UnixListener::bind(socket_path)?;
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    tracing::info!(path = %socket_path.display(), "intentd listening on UDS");

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _addr)) => {
                        let api = api.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, api).await {
                                tracing::debug!(error = %e, "uds connection ended");
                            }
                        });
                    }
                    Err(e) => tracing::warn!(error = %e, "uds accept failed"),
                }
            }
            _ = &mut shutdown => break,
        }
    }

    let _ = std::fs::remove_file(socket_path);
    tracing::info!("intentd UDS listener stopped");
    Ok(())
}

/// Read newline-delimited requests from one connection and write responses.
#[cfg(unix)]
async fn handle_connection(stream: UnixStream, api: Arc<dyn WorkspaceApi>) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // EOF
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        if let Some(response) = handle_message(&*api, trimmed).await {
            write_half.write_all(response.as_bytes()).await?;
            write_half.write_all(b"\n").await?;
            write_half.flush().await?;
        }
    }
    Ok(())
}

/// Non-Unix fallback: UDS is unavailable, so report a clear runtime error
/// instead of failing to compile. A real Windows transport (TCP/TLS) is M5.
#[cfg(not(unix))]
pub async fn serve_uds<F>(
    _api: Arc<dyn WorkspaceApi>,
    _socket_path: &Path,
    _shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()>,
{
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "UDS transport is not supported on this platform",
    ))
}
