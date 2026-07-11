//! Unix-domain-socket listener (§5.1).
//!
//! Binds a `tokio::net::UnixListener` at the resolved socket path with mode
//! `0600`, removes a stale socket before bind, and serves newline-delimited
//! JSON-RPC frames. One task is spawned per connection.
//!
//! A connection is no longer strictly request/response: in addition to reading
//! requests and writing their responses, the daemon PUSHES server-initiated
//! `events.event` notifications (PROTOCOL §6) over the same open connection.
//! `events.subscribe` / `events.unsubscribe` are handled as a FAST-PATH before
//! the JSON-RPC dispatcher (mirroring `websocket-api-server.ts`). Per-connection
//! subscription state is runtime-only and dropped when the connection closes.
//!
//! UDS is Unix-only. The listener is gated behind `#[cfg(unix)]`; on other
//! platforms (e.g. Windows) the crate still builds and `serve_uds` returns an
//! `Unsupported` error at runtime. A Windows transport (TCP/TLS) is deferred to
//! M5 and intentionally not provided here.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use intent_core::WorkspaceApi;
use intent_services::EventBus;

use crate::control::SystemControl;
use crate::reverse::PrimaryReverseRegistry;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
#[cfg(unix)]
use tokio::sync::mpsc;

#[cfg(unix)]
use crate::conn::{process_frame, ConnSubs, OUTBOUND_CAPACITY};
#[cfg(unix)]
use crate::forward::ForwardRegistry;
#[cfg(unix)]
use crate::reverse::ReverseChannel;

/// Serve JSON-RPC over a UDS until `shutdown` resolves. `bus` is the shared
/// in-process event bus that connection subscriptions are wired to. `control`,
/// when present, exposes the `system.status`/`system.shutdown` control surface
/// (§5.7) to local UDS clients (`intentd status`/`stop`).
///
/// This wrapper installs a fresh (empty) [`PrimaryReverseRegistry`] so tests
/// and other lightweight callers stay one-liner. Composition roots that share
/// a registry across the UDS + WSS listeners (REV-1) call
/// [`serve_uds_with_reverse`] instead.
#[cfg(unix)]
pub async fn serve_uds<F>(
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    socket_path: &Path,
    control: Option<Arc<dyn SystemControl>>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()>,
{
    serve_uds_with_reverse(
        api,
        bus,
        socket_path,
        control,
        Arc::new(PrimaryReverseRegistry::new()),
        shutdown,
    )
    .await
}

/// Variant of [`serve_uds`] that threads the shared REV-1 primary registry
/// through so every accepted connection registers its per-connection reverse
/// channel with the sticky "first-client wins" target set used by
/// agent-initiated reverse RPCs (§5.14/§12.4). Composition roots build ONE
/// registry, hand it to both the UDS + WSS listeners, and hand it to
/// `Services::with_reverse_dispatch` so agent-initiated `browser.exec` calls
/// see the same live set of clients.
#[cfg(unix)]
pub async fn serve_uds_with_reverse<F>(
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    socket_path: &Path,
    control: Option<Arc<dyn SystemControl>>,
    reverse_registry: Arc<PrimaryReverseRegistry>,
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
                        let bus = bus.clone();
                        let control = control.clone();
                        let reverse_registry = reverse_registry.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, api, bus, control, reverse_registry).await {
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

/// Serve one connection: read newline-delimited frames, answer requests, push
/// matching `events.event` notifications, and clean up all subscriptions on exit.
#[cfg(unix)]
async fn handle_connection(
    stream: UnixStream,
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    control: Option<Arc<dyn SystemControl>>,
    reverse_registry: Arc<PrimaryReverseRegistry>,
) -> std::io::Result<()> {
    let (read_half, write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // One writer task drains the outbound queue so responses and pushed
    // notifications never interleave mid-frame on the socket.
    let (out_tx, mut out_rx) = mpsc::channel::<String>(OUTBOUND_CAPACITY);
    let writer = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(frame) = out_rx.recv().await {
            if write_half.write_all(frame.as_bytes()).await.is_err()
                || write_half.write_all(b"\n").await.is_err()
                || write_half.flush().await.is_err()
            {
                break;
            }
        }
    });

    let mut subs = ConnSubs::default();
    let mut forwards = ForwardRegistry::default();
    let reverse = ReverseChannel::new(out_tx.clone());
    // REV-1: register this connection's reverse channel with the shared
    // primary-target set so agent-initiated `browser.exec` calls can route to
    // whichever client connected first. The guard drops when this function
    // returns (normal exit, error, or panic-unwind) so failover is exactly the
    // connection arrival order.
    let _reverse_guard = reverse_registry.register(reverse.clone());
    // Per-connection logical-client binding (§16): `None` until `client.hello`.
    let mut client_id: Option<intent_core::ClientId> = None;
    let mut line = String::new();
    let io_result = loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break Ok(()), // EOF
            Ok(_) => {}
            Err(e) => break Err(e),
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        // A send failure means the writer/client is gone → end the connection.
        // UDS is the local control transport, so `is_local = true` (§12.3).
        if !process_frame(
            trimmed,
            &api,
            &bus,
            &out_tx,
            &mut subs,
            &mut forwards,
            &reverse,
            control.as_ref(),
            &mut client_id,
            true,
        )
        .await
        {
            break Ok(());
        }
    };

    // Cleanup: abort all subscriptions + forwards, then let the writer finish.
    drop(subs);
    drop(forwards);
    drop(out_tx);
    let _ = writer.await;
    io_result
}

/// Non-Unix fallback: UDS is unavailable, so report a clear runtime error
/// instead of failing to compile. A real Windows transport (TCP/TLS) is M5.
#[cfg(not(unix))]
pub async fn serve_uds<F>(
    _api: Arc<dyn WorkspaceApi>,
    _bus: EventBus,
    _socket_path: &Path,
    _control: Option<Arc<dyn SystemControl>>,
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
