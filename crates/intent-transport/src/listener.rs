//! Local listener (§5.1): Unix domain socket on Unix, named pipe on Windows.
//!
//! On Unix this binds a `tokio::net::UnixListener` at the resolved socket path
//! with mode `0600`, removes a stale socket before bind, and serves
//! newline-delimited JSON-RPC frames. One task is spawned per connection.
//!
//! A connection is no longer strictly request/response: in addition to reading
//! requests and writing their responses, the daemon PUSHES server-initiated
//! `events.event` notifications (PROTOCOL §6) over the same open connection.
//! `events.subscribe` / `events.unsubscribe` are handled as a FAST-PATH before
//! the JSON-RPC dispatcher (mirroring `websocket-api-server.ts`). Per-connection
//! subscription state is runtime-only and dropped when the connection closes.
//!
//! On Windows the same per-connection frame loop is served over a named pipe
//! (`tokio::net::windows::named_pipe`) whose name is derived from the resolved
//! socket path via [`derive_pipe_name`], so the daemon and cloudlands-fe agree
//! on the rendezvous point without exchanging extra state. On targets that are
//! neither unix nor windows, `serve_uds` still returns an `Unsupported` error
//! at runtime.

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use intent_core::WorkspaceApi;
use intent_services::EventBus;
#[cfg(any(windows, test))]
use sha2::{Digest, Sha256};

use crate::control::SystemControl;
use crate::reverse::PrimaryReverseRegistry;
use crate::rpc_limit::RpcLimiter;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use tokio::net::UnixListener;

#[cfg(any(unix, windows))]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[cfg(any(unix, windows))]
use crate::conn::{outbound_channel, process_frame, ConnSubs};
#[cfg(any(unix, windows))]
use crate::forward::ForwardRegistry;
#[cfg(any(unix, windows))]
use crate::reverse::ReverseChannel;

/// Derive the Windows named-pipe name for a resolved socket path (the spec's
/// pipe-name contract, mirrored byte-for-byte by cloudlands-fe):
/// `\\.\pipe\intentd-<hash16>` where `<hash16>` = first 16 hex chars of the
/// SHA-256 of the socket path after normalization — absolute form, backslash
/// separators, lowercased. Hashing (instead of embedding the path) isolates
/// instances per data dir (prod vs dev vs tests) without pipe-name length or
/// charset concerns.
///
/// Known vector (the cloudlands-fe unit test mirrors this exact pair):
///   input:      `C:\Users\Alice\AppData\Roaming\intentd\intentd.sock`
///   normalized: `c:\users\alice\appdata\roaming\intentd\intentd.sock`
///   pipe name:  `\\.\pipe\intentd-f763fc173ce41365`
///
/// The caller must pass an ABSOLUTE path (the Windows listener resolves via
/// `std::path::absolute` first); this function only normalizes separators and
/// case so it compiles and is unit-tested on every platform.
#[cfg(any(windows, test))]
pub(crate) fn derive_pipe_name(resolved_socket_path: &str) -> String {
    let normalized = resolved_socket_path.replace('/', "\\").to_lowercase();
    let digest = Sha256::digest(normalized.as_bytes());
    format!(r"\\.\pipe\intentd-{}", hex::encode(&digest[..8]))
}

/// Resolve `socket_path` to absolute form and derive the pipe name it maps to.
/// Public so local clients (the `intentd` CLI) connect to the exact same pipe
/// the listener binds, without re-implementing the resolution.
#[cfg(windows)]
pub fn pipe_name_for_socket_path(socket_path: &Path) -> std::io::Result<String> {
    let absolute = std::path::absolute(socket_path)?;
    Ok(derive_pipe_name(&absolute.to_string_lossy()))
}

/// Serve JSON-RPC over the local transport (UDS on Unix, named pipe on
/// Windows) until `shutdown` resolves. `bus` is the shared in-process event
/// bus that connection subscriptions are wired to. `control`, when present,
/// exposes the `system.status`/`system.shutdown` control surface (§5.7) to
/// local clients (`intentd status`/`stop`).
///
/// This wrapper installs a fresh (empty) [`PrimaryReverseRegistry`] and an
/// unlimited [`RpcLimiter`] so tests and other lightweight callers stay
/// one-liner. Composition roots that share a registry across the UDS + WSS
/// listeners (REV-1) — and the daemon-wide outstanding-RPC cap — call
/// [`serve_uds_with_reverse`] instead.
///
/// # Errors
///
/// Returns the underlying I/O error if binding or serving the local socket fails.
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
        None,
        Arc::new(PrimaryReverseRegistry::new()),
        RpcLimiter::unlimited(),
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
/// see the same live set of clients. `server_pairing_info`, when present,
/// exposes `server.pairingInfo`/`server.rotateToken` (§5.2) to local clients.
/// `limiter` is the daemon-wide outstanding-slow-path-RPC cap
/// (`server.maxOutstandingRpcs`) shared with the WSS listener.
///
/// # Errors
///
/// Returns the underlying I/O error if binding or serving the local socket fails.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
pub async fn serve_uds_with_reverse<F>(
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    socket_path: &Path,
    control: Option<Arc<dyn SystemControl>>,
    server_pairing_info: Option<Arc<dyn crate::server::ServerPairingInfo>>,
    reverse_registry: Arc<PrimaryReverseRegistry>,
    limiter: RpcLimiter,
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
                        let server_pairing_info = server_pairing_info.clone();
                        let reverse_registry = reverse_registry.clone();
                        let limiter = limiter.clone();
                        tokio::spawn(async move {
                            let (read_half, write_half) = stream.into_split();
                            if let Err(e) = handle_connection(read_half, write_half, api, bus, control, server_pairing_info, reverse_registry, limiter).await {
                                tracing::debug!(error = %e, "uds connection ended");
                            }
                        });
                    }
                    Err(e) => tracing::warn!(error = %e, "uds accept failed"),
                }
            }
            () = &mut shutdown => break,
        }
    }

    let _ = std::fs::remove_file(socket_path);
    tracing::info!("intentd UDS listener stopped");
    Ok(())
}

/// Serve one connection: read newline-delimited frames, answer requests, push
/// matching `events.event` notifications, and clean up all subscriptions on exit.
/// Generic over the split stream halves so the Unix socket and Windows named
/// pipe share one frame loop byte-for-byte.
#[cfg(any(unix, windows))]
#[allow(clippy::too_many_arguments)]
async fn handle_connection<R, W>(
    read_half: R,
    write_half: W,
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    control: Option<Arc<dyn SystemControl>>,
    server_pairing_info: Option<Arc<dyn crate::server::ServerPairingInfo>>,
    reverse_registry: Arc<PrimaryReverseRegistry>,
    limiter: RpcLimiter,
) -> std::io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(read_half);

    // One writer task drains the two-lane outbound queue (priority lane
    // first, so RPC responses overtake queued event/subscription pushes on a
    // saturated link) and responses and pushed notifications never interleave
    // mid-frame on the socket.
    let (out_tx, mut out_rx) = outbound_channel();
    let writer = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(frame) = out_rx.recv().await {
            // Hard cap: never write multi-hundred-MB frames (observed git.diffs
            // at 277 MiB HOL'd the writer for ~38s and timed out host.status).
            // Last-resort backstop for non-response frames (subscription
            // pushes/events): oversized router responses are already replaced
            // with a `-32010` error at serialization, where the request id is
            // known.
            if frame.len() > crate::MAX_OUTBOUND_MESSAGE_BYTES {
                tracing::error!(
                    frame_bytes = frame.len(),
                    limit = crate::MAX_OUTBOUND_MESSAGE_BYTES,
                    "dropping oversized outbound UDS frame"
                );
                continue;
            }
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
    let reverse = ReverseChannel::new(out_tx.priority_sender());
    // REV-1: register this connection's reverse channel with the shared
    // primary-target set so agent-initiated `browser.exec` calls can route to
    // whichever client connected first. The guard drops when this function
    // returns (normal exit, error, or panic-unwind) so failover is exactly the
    // connection arrival order.
    let reverse_guard = reverse_registry.register(reverse.clone());
    // Per-connection logical-client binding (§16): `None` until `client.hello`.
    let mut client_id: Option<intent_core::ClientId> = None;
    let mut line = Vec::new();
    let io_result = loop {
        line.clear();
        match read_line_bounded(&mut reader, &mut line, crate::MAX_INBOUND_MESSAGE_BYTES).await {
            Ok(BoundedLine::Eof) => break Ok(()),
            Ok(BoundedLine::Line) => {}
            Ok(BoundedLine::TooLong) => {
                // Over-limit frame (monorepo#472): answer with a `-32600`
                // error (`id: null` — the request was never parsed) and end
                // the connection WITHOUT draining the rest of the oversized
                // line into memory.
                let frame = crate::events::error_frame(
                    &serde_json::Value::Null,
                    -32600,
                    &format!(
                        "message exceeds maximum size of {} bytes",
                        crate::MAX_INBOUND_MESSAGE_BYTES
                    ),
                );
                let _ = out_tx.send_priority(frame).await;
                break Ok(());
            }
            Err(e) => break Err(e),
        }
        let Ok(text) = std::str::from_utf8(&line) else {
            break Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame is not valid UTF-8",
            ));
        };
        let trimmed = text.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        // A send failure means the writer/client is gone → end the connection.
        // UDS is the local control transport, so `is_local = true` (§12.3).
        // Wrap in connection context (is_tcp=false for UDS) so server.* RPCs can
        // gate on real origin (§5.2).
        let frame_ok = crate::context::with_connection_context(false, async {
            process_frame(
                trimmed,
                &api,
                &bus,
                &out_tx,
                &mut subs,
                &mut forwards,
                &reverse,
                control.as_ref(),
                server_pairing_info.as_ref(),
                &mut client_id,
                true,
                &limiter,
            )
            .await
        })
        .await;
        if !frame_ok {
            break Ok(());
        }
    };

    // Cleanup: abort all subscriptions + forwards, then close the outbound
    // queue and let the writer finish. The reverse channel and its registry
    // guard hold `out_tx` clones, so both must drop before the writer can
    // observe the channel closing — otherwise the writer (and the socket's
    // write half) would outlive the connection and the peer would never see
    // EOF after a server-initiated close (e.g. an oversized frame).
    drop(subs);
    drop(forwards);
    drop(reverse_guard);
    drop(reverse);
    drop(out_tx);
    let _ = writer.await;
    io_result
}

/// Outcome of one bounded line read (see [`read_line_bounded`]).
#[cfg(any(unix, windows))]
enum BoundedLine {
    /// A complete line (newline consumed, not included in the buffer) — or the
    /// final unterminated line before EOF.
    Line,
    /// Clean EOF with no buffered bytes.
    Eof,
    /// The line exceeded the limit; the tail was NOT read into memory.
    TooLong,
}

/// Read one newline-delimited line into `buf`, never buffering more than
/// `limit` bytes (monorepo#472). Unlike `read_line`, an over-limit line yields
/// [`BoundedLine::TooLong`] with at most `limit` bytes consumed, so a hostile
/// or buggy client cannot make the daemon buffer an unbounded frame.
#[cfg(any(unix, windows))]
async fn read_line_bounded<R>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    limit: usize,
) -> std::io::Result<BoundedLine>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(if buf.is_empty() {
                BoundedLine::Eof
            } else {
                BoundedLine::Line
            });
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            if buf.len() + pos > limit {
                return Ok(BoundedLine::TooLong);
            }
            buf.extend_from_slice(&available[..pos]);
            reader.consume(pos + 1);
            return Ok(BoundedLine::Line);
        }
        let len = available.len();
        if buf.len() + len > limit {
            return Ok(BoundedLine::TooLong);
        }
        buf.extend_from_slice(available);
        reader.consume(len);
    }
}

/// Windows implementation of [`serve_uds_with_reverse`]: serve the same
/// newline-delimited JSON-RPC frame loop over a named pipe whose name is
/// derived from `socket_path` (see [`derive_pipe_name`]). Loop of
/// create-instance → connect → spawn handler; the next instance is created
/// BEFORE the connected one is handed to its task so there is no accept gap.
///
/// The first instance is created with `first_pipe_instance(true)` so a second
/// daemon on the same data dir fails fast at bind time — the single-instance
/// guard equivalent of the UDS bind conflict. Named pipes are per-boot kernel
/// objects scoped to the creating process, so there is no stale-file removal
/// before bind and nothing to unlink on shutdown. The default pipe security
/// descriptor grants full control to the creating user, SYSTEM, and
/// Administrators, and read-only to Everyone (who therefore cannot open a
/// duplex pipe) — so other non-admin users cannot connect, though this is not
/// a strict 0600 equivalent.
#[cfg(windows)]
#[allow(clippy::too_many_arguments)]
pub async fn serve_uds_with_reverse<F>(
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    socket_path: &Path,
    control: Option<Arc<dyn SystemControl>>,
    server_pairing_info: Option<Arc<dyn crate::server::ServerPairingInfo>>,
    reverse_registry: Arc<PrimaryReverseRegistry>,
    limiter: RpcLimiter,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()>,
{
    use tokio::net::windows::named_pipe::ServerOptions;

    let pipe_name = pipe_name_for_socket_path(socket_path)?;
    let mut server = ServerOptions::new()
        .first_pipe_instance(true)
        .create(&pipe_name)?;
    tracing::info!(pipe = %pipe_name, path = %socket_path.display(), "intentd listening on named pipe");

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            connected = server.connect() => {
                match connected {
                    Ok(()) => {
                        let next = ServerOptions::new().create(&pipe_name)?;
                        let stream = std::mem::replace(&mut server, next);
                        let api = api.clone();
                        let bus = bus.clone();
                        let control = control.clone();
                        let server_pairing_info = server_pairing_info.clone();
                        let reverse_registry = reverse_registry.clone();
                        let limiter = limiter.clone();
                        tokio::spawn(async move {
                            let (read_half, write_half) = tokio::io::split(stream);
                            if let Err(e) = handle_connection(read_half, write_half, api, bus, control, server_pairing_info, reverse_registry, limiter).await {
                                tracing::debug!(error = %e, "named-pipe connection ended");
                            }
                        });
                    }
                    Err(e) => tracing::warn!(error = %e, "named-pipe connect failed"),
                }
            }
            _ = &mut shutdown => break,
        }
    }

    tracing::info!("intentd named-pipe listener stopped");
    Ok(())
}

/// Fallback for targets that are neither unix nor windows: the composition
/// root (`intentd/src/main.rs`) references this symbol unconditionally, so the
/// crate must expose it on every platform. Any attempt to serve reports an
/// `Unsupported` error at runtime.
#[cfg(not(any(unix, windows)))]
#[allow(clippy::too_many_arguments)]
pub async fn serve_uds_with_reverse<F>(
    _api: Arc<dyn WorkspaceApi>,
    _bus: EventBus,
    _socket_path: &Path,
    _control: Option<Arc<dyn SystemControl>>,
    _server_pairing_info: Option<Arc<dyn crate::server::ServerPairingInfo>>,
    _reverse_registry: Arc<PrimaryReverseRegistry>,
    _limiter: RpcLimiter,
    _shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()>,
{
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "local IPC transport is not supported on this platform",
    ))
}

#[cfg(test)]
mod pipe_name_tests {
    use super::derive_pipe_name;

    /// Known vector from the spec's pipe-name contract — the cloudlands-fe
    /// unit test mirrors this exact input → output pair.
    #[test]
    fn derives_known_vector() {
        assert_eq!(
            derive_pipe_name(r"C:\Users\Alice\AppData\Roaming\intentd\intentd.sock"),
            r"\\.\pipe\intentd-f763fc173ce41365"
        );
    }

    #[test]
    fn normalizes_separators_and_case() {
        assert_eq!(
            derive_pipe_name("C:/Dev/Intent Data/intentd.sock"),
            r"\\.\pipe\intentd-3f81294c4c07e710"
        );
        assert_eq!(
            derive_pipe_name(r"c:\dev\intent data\intentd.sock"),
            derive_pipe_name("C:/DEV/Intent Data/intentd.sock"),
        );
    }

    #[test]
    fn distinct_paths_get_distinct_pipes() {
        assert_ne!(
            derive_pipe_name(r"c:\a\intentd.sock"),
            derive_pipe_name(r"c:\b\intentd.sock")
        );
    }
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::{read_line_bounded, BoundedLine};
    use tokio::io::BufReader;

    #[tokio::test]
    async fn bounded_reader_reads_lines_within_limit() {
        let data: &[u8] = b"first\nsecond\n";
        let mut reader = BufReader::new(data);
        let mut buf = Vec::new();
        assert!(matches!(
            read_line_bounded(&mut reader, &mut buf, 64).await.unwrap(),
            BoundedLine::Line
        ));
        assert_eq!(buf, b"first");
        buf.clear();
        assert!(matches!(
            read_line_bounded(&mut reader, &mut buf, 64).await.unwrap(),
            BoundedLine::Line
        ));
        assert_eq!(buf, b"second");
        buf.clear();
        assert!(matches!(
            read_line_bounded(&mut reader, &mut buf, 64).await.unwrap(),
            BoundedLine::Eof
        ));
    }

    #[tokio::test]
    async fn bounded_reader_returns_final_unterminated_line() {
        let data: &[u8] = b"tail-no-newline";
        let mut reader = BufReader::new(data);
        let mut buf = Vec::new();
        assert!(matches!(
            read_line_bounded(&mut reader, &mut buf, 64).await.unwrap(),
            BoundedLine::Line
        ));
        assert_eq!(buf, b"tail-no-newline");
    }

    #[tokio::test]
    async fn bounded_reader_allows_line_exactly_at_limit() {
        let data = [vec![b'a'; 8], b"\n".to_vec()].concat();
        let mut reader = BufReader::new(&data[..]);
        let mut buf = Vec::new();
        assert!(matches!(
            read_line_bounded(&mut reader, &mut buf, 8).await.unwrap(),
            BoundedLine::Line
        ));
        assert_eq!(buf.len(), 8);
    }

    #[tokio::test]
    async fn bounded_reader_rejects_over_limit_line_without_buffering_it() {
        // A 1 MiB line against an 8-byte limit: TooLong, with at most `limit`
        // bytes buffered — the reader must not slurp the rest.
        let data = [vec![b'a'; 1024 * 1024], b"\n".to_vec()].concat();
        let mut reader = BufReader::new(&data[..]);
        let mut buf = Vec::new();
        assert!(matches!(
            read_line_bounded(&mut reader, &mut buf, 8).await.unwrap(),
            BoundedLine::TooLong
        ));
        assert!(
            buf.len() <= 8,
            "buffered {} bytes past the limit",
            buf.len()
        );
    }
}
