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

#[cfg(unix)]
use intent_services::{Subscription, SubscriptionFilter};
#[cfg(unix)]
use serde_json::{json, Value};
#[cfg(unix)]
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
#[cfg(unix)]
use tokio::sync::mpsc;
#[cfg(unix)]
use tokio::task::JoinHandle;

#[cfg(unix)]
use crate::events::{self, FastPath};
#[cfg(unix)]
use crate::router::handle_message;

/// Capacity of the per-connection outbound frame queue (responses + pushed
/// notifications are serialized through one writer task in send order).
#[cfg(unix)]
const OUTBOUND_CAPACITY: usize = 256;

/// Serve JSON-RPC over a UDS until `shutdown` resolves. `bus` is the shared
/// in-process event bus that connection subscriptions are wired to.
#[cfg(unix)]
pub async fn serve_uds<F>(
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
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
                        let bus = bus.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(stream, api, bus).await {
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

/// Per-connection record for one active subscription: the forwarder task (its
/// `Drop` aborts delivery and releases the bus subscription) and the optional
/// `replaceGroup` it belongs to.
#[cfg(unix)]
struct ConnSub {
    handle: JoinHandle<()>,
    replace_group: Option<String>,
}

#[cfg(unix)]
impl Drop for ConnSub {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Runtime subscription registry for a single connection. Dropping it (on
/// connection close) aborts every forwarder → disconnect cleanup (§6.1).
#[cfg(unix)]
#[derive(Default)]
struct ConnSubs {
    subs: HashMap<String, ConnSub>,
}

#[cfg(unix)]
impl ConnSubs {
    fn insert(&mut self, id: String, handle: JoinHandle<()>, replace_group: Option<String>) {
        self.subs.insert(
            id,
            ConnSub {
                handle,
                replace_group,
            },
        );
    }

    /// Remove one subscription; `true` if it existed (TS `handleUnsubscribe`).
    fn remove(&mut self, id: &str) -> bool {
        self.subs.remove(id).is_some()
    }

    /// Drop every subscription sharing `group` (`replaceGroup` semantics).
    fn remove_group(&mut self, group: &str) {
        let ids: Vec<String> = self
            .subs
            .iter()
            .filter(|(_, s)| s.replace_group.as_deref() == Some(group))
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.subs.remove(&id);
        }
    }
}

/// Serve one connection: read newline-delimited frames, answer requests, push
/// matching `events.event` notifications, and clean up all subscriptions on exit.
#[cfg(unix)]
async fn handle_connection(
    stream: UnixStream,
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
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
        if !process_frame(trimmed, &*api, &bus, &out_tx, &mut subs).await {
            break Ok(());
        }
    };

    // Cleanup: abort all forwarders, then let the writer drain and finish.
    drop(subs);
    drop(out_tx);
    let _ = writer.await;
    io_result
}

/// Route one frame: intercept the `events.` fast-path, else hand to the
/// JSON-RPC dispatcher. Returns `false` when the outbound channel is closed.
#[cfg(unix)]
async fn process_frame(
    raw: &str,
    api: &dyn WorkspaceApi,
    bus: &EventBus,
    out_tx: &mpsc::Sender<String>,
    subs: &mut ConnSubs,
) -> bool {
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        if let Some(fast_path) = events::classify(&value) {
            return handle_fast_path(fast_path, bus, out_tx, subs).await;
        }
    }
    match handle_message(api, raw).await {
        Some(response) => out_tx.send(response).await.is_ok(),
        None => true,
    }
}

/// Handle a classified `events.subscribe` / `events.unsubscribe` request,
/// mirroring `handleSubscribe` / `handleUnsubscribe`. Returns `false` when the
/// outbound channel is closed.
#[cfg(unix)]
async fn handle_fast_path(
    fast_path: FastPath,
    bus: &EventBus,
    out_tx: &mpsc::Sender<String>,
    subs: &mut ConnSubs,
) -> bool {
    match fast_path {
        FastPath::Subscribe { id, params } => match events::parse_subscribe_params(&params) {
            Ok(p) => {
                let events::SubscribeParams {
                    event_types,
                    workspace_id,
                    replace_group,
                } = p;
                if let Some(group) = replace_group.as_deref() {
                    subs.remove_group(group);
                }
                // Canonical WS bridge: each accepted event is delivered
                // individually (no server-side coalescing, §6.6).
                let subscription = bus.subscribe(SubscriptionFilter {
                    event_types,
                    workspace_id,
                    batch_window: None,
                    ..Default::default()
                });
                let subscription_id = events::next_subscription_id();
                // Enqueue the response before spawning the forwarder so it can
                // never be preceded by an `events.event` notification.
                if id.present {
                    let frame = events::success_frame(
                        id.echo,
                        json!({ "subscriptionId": subscription_id }),
                    );
                    if out_tx.send(frame).await.is_err() {
                        return false;
                    }
                }
                let handle = tokio::spawn(forward_subscription(
                    subscription,
                    subscription_id.clone(),
                    out_tx.clone(),
                ));
                subs.insert(subscription_id, handle, replace_group);
                true
            }
            Err(msg) => send_fast_path_error(id, &msg, out_tx).await,
        },
        FastPath::Unsubscribe { id, params } => match events::parse_unsubscribe_id(&params) {
            Ok(subscription_id) => {
                let success = subs.remove(&subscription_id);
                if id.present {
                    let frame = events::success_frame(id.echo, json!({ "success": success }));
                    return out_tx.send(frame).await.is_ok();
                }
                true
            }
            Err(msg) => send_fast_path_error(id, &msg, out_tx).await,
        },
    }
}

/// Send a `-32602` error for a fast-path request, but only when it had an `id`
/// (notifications get no response). Returns `false` if the channel is closed.
#[cfg(unix)]
async fn send_fast_path_error(
    id: events::IdInfo,
    message: &str,
    out_tx: &mpsc::Sender<String>,
) -> bool {
    if id.present {
        let frame = events::error_frame(id.echo, -32602, message);
        return out_tx.send(frame).await.is_ok();
    }
    true
}

/// Forward filtered events from one bus subscription to the connection's
/// outbound queue as `events.event` notifications until the bus or connection
/// closes. Aborted by [`ConnSub`] on unsubscribe / disconnect.
#[cfg(unix)]
async fn forward_subscription(
    mut subscription: Subscription,
    subscription_id: String,
    out_tx: mpsc::Sender<String>,
) {
    while let Some(batch) = subscription.recv().await {
        for event in batch {
            let frame = events::build_event_notification(&subscription_id, &event);
            if out_tx.send(frame).await.is_err() {
                return;
            }
        }
    }
}

/// Non-Unix fallback: UDS is unavailable, so report a clear runtime error
/// instead of failing to compile. A real Windows transport (TCP/TLS) is M5.
#[cfg(not(unix))]
pub async fn serve_uds<F>(
    _api: Arc<dyn WorkspaceApi>,
    _bus: EventBus,
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
