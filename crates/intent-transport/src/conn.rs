//! Transport-agnostic connection orchestration (§6, §16).
//!
//! The per-connection subscription registry and frame routing shared by every
//! listener (UDS in [`crate::listener`], WSS in [`crate::ws`]). Both transports
//! read newline/`Text` frames, run the `events.` fast-path before the JSON-RPC
//! dispatcher, push `events.event` notifications over the same connection, and
//! drop all subscriptions when the connection closes — so the wire result is
//! identical regardless of transport. The only difference is framing, which the
//! transports handle by draining the two-lane outbound queue
//! ([`OutboundSender`] / [`OutboundReceiver`], priority lane first).

use intent_core::events::{NOTE_CREATED, NOTE_DELETED, NOTE_UPDATED};
use intent_core::{AgentId, ClientId, NoteId, WorkspaceApi, WorkspaceId};
use intent_services::{Delivery, EventBus, Subscription, SubscriptionFilter};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::browser;
use crate::client;
use crate::conflate::{self, ChatItem, ConflationBuffer, Enqueue, EventItem};
use crate::control::{self, SystemControl};
use crate::drafts;
use crate::events::{self, FastPath};
use crate::forward::{self, ForwardRegistry};
use crate::host;
use crate::panic_guard;
use crate::reverse::ReverseChannel;
use crate::router::{check_envelope, handle_message, EnvelopeCheck};
use crate::rpc_limit::{Overloaded, RpcLimiter, OVERLOAD_ERROR_CODE, OVERLOAD_ERROR_MESSAGE};
use crate::subscriptions::{self, Channel, SubFastPath};

/// Capacity of the per-connection **priority** outbound lane (RPC responses,
/// error frames, reverse-RPC requests). Deliberately larger than
/// [`BULK_CAPACITY`]: a saturated bulk lane must not be able to starve
/// latency-critical frames of queue headroom, so response/reverse-RPC bursts
/// only feel backpressure after 4x the bulk depth.
pub(crate) const PRIORITY_CAPACITY: usize = 1024;

/// Capacity of the per-connection **bulk** outbound lane (`events.event`
/// notifications, `subscription.push` frames). Kept small so a slow consumer
/// applies backpressure to event forwarders early, while the priority lane
/// stays open for responses.
pub(crate) const BULK_CAPACITY: usize = 256;

/// Per-connection outbound frame queue, split into two lanes:
///
/// - **priority** — JSON-RPC responses (fast-path and dispatcher), error
///   frames, and daemon-initiated reverse-RPC requests. Latency-critical:
///   a client awaiting `host.status` must not sit behind megabytes of event
///   traffic.
/// - **bulk** — `events.event` notifications and `subscription.push`
///   snapshot/delta frames pushed by forwarder tasks. High-volume,
///   throughput-bound.
///
/// The transports drain both lanes through [`OutboundReceiver::recv`], which
/// always empties the priority lane before taking a bulk frame, so an RPC
/// response overtakes queued event traffic even on a saturated link. Frames
/// within one lane keep FIFO order, which preserves the per-subscription
/// invariants (response before first push, strictly monotonic `seq`): each
/// subscription's frames all travel on the bulk lane in publish order, and
/// its `{ subscriptionId }` response is enqueued on the priority lane before
/// the forwarder is spawned.
#[derive(Clone)]
pub(crate) struct OutboundSender {
    priority: mpsc::Sender<String>,
    bulk: mpsc::Sender<String>,
}

impl OutboundSender {
    /// Queue a latency-critical frame (RPC response / error / reverse
    /// request). `Err` means the connection's writer is gone.
    pub(crate) async fn send_priority(&self, frame: String) -> Result<(), ()> {
        self.priority.send(frame).await.map_err(|_| ())
    }

    /// Queue a bulk frame (event notification / subscription push). `Err`
    /// means the connection's writer is gone.
    pub(crate) async fn send_bulk(&self, frame: String) -> Result<(), ()> {
        self.bulk.send(frame).await.map_err(|_| ())
    }

    /// Whether the writer has stopped draining (both lanes closed together;
    /// checking one suffices).
    pub(crate) fn is_closed(&self) -> bool {
        self.priority.is_closed()
    }

    /// The priority-lane sender for collaborators that only ever send
    /// latency-critical frames (the reverse-RPC channel).
    pub(crate) fn priority_sender(&self) -> mpsc::Sender<String> {
        self.priority.clone()
    }

    /// The bulk-lane sender for the subscription forwarders, which only ever
    /// send bulk frames and need `reserve` / `try_reserve` on the lane for
    /// backpressure conflation (see [`crate::conflate`]).
    pub(crate) fn bulk_sender(&self) -> mpsc::Sender<String> {
        self.bulk.clone()
    }
}

/// Receiving half of the two-lane outbound queue; owned by the transport
/// writer.
pub(crate) struct OutboundReceiver {
    pub(crate) priority: mpsc::Receiver<String>,
    pub(crate) bulk: mpsc::Receiver<String>,
    priority_open: bool,
    bulk_open: bool,
}

impl OutboundReceiver {
    /// Next frame to write, priority lane first. Empties the priority lane
    /// before taking a bulk frame; when both lanes are idle, waits on both
    /// (biased toward priority). Returns `None` once every sender is dropped
    /// and both lanes are drained. Cancel-safe: no frame is lost when the
    /// caller races this against other select arms.
    pub(crate) async fn recv(&mut self) -> Option<String> {
        loop {
            // Drain whatever is already queued on the priority lane first.
            if self.priority_open {
                match self.priority.try_recv() {
                    Ok(frame) => return Some(frame),
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => self.priority_open = false,
                }
            }
            match (self.priority_open, self.bulk_open) {
                (true, true) => {
                    tokio::select! {
                        biased;
                        frame = self.priority.recv() => match frame {
                            Some(frame) => return Some(frame),
                            None => self.priority_open = false,
                        },
                        frame = self.bulk.recv() => match frame {
                            Some(frame) => return Some(frame),
                            None => self.bulk_open = false,
                        },
                    }
                }
                (true, false) => match self.priority.recv().await {
                    Some(frame) => return Some(frame),
                    None => self.priority_open = false,
                },
                (false, true) => match self.bulk.recv().await {
                    Some(frame) => return Some(frame),
                    None => self.bulk_open = false,
                },
                (false, false) => return None,
            }
        }
    }
}

/// Build one connection's two-lane outbound queue ([`PRIORITY_CAPACITY`] /
/// [`BULK_CAPACITY`] frames).
pub(crate) fn outbound_channel() -> (OutboundSender, OutboundReceiver) {
    let (priority_tx, priority_rx) = mpsc::channel::<String>(PRIORITY_CAPACITY);
    let (bulk_tx, bulk_rx) = mpsc::channel::<String>(BULK_CAPACITY);
    (
        OutboundSender {
            priority: priority_tx,
            bulk: bulk_tx,
        },
        OutboundReceiver {
            priority: priority_rx,
            bulk: bulk_rx,
            priority_open: true,
            bulk_open: true,
        },
    )
}

/// Per-connection record for one active subscription: the forwarder task (its
/// `Drop` aborts delivery and releases the bus subscription) and the optional
/// `replaceGroup` it belongs to.
struct ConnSub {
    handle: JoinHandle<()>,
    replace_group: Option<String>,
}

impl Drop for ConnSub {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Runtime subscription registry for a single connection. Dropping it (on
/// connection close) aborts every forwarder → disconnect cleanup (§6.1).
#[derive(Default)]
pub(crate) struct ConnSubs {
    subs: HashMap<String, ConnSub>,
}

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

/// Route one frame: deliver replies to daemon-initiated reverse RPCs (§12.4),
/// then intercept the `system.*` control methods (`system.status` on both
/// transports; `system.shutdown`/`system.importLegacy` UDS-only via the
/// `is_uds` guard inside `control::handle`), the `host.status` capability
/// probe (both transports), the `forward.*` port-forwarding methods, and the
/// `events.` fast-path, else hand to the JSON-RPC dispatcher. `control` is
/// `Some` on every transport that wires the control surface — the composition
/// root passes `Some(control)` to both the UDS and WSS listeners (remote
/// `system.status` needs it); `forwards`/`reverse` are the connection's port-forward registry
/// and reverse-RPC channel; `client_id` is the connection's logical-client
/// binding, set by `client.hello` and consumed by `drafts.*` (§16); `is_local`
/// reflects that connection's resolved locality (§5.14). Returns `false` when
/// the outbound channel is closed.
///
/// The fast-paths that mutate per-connection state (`reverse.route_response`,
/// `system.*`, `forward.*`, `client.hello`, `drafts.*`, `events.`/subscription
/// fast-paths) run inline on the read loop and stay serialized. The two
/// stateless slow paths — `host::handle` and the [`handle_message`] JSON-RPC
/// dispatcher — are spawned onto detached tokio tasks that write their response
/// frame through a cloned outbound sender, so a long-running request (e.g.
/// `host.exec`) cannot delay responses to other requests on the same connection.
/// Out-of-order responses are fine: JSON-RPC correlates by `id`.
///
/// Every frame sent here is a response/error frame and travels on the
/// priority lane; only the forwarder tasks push on the bulk lane.
///
/// Every detached spawn claims a slot from the daemon-wide `limiter`
/// (`server.maxOutstandingRpcs`) first; when the cap is reached the request is
/// rejected immediately with `-32011 "Server overloaded"` (notifications are
/// dropped silently) rather than queued. The permit is moved into the spawned
/// task, so the slot is released when the task ends — panic unwinds included.
/// Frames that fail parse/envelope validation are exempt: they are answered
/// inline with the router's `-32700`/`-32600`, so the error matrix does not
/// change under load.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_frame(
    raw: &str,
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    out_tx: &OutboundSender,
    subs: &mut ConnSubs,
    forwards: &mut ForwardRegistry,
    reverse: &ReverseChannel,
    control: Option<&Arc<dyn SystemControl>>,
    server_pairing_info: Option<&Arc<dyn crate::server::ServerPairingInfo>>,
    client_id: &mut Option<ClientId>,
    is_local: bool,
    limiter: &RpcLimiter,
) -> bool {
    let parsed = serde_json::from_str::<Value>(raw).ok();
    if let Some(value) = &parsed {
        // A reply to a daemon-initiated reverse request (FE-served intents such
        // as `host.openExternal`, §12.4) — route it to the awaiting caller and
        // never treat it as a client request. It routes a *response* frame (no
        // request handler runs and there is no request id to echo an error
        // to), so a panic here (e.g. a poisoned pending map) yields no error
        // frame — but it must still not tear down the read loop, so it gets
        // its own unwind guard: treat the frame as consumed and keep serving.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reverse.route_response(value)
        })) {
            Ok(true) => return true,
            Ok(false) => {}
            Err(_) => {
                tracing::error!("reverse-response routing panicked; connection kept alive");
                return true;
            }
        }
        // Every handler path below (inline or spawned) runs under a panic
        // guard: a panicking handler yields `-32603` with the echoed request
        // `id` (no frame for notifications) and the connection stays open.
        let (rpc_id, method) = panic_guard::request_identity(value);
        // Single inbound chokepoint shared by UDS and WS (both read loops
        // route through `process_frame`): log-only large-frame warning,
        // throttled per method, bulk-transfer methods exempt.
        crate::protocol::warn_if_large_frame(
            crate::protocol::FrameDirection::Inbound,
            &method,
            raw.len(),
        );
        if let Some(control) = control {
            if let Some(req) = control::classify(value) {
                let is_uds = !crate::context::is_tcp_connection();
                let frame = panic_guard::guard_frame(
                    &method,
                    rpc_id.clone(),
                    control::handle(req, control.as_ref(), is_local, is_uds),
                )
                .await;
                return match frame {
                    Some(frame) => out_tx.send_priority(frame).await.is_ok(),
                    None => true,
                };
            }
        }
        if let Some(server_info) = server_pairing_info {
            if let Some(req) = crate::server::classify(value) {
                // server.* RPCs are local-only; gate on real connection origin (UDS vs TCP)
                // not the locality flag. Task-local context set by transport (§5.2).
                let is_local = !crate::context::is_tcp_connection();
                let frame = panic_guard::guard_frame(
                    &method,
                    rpc_id.clone(),
                    crate::server::handle(req, server_info, is_local),
                )
                .await;
                return match frame {
                    Some(frame) => out_tx.send_priority(frame).await.is_ok(),
                    None => true,
                };
            }
            if let Some(req) = crate::pairing::classify(value) {
                // pairing.getInfo shares the server.* provider and local-only gating:
                // the payload embeds the bearer token, so it never crosses TCP.
                let is_local = !crate::context::is_tcp_connection();
                let frame = panic_guard::guard_frame(
                    &method,
                    rpc_id.clone(),
                    crate::pairing::handle(req, server_info, is_local),
                )
                .await;
                return match frame {
                    Some(frame) => out_tx.send_priority(frame).await.is_ok(),
                    None => true,
                };
            }
        }
        if let Some(req) = host::classify(value) {
            // Slow path: spawn so `host.exec` and friends can't block the read
            // loop (UDS HOL fix). `openInEditor` in particular awaits an
            // FE-served reverse RPC on this same connection (§5.14) — running
            // it inline would deadlock frame reads until the reverse timeout.
            // Response is delivered through the cloned outbound sender; if the
            // connection has since closed the send is dropped silently.
            let permit = match limiter.try_acquire() {
                Ok(permit) => permit,
                Err(overloaded) => {
                    return reject_overloaded(&method, rpc_id, out_tx, overloaded).await
                }
            };
            let api = Arc::clone(api);
            let bus = bus.clone();
            let out = out_tx.clone();
            let reverse = reverse.clone();
            let is_tcp = crate::context::is_tcp_connection();
            let (rpc_id, method) = (rpc_id.clone(), method.clone());
            tokio::spawn(async move {
                // Held for the whole task, so the slot is released when it
                // ends — including a panic unwind through `guard_frame`.
                let _permit = permit;
                crate::context::with_connection_context(is_tcp, async {
                    if let Some(frame) = panic_guard::guard_frame(
                        &method,
                        rpc_id,
                        host::handle(req, api.as_ref(), Some(&bus), is_local, &reverse),
                    )
                    .await
                    {
                        let _ = out.send_priority(frame).await;
                    }
                })
                .await;
            });
            return true;
        }
        if let Some(req) = browser::classify(value) {
            // Slow path: `browser.exec` awaits an FE-served reverse RPC on this
            // same connection (§12.4), so run it off the read loop for the same
            // reason as `host::classify` — inline would block frame reads until
            // the reverse timeout.
            let permit = match limiter.try_acquire() {
                Ok(permit) => permit,
                Err(overloaded) => {
                    return reject_overloaded(&method, rpc_id, out_tx, overloaded).await
                }
            };
            let out = out_tx.clone();
            let reverse = reverse.clone();
            let is_tcp = crate::context::is_tcp_connection();
            let (rpc_id, method) = (rpc_id.clone(), method.clone());
            tokio::spawn(async move {
                let _permit = permit;
                crate::context::with_connection_context(is_tcp, async {
                    if let Some(frame) =
                        panic_guard::guard_frame(&method, rpc_id, browser::handle(req, &reverse))
                            .await
                    {
                        let _ = out.send_priority(frame).await;
                    }
                })
                .await;
            });
            return true;
        }
        if let Some(req) = forward::classify(value) {
            let frame = panic_guard::guard_frame(
                &method,
                rpc_id.clone(),
                forward::handle(req, forwards, is_local),
            )
            .await;
            return match frame {
                Some(frame) => out_tx.send_priority(frame).await.is_ok(),
                None => true,
            };
        }
        if let Some(req) = client::classify(value) {
            let frame = panic_guard::guard_frame(
                &method,
                rpc_id.clone(),
                client::handle(req, api.as_ref(), client_id, is_local),
            )
            .await;
            return match frame {
                Some(frame) => out_tx.send_priority(frame).await.is_ok(),
                None => true,
            };
        }
        if let Some(req) = drafts::classify(value) {
            let frame = panic_guard::guard_frame(
                &method,
                rpc_id.clone(),
                drafts::handle(req, api.as_ref(), client_id),
            )
            .await;
            return match frame {
                Some(frame) => out_tx.send_priority(frame).await.is_ok(),
                None => true,
            };
        }
        // `AssertUnwindSafe` over `&mut ConnSubs` is sound here: if a
        // subscribe handler panics after `tokio::spawn(forward_*)` but before
        // `subs.insert(...)`, the forwarder is spawned but unregistered, so
        // unsubscribe/`replaceGroup` can't reach it. The leak is bounded — the
        // forwarder exits when its `out_tx.send` fails after the connection
        // closes — and the registry itself is never left mid-mutation.
        if let Some(sub) = subscriptions::classify(value) {
            return panic_guard::guard_send(
                &method,
                rpc_id.clone(),
                out_tx,
                handle_sub_fast_path(sub, api, bus, out_tx, subs),
            )
            .await;
        }
        if let Some(fast_path) = events::classify(value) {
            return panic_guard::guard_send(
                &method,
                rpc_id.clone(),
                out_tx,
                handle_fast_path(fast_path, bus, out_tx, subs),
            )
            .await;
        }
    }
    // Slow path: the ported-methods dispatcher can touch any service, so spawn
    // it too. Owns the raw frame so the read loop can advance to the next line.
    // Thread connection context (UDS vs TCP) through so ServerControl can guard
    // self-terminating stop calls.
    // Identity comes from the already-parsed frame — no re-parse of `raw`.
    // An unparseable frame gets `id: null` (per JSON-RPC, error responses to
    // unknown/invalid frames use a null id), so a panic in `handle_message`
    // (which owns the -32700 reply) still yields a response instead of
    // silently hanging the client.
    let (rpc_id, method) = parsed.as_ref().map_or_else(
        || (Some(Value::Null), String::new()),
        panic_guard::request_identity,
    );
    // A frame that fails parse/envelope validation never reaches a service:
    // `handle_message` answers it with `-32700`/`-32600` immediately. Gating it
    // on the limiter would mask those codes behind `-32011` (and would silently
    // drop an invalid notification-shaped frame that the router must still
    // answer), so run it inline — it is pure, instant work on the read loop.
    if !is_dispatchable(parsed.as_ref()) {
        let frame =
            panic_guard::guard_frame(&method, rpc_id, handle_message(api.as_ref(), raw)).await;
        return match frame {
            Some(frame) => out_tx.send_priority(frame).await.is_ok(),
            None => true,
        };
    }
    let permit = match limiter.try_acquire() {
        Ok(permit) => permit,
        Err(overloaded) => return reject_overloaded(&method, rpc_id, out_tx, overloaded).await,
    };
    let api = api.clone();
    let out_tx = out_tx.clone();
    let raw = raw.to_string();
    let is_tcp = crate::context::is_tcp_connection();
    tokio::spawn(async move {
        let _permit = permit;
        crate::context::with_connection_context(is_tcp, async {
            if let Some(response) =
                panic_guard::guard_frame(&method, rpc_id, handle_message(api.as_ref(), &raw)).await
            {
                let _ = out_tx.send_priority(response).await;
            }
        })
        .await;
    });
    true
}

/// Whether a generic frame can actually reach a service handler, i.e. it
/// passes the envelope validation [`handle_message`] performs before dispatch
/// (§9: `-32700` for unparseable JSON, `-32600` for a bad envelope). Only
/// dispatchable frames are gated by the outstanding-RPC limiter; everything
/// else is answered inline so the error matrix is unchanged under load.
/// Delegates to the router's [`check_envelope`] so both paths share one
/// implementation of the envelope-validity rules.
fn is_dispatchable(parsed: Option<&Value>) -> bool {
    matches!(
        parsed.map(check_envelope),
        Some(EnvelopeCheck::Valid { .. })
    )
}

/// Reject one over-limit slow-path frame (`server.maxOutstandingRpcs`): a
/// request echoes `-32011 "Server overloaded"` with its `id`, a notification
/// (no `id`) is dropped without a response per PROTOCOL §9. Returns `false`
/// only when the outbound channel is closed.
///
/// Sustained overload rejects a frame per read, so only the transition into
/// saturation warns; the individual rejections log at `debug` to keep the log
/// readable exactly when it matters most.
async fn reject_overloaded(
    method: &str,
    rpc_id: Option<Value>,
    out_tx: &OutboundSender,
    overloaded: Overloaded,
) -> bool {
    if overloaded.newly_saturated {
        tracing::warn!(
            method,
            "outstanding slow-path RPC limit reached; rejecting requests with -32011"
        );
    } else {
        tracing::debug!(
            method,
            "rejecting RPC: outstanding slow-path RPC limit reached"
        );
    }
    match rpc_id {
        Some(id) => out_tx
            .send_priority(events::error_frame(
                id,
                OVERLOAD_ERROR_CODE,
                OVERLOAD_ERROR_MESSAGE,
            ))
            .await
            .is_ok(),
        None => !out_tx.is_closed(),
    }
}

/// Handle a classified `events.subscribe` / `events.unsubscribe` request,
/// mirroring `handleSubscribe` / `handleUnsubscribe`. Returns `false` when the
/// outbound channel is closed.
async fn handle_fast_path(
    fast_path: FastPath,
    bus: &EventBus,
    out_tx: &OutboundSender,
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
                // Enqueue the response (priority lane) before spawning the
                // forwarder (bulk lane); the writer drains priority first, so
                // it can never be preceded by an `events.event` notification.
                if id.present {
                    let frame = events::success_frame(
                        id.echo,
                        json!({ "subscriptionId": subscription_id }),
                    );
                    if out_tx.send_priority(frame).await.is_err() {
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
                    return out_tx.send_priority(frame).await.is_ok();
                }
                true
            }
            Err(msg) => send_fast_path_error(id, &msg, out_tx).await,
        },
    }
}

/// Send a `-32602` error for a fast-path request, but only when it had an `id`
/// (notifications get no response). Returns `false` if the channel is closed.
async fn send_fast_path_error(id: events::IdInfo, message: &str, out_tx: &OutboundSender) -> bool {
    if id.present {
        let frame = events::error_frame(id.echo, -32602, message);
        return out_tx.send_priority(frame).await.is_ok();
    }
    true
}

/// Forward filtered events from one bus subscription to the connection's
/// outbound queue (bulk lane) as `events.event` notifications until the bus
/// or connection closes. Aborted by [`ConnSub`] on unsubscribe / disconnect.
///
/// Under backpressure (the bulk lane is full) the high-volume transient
/// types are conflated per key instead of blocking (see [`crate::conflate`]):
/// latest-wins for `agent:stream:activity` / `file:*`, byte-concat for
/// `terminal:data`. Non-conflatable events act as barriers — the buffer is
/// flushed first (a conflated frame always lands before its stream's terminal
/// event, e.g. `terminal:exit`), then the barrier blocks as before. With no
/// congestion the buffer stays empty and every frame passes straight through.
async fn forward_subscription(
    mut subscription: Subscription,
    subscription_id: String,
    out_tx: OutboundSender,
) {
    // Everything this forwarder emits travels on the bulk lane; conflation
    // needs `reserve` / `try_reserve` on it, so hold the lane sender directly.
    let out_tx = out_tx.bulk_sender();
    let mut buffer: ConflationBuffer<EventItem> = ConflationBuffer::new();
    loop {
        tokio::select! {
            biased;
            // Drain a buffered conflated frame as soon as the lane has room.
            permit = out_tx.reserve(), if !buffer.is_empty() => match permit {
                Ok(permit) => {
                    if let Some(item) = buffer.pop() {
                        permit.send(item.into_frame(&subscription_id));
                    }
                }
                Err(_) => return,
            },
            maybe = subscription.recv() => {
                let Some(batch) = maybe else {
                    // Bus closed: flush anything still pending, then stop.
                    let _ = buffer
                        .drain_all(&out_tx, |item| item.into_frame(&subscription_id))
                        .await;
                    return;
                };
                for event in batch {
                    match conflate::event_key(&event) {
                        Some(key) => {
                            let item = EventItem::new(&key, event);
                            let sid = &subscription_id;
                            match conflate::offer(&mut buffer, key, item, &out_tx, |item| {
                                item.into_frame(sid)
                            }) {
                                Enqueue::Closed => return,
                                Enqueue::Sent | Enqueue::Buffered => {}
                                // Buffer at capacity (too many distinct keys /
                                // bytes pending): fall back to the original
                                // blocking backpressure — flush, then send.
                                Enqueue::Overflow(item) => {
                                    if !buffer
                                        .drain_all(&out_tx, |item| item.into_frame(&subscription_id))
                                        .await
                                    {
                                        return;
                                    }
                                    if out_tx.send(item.into_frame(&subscription_id)).await.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                        None => {
                            // Barrier: flush conflated frames first, then block.
                            if !buffer
                                .drain_all(&out_tx, |item| item.into_frame(&subscription_id))
                                .await
                            {
                                return;
                            }
                            let frame =
                                events::build_event_notification(&subscription_id, &event);
                            if out_tx.send(frame).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Handle a classified `note.subscribe` / `note.unsubscribe` request (TB-0 §1).
/// Subscribe wires the bus subscription FIRST (so concurrent mutations are
/// captured), enqueues the `{ subscriptionId }` response, then spawns the
/// forwarder that emits the snapshot (seq 0) and tails deltas. Returns `false`
/// when the outbound channel is closed.
async fn handle_sub_fast_path(
    sub: SubFastPath,
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    out_tx: &OutboundSender,
    subs: &mut ConnSubs,
) -> bool {
    match sub {
        SubFastPath::Subscribe {
            id,
            channel: Channel::Note,
            params,
        } => match subscriptions::parse_subscribe_params(&params) {
            Ok(p) => {
                let subscriptions::NoteSubscribeParams {
                    workspace_id,
                    replace_group,
                } = p;
                if let Some(group) = replace_group.as_deref() {
                    subs.remove_group(group);
                }
                // Subscribe before the snapshot so a mutation racing the read is
                // captured and re-emitted as a delta (idempotent over-delivery,
                // §1.3). Each matched event is delivered individually (no
                // coalescing) to keep `seq` strictly monotonic.
                let subscription = bus.subscribe(SubscriptionFilter {
                    event_types: vec![
                        NOTE_CREATED.to_string(),
                        NOTE_UPDATED.to_string(),
                        NOTE_DELETED.to_string(),
                    ],
                    workspace_id: Some(workspace_id.clone()),
                    batch_window: None,
                    ..Default::default()
                });
                let subscription_id = events::next_subscription_id();
                // Enqueue the response (priority lane) before spawning the
                // forwarder (bulk lane); the writer drains priority first, so it
                // can never be preceded by a `subscription.push` notification (§3.4).
                if id.present {
                    let frame = events::success_frame(
                        id.echo,
                        json!({ "subscriptionId": subscription_id }),
                    );
                    if out_tx.send_priority(frame).await.is_err() {
                        return false;
                    }
                }
                let handle = tokio::spawn(forward_note_subscription(
                    api.clone(),
                    WorkspaceId::from(workspace_id),
                    subscription,
                    subscription_id.clone(),
                    out_tx.clone(),
                ));
                subs.insert(subscription_id, handle, replace_group);
                true
            }
            Err(msg) => send_fast_path_error(id, &msg, out_tx).await,
        },
        // The per-agent `chat` channel (CS-0) reuses the same
        // subscribe-before-snapshot discipline as the note channel, but is
        // scoped by `agentId` (not `workspaceId`) and emits a `messages[]`
        // object snapshot from `agent.getConversation`. The bus subscription
        // tails the chat stream family (`chat:stream:delta` + tool/end/message
        // signals); the forwarder isolates it to one agent by
        // `sessionId == agentId`.
        SubFastPath::Subscribe {
            id,
            channel: Channel::Chat,
            params,
        } => match subscriptions::parse_chat_subscribe_params(&params) {
            Ok(p) => {
                let subscriptions::ChatSubscribeParams {
                    agent_id,
                    since_message_id,
                    delta_encoding,
                    projection,
                    replace_group,
                } = p;
                if let Some(group) = replace_group.as_deref() {
                    subs.remove_group(group);
                }
                // The chat channel is per-agent, not workspace-scoped, so the
                // bus filter carries no `workspaceId`; the forwarder narrows the
                // stream family to this agent (cross-agent isolation).
                let subscription = bus.subscribe(SubscriptionFilter {
                    event_types: subscriptions::channel_event_types(Channel::Chat),
                    workspace_id: None,
                    batch_window: None,
                    ..Default::default()
                });
                let subscription_id = events::next_subscription_id();
                if id.present {
                    let frame = events::success_frame(
                        id.echo,
                        json!({ "subscriptionId": subscription_id }),
                    );
                    if out_tx.send_priority(frame).await.is_err() {
                        return false;
                    }
                }
                let handle = tokio::spawn(forward_chat_subscription(
                    api.clone(),
                    AgentId::from(agent_id),
                    since_message_id,
                    delta_encoding,
                    projection,
                    subscription,
                    subscription_id.clone(),
                    out_tx.clone(),
                ));
                subs.insert(subscription_id, handle, replace_group);
                true
            }
            Err(msg) => send_fast_path_error(id, &msg, out_tx).await,
        },
        // TB-5 channels (`task`/`agent`/`workspace`/`comment`) reuse the same
        // subscribe-before-snapshot discipline as the note channel; only the
        // param scope, bus filter, and snapshot/delta mapper differ.
        SubFastPath::Subscribe {
            id,
            channel,
            params,
        } => {
            let parsed = parse_channel_params(channel, &params);
            match parsed {
                Ok(ChannelParams {
                    workspace_id,
                    note_id,
                    replace_group,
                }) => {
                    if let Some(group) = replace_group.as_deref() {
                        subs.remove_group(group);
                    }
                    let filter_ws = if subscriptions::channel_is_global(channel) {
                        None
                    } else {
                        workspace_id.clone()
                    };
                    let subscription = bus.subscribe(SubscriptionFilter {
                        event_types: subscriptions::channel_event_types(channel),
                        workspace_id: filter_ws,
                        batch_window: None,
                        ..Default::default()
                    });
                    let subscription_id = events::next_subscription_id();
                    if id.present {
                        let frame = events::success_frame(
                            id.echo,
                            json!({ "subscriptionId": subscription_id }),
                        );
                        if out_tx.send_priority(frame).await.is_err() {
                            return false;
                        }
                    }
                    let handle = tokio::spawn(forward_channel_subscription(
                        api.clone(),
                        channel,
                        workspace_id
                            .map_or_else(|| WorkspaceId::from(String::new()), WorkspaceId::from),
                        note_id.map(NoteId::from),
                        subscription,
                        subscription_id.clone(),
                        out_tx.clone(),
                    ));
                    subs.insert(subscription_id, handle, replace_group);
                    true
                }
                Err(msg) => send_fast_path_error(id, &msg, out_tx).await,
            }
        }
        SubFastPath::Unsubscribe { id, params } => match events::parse_unsubscribe_id(&params) {
            Ok(subscription_id) => {
                let success = subs.remove(&subscription_id);
                if id.present {
                    let frame = events::success_frame(id.echo, json!({ "success": success }));
                    return out_tx.send_priority(frame).await.is_ok();
                }
                true
            }
            Err(msg) => send_fast_path_error(id, &msg, out_tx).await,
        },
    }
}

/// Per-subscription forwarder for the `note` collection channel. Materializes
/// the snapshot (seq 0) from `list_notes`, then maps each `note:*` change event
/// to a `{ added, updated, removedIds }` delta (re-reading the entity, §2.2) at
/// the next seq. Owns `seq`, so strict monotonicity holds without shared state.
/// Aborted by [`ConnSub`] on unsubscribe / disconnect.
async fn forward_note_subscription(
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    mut subscription: Subscription,
    subscription_id: String,
    out_tx: OutboundSender,
) {
    let snapshot = match api.list_notes(&workspace_id).await {
        Ok(notes) => serde_json::to_value(notes).unwrap_or_else(|_| Value::Array(Vec::new())),
        Err(_) => Value::Array(Vec::new()),
    };
    let frame = subscriptions::build_snapshot_push(&subscription_id, 0, &snapshot);
    if out_tx.send_bulk(frame).await.is_err() {
        return;
    }
    let mut seq: u64 = 1;
    while let Some(batch) = subscription.recv().await {
        for event in batch {
            if let Some(delta) =
                subscriptions::note_delta(api.as_ref(), &workspace_id, &event).await
            {
                let frame = subscriptions::build_delta_push(&subscription_id, seq, &delta);
                if out_tx.send_bulk(frame).await.is_err() {
                    return;
                }
                seq += 1;
            }
        }
    }
}

/// Per-subscription forwarder for the per-agent `chat` channel (CS-0). Pushes
/// the seq-0 snapshot — the newest `agent.getConversation` page as the
/// `messages[]` object (CS-0 D3) — then tails the chat stream family
/// (`chat:stream:delta` + tool/end/message signals) FILTERED to this agent
/// (`sessionId == agentId`, cross-agent isolation).
///
/// The forwarder owns the filtered-subscription lifecycle (aborted by
/// [`ConnSub`] on unsubscribe / disconnect), the seq-0 snapshot, AND the
/// monotonic per-subscription delta `seq` (1, 2, …). Each tailed chat-family
/// event is translated by the stateful [`subscriptions::ChatDeltaState`] mapper
/// into a `{ added, updated, removedIds }` block delta (CS-0 D2/D4/D6) pushed in
/// strict seq order; `stream:end` reconciles against the persisted message so the
/// snapshot + deltas equal a fresh `getConversation` snapshot (CS-3).
///
/// `since_message_id` (§7.1 resume) trims the seq-0 snapshot to the messages
/// after that id (`resumed: true`) or falls back to the standard full page
/// (`resumed: false`) — see [`subscriptions::chat_snapshot`].
///
/// **Lag self-heal.** The broadcast ring drops this subscriber's oldest
/// undelivered events when it falls behind (slow consumer — e.g. the bulk lane
/// starved by large priority-lane responses). The loss is silent on the wire:
/// `seq` is assigned only to frames actually sent, so the client sees no gap —
/// a dropped turn tail (`chat:stream:delta` + `agent:stream:end`) would strand
/// the transcript mid-turn forever. On an in-band [`Delivery::Lagged`] marker
/// the forwarder therefore re-emits a fresh snapshot at the next `seq` (the
/// client's reconciler rebuilds from any snapshot with `seq >= expected`) and
/// reseeds the mapper from it — chosen over replaying the finalize/reconcile
/// path because a drop can also swallow whole `agent:message` rows and
/// tool-call events that reconcile (scoped to the in-flight assistant message)
/// would never restore, while the snapshot converges every §7.1 case with zero
/// protocol churn. Cost stays bounded: one
/// [`subscriptions::chat_recovery_snapshot`] (one server-clamped newest page,
/// retried at most once) per lag burst — queued markers and batches are
/// drained first, so a burst coalesces into ONE recovery read, and the
/// discarded queued events are already reflected in the snapshot (persisted
/// rows in the page, in-flight streamed content via the live-turn slot merge —
/// the same coherence argument as a fresh mid-turn subscribe; the one
/// qualifier: `route_notification` publishes a transient chunk delta BEFORE
/// updating the live-turn slot, so the snapshot can lag the discarded queue by
/// at most that one in-flight chunk, which the next full-text delta or the
/// turn-end persist supersedes — it can never strand).
///
/// Unlike the seq-0 snapshot, recovery must NOT degrade to an empty page on a
/// read failure: it lands at a LATER `seq` than content the client already
/// rendered, so an empty value would rebuild the transcript as blank. On a
/// persistent read failure the recovery stays PENDING — batches are discarded
/// (the eventual snapshot supersedes them) and the read is re-attempted on
/// the next delivery or after [`CHAT_RECOVERY_RETRY`], whichever comes first,
/// so the client keeps its rendered transcript until a good page converges it.
#[allow(clippy::too_many_arguments)]
async fn forward_chat_subscription(
    api: Arc<dyn WorkspaceApi>,
    agent_id: AgentId,
    since_message_id: Option<String>,
    delta_encoding: subscriptions::DeltaEncoding,
    projection: Option<intent_core::ConversationProjection>,
    mut subscription: Subscription,
    subscription_id: String,
    out_tx: OutboundSender,
) {
    // Everything this forwarder emits travels on the bulk lane; conflation
    // needs `reserve` / `try_reserve` on it, so hold the lane sender directly.
    let out_tx = out_tx.bulk_sender();
    let mut snapshot = subscriptions::chat_snapshot(
        api.as_ref(),
        &agent_id,
        since_message_id.as_deref(),
        projection,
    )
    .await;
    subscriptions::stamp_delta_encoding(&mut snapshot, delta_encoding);
    let frame = subscriptions::build_snapshot_push(&subscription_id, 0, &snapshot);
    if out_tx.send(frame).await.is_err() {
        return;
    }
    let mut state = subscriptions::ChatDeltaState::new(&agent_id, delta_encoding, projection);
    // Mid-turn resume (CS-0 D5): if the snapshot carried an in-flight message,
    // seed the delta state from it so the next chunk continues the streamed text
    // (full-text deltas append server-side; incremental deltas append
    // client-side onto the snapshot's text) instead of restarting from empty.
    state.seed_from_snapshot(&snapshot);
    // Backpressure conflation (see `crate::conflate`): built chunk deltas are
    // conflatable per block — full-text deltas merge latest-wins (each
    // carries the FULL accumulated text, D2, so the newest supersedes);
    // incremental deltas merge by `textDelta` concat (append-only fragments
    // compose, same as `terminal:data`). The buffer sits post-state (the
    // mapper consumed every event in order) and pre-seq: `seq` is assigned
    // only when a frame actually goes out, staying contiguous. Every other
    // delta (tool calls, `stream:end` reconcile, message rows) is a barrier
    // that flushes the buffer first, so a conflated chunk always lands before
    // its turn's terminal frame.
    let mut buffer: ConflationBuffer<ChatItem> = ConflationBuffer::new();
    let mut seq: u64 = 1;
    // `Some(skipped)` while a lag recovery snapshot is owed but not yet
    // emitted (read failed persistently); cleared once a good page goes out.
    let mut pending_recovery: Option<u64> = None;
    loop {
        tokio::select! {
            biased;
            // Drain a buffered conflated delta as soon as the lane has room.
            permit = out_tx.reserve(), if !buffer.is_empty() => match permit {
                Ok(permit) => {
                    if let Some(item) = buffer.pop() {
                        permit.send(item.into_frame(&subscription_id, seq));
                        seq += 1;
                    }
                }
                Err(_) => return,
            },
            // A pending recovery with a quiet bus: retry on a timer so the
            // client is not left stale until the next event happens to arrive.
            () = tokio::time::sleep(CHAT_RECOVERY_RETRY), if pending_recovery.is_some() => {
                if !attempt_chat_recovery(
                    api.as_ref(), &agent_id, &subscription_id, delta_encoding, projection,
                    &mut seq, &out_tx, &mut state, &mut pending_recovery,
                ).await {
                    return;
                }
            }
            maybe = subscription.recv_delivery() => {
                let Some(delivery) = maybe else {
                    let _ = buffer
                        .drain_all(&out_tx, |item| {
                            let frame = item.into_frame(&subscription_id, seq);
                            seq += 1;
                            frame
                        })
                        .await;
                    return;
                };
                let batch = match delivery {
                    // While a recovery is owed, batches are discarded — the
                    // recovery snapshot (read after they were published)
                    // supersedes them — and each delivery re-attempts the read.
                    Delivery::Batch(_) if pending_recovery.is_some() => {
                        if !attempt_chat_recovery(
                            api.as_ref(), &agent_id, &subscription_id, delta_encoding, projection,
                            &mut seq, &out_tx, &mut state, &mut pending_recovery,
                        ).await {
                            return;
                        }
                        continue;
                    }
                    Delivery::Batch(batch) => batch,
                    // Upstream loss: the ring dropped events before delivery,
                    // possibly this agent's turn tail. Self-heal by re-emitting
                    // a fresh bounded snapshot at the next seq (see the doc
                    // comment above). Coalesce first: discard everything still
                    // queued (all published before the snapshot read below, so
                    // the snapshot supersedes it — persisted rows land in the
                    // page, in-flight content rides the live-turn slot merge)
                    // and fold further lag markers in, so one burst triggers
                    // exactly ONE bounded recovery read. Pending conflated
                    // deltas are superseded the same way and dropped with the
                    // pre-lag mapper state.
                    Delivery::Lagged(n) => {
                        let mut skipped = n;
                        while let Some(queued) = subscription.try_recv_delivery() {
                            if let Delivery::Lagged(more) = queued {
                                skipped = skipped.saturating_add(more);
                            }
                        }
                        tracing::warn!(
                            agent = %agent_id,
                            skipped,
                            "chat subscription lagged; re-emitting a fresh snapshot to converge"
                        );
                        buffer = ConflationBuffer::new();
                        pending_recovery = Some(
                            pending_recovery.unwrap_or(0).saturating_add(skipped),
                        );
                        if !attempt_chat_recovery(
                            api.as_ref(), &agent_id, &subscription_id, delta_encoding, projection,
                            &mut seq, &out_tx, &mut state, &mut pending_recovery,
                        ).await {
                            return;
                        }
                        continue;
                    }
                };
                for event in batch {
                    // Cross-agent isolation: only this agent's stream events
                    // belong to this subscription.
                    if event.session_id.as_deref() != Some(agent_id.as_str()) {
                        continue;
                    }
                    let conflatable = conflate::chat_event_conflatable(&event);
                    let Some(delta) = state.delta(api.as_ref(), &event).await else {
                        continue;
                    };
                    if conflatable {
                        if let Some((key, item)) = ChatItem::from_delta(&delta) {
                            let (sid, s) = (&subscription_id, &mut seq);
                            match conflate::offer(&mut buffer, key, item, &out_tx, |item| {
                                let frame = item.into_frame(sid, *s);
                                *s += 1;
                                frame
                            }) {
                                Enqueue::Closed => return,
                                Enqueue::Sent | Enqueue::Buffered => continue,
                                // Buffer at capacity: fall back to the
                                // original blocking backpressure — flush,
                                // then send this delta at the next seq.
                                Enqueue::Overflow(item) => {
                                    let drained = buffer
                                        .drain_all(&out_tx, |item| {
                                            let frame = item.into_frame(&subscription_id, seq);
                                            seq += 1;
                                            frame
                                        })
                                        .await;
                                    if !drained {
                                        return;
                                    }
                                    let frame = item.into_frame(&subscription_id, seq);
                                    seq += 1;
                                    if out_tx.send(frame).await.is_err() {
                                        return;
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                    // Barrier: flush conflated deltas first, then block.
                    let drained = buffer
                        .drain_all(&out_tx, |item| {
                            let frame = item.into_frame(&subscription_id, seq);
                            seq += 1;
                            frame
                        })
                        .await;
                    if !drained {
                        return;
                    }
                    let frame = subscriptions::build_delta_push(&subscription_id, seq, &delta);
                    seq += 1;
                    if out_tx.send(frame).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

/// Retry cadence for a pending lag recovery whose bounded page read keeps
/// failing (see [`forward_chat_subscription`]). Long enough to give a
/// transient store failure room to clear, short enough that a quiet bus does
/// not leave the client stale for long.
const CHAT_RECOVERY_RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// One attempt at the owed lag-recovery snapshot: read the bounded page
/// (fallibly — see [`subscriptions::chat_recovery_snapshot`]), and on success
/// emit it at the next `seq`, reseed the mapper, and clear the pending flag.
/// On a failed read the recovery stays pending for the caller to re-attempt.
/// Returns `false` only when the outbound lane is closed (caller returns).
#[allow(clippy::too_many_arguments)]
async fn attempt_chat_recovery(
    api: &dyn WorkspaceApi,
    agent_id: &AgentId,
    subscription_id: &str,
    delta_encoding: subscriptions::DeltaEncoding,
    projection: Option<intent_core::ConversationProjection>,
    seq: &mut u64,
    out_tx: &mpsc::Sender<String>,
    state: &mut subscriptions::ChatDeltaState,
    pending_recovery: &mut Option<u64>,
) -> bool {
    let Some(mut snapshot) = subscriptions::chat_recovery_snapshot(api, agent_id, projection).await
    else {
        tracing::warn!(
            agent = %agent_id,
            "chat lag recovery read failed; keeping recovery pending"
        );
        return true;
    };
    subscriptions::stamp_delta_encoding(&mut snapshot, delta_encoding);
    let frame = subscriptions::build_snapshot_push(subscription_id, *seq, &snapshot);
    *seq += 1;
    if out_tx.send(frame).await.is_err() {
        return false;
    }
    *state = subscriptions::ChatDeltaState::new(agent_id, delta_encoding, projection);
    state.seed_from_snapshot(&snapshot);
    *pending_recovery = None;
    true
}

/// The resolved scope of a TB-5 channel subscribe: `workspace_id` is `None` only
/// for the global `workspace` channel; `note_id` is `Some` only for the
/// per-note `comment` channel.
struct ChannelParams {
    workspace_id: Option<String>,
    note_id: Option<String>,
    replace_group: Option<String>,
}

/// Parse a TB-5 channel's subscribe params into the common [`ChannelParams`]
/// scope, validating the required fields per channel (§6.2).
fn parse_channel_params(
    channel: Channel,
    params: &serde_json::Map<String, Value>,
) -> Result<ChannelParams, String> {
    match channel {
        Channel::Workspace => {
            let p = subscriptions::parse_workspace_subscribe_params(params)?;
            Ok(ChannelParams {
                workspace_id: None,
                note_id: None,
                replace_group: p.replace_group,
            })
        }
        Channel::Comment => {
            let p = subscriptions::parse_comment_subscribe_params(params)?;
            Ok(ChannelParams {
                workspace_id: Some(p.workspace_id),
                note_id: Some(p.note_id),
                replace_group: p.replace_group,
            })
        }
        // `note`/`task`/`agent` are all workspace-scoped collections.
        _ => {
            let p = subscriptions::parse_subscribe_params(params)?;
            Ok(ChannelParams {
                workspace_id: Some(p.workspace_id),
                note_id: None,
                replace_group: p.replace_group,
            })
        }
    }
}

/// Per-subscription forwarder for the TB-5 channels. Mirrors
/// [`forward_note_subscription`] but dispatches snapshot + delta through the
/// channel-generic [`subscriptions::channel_snapshot`] /
/// [`subscriptions::channel_delta`] — except the task channel, which routes
/// through the stateful [`subscriptions::task_snapshot`] /
/// [`subscriptions::task_delta`] pair so spec-body edits refresh flipped
/// `specLinked` flags (monorepo#2407). Owns `seq` for strict monotonicity;
/// aborted by [`ConnSub`] on unsubscribe / disconnect.
async fn forward_channel_subscription(
    api: Arc<dyn WorkspaceApi>,
    channel: Channel,
    workspace_id: WorkspaceId,
    note_id: Option<NoteId>,
    mut subscription: Subscription,
    subscription_id: String,
    out_tx: OutboundSender,
) {
    // The task channel's delta mapper is stateful: it tracks the spec's
    // task-link set so a spec-body edit can re-emit the rows whose
    // `specLinked` flag flipped (monorepo#2407). Seeded from the snapshot's
    // own `list_notes` read — no extra query.
    let mut spec_links = HashSet::new();
    let snapshot = if channel == Channel::Task {
        let (snapshot, links) = subscriptions::task_snapshot(api.as_ref(), &workspace_id).await;
        spec_links = links;
        snapshot
    } else {
        subscriptions::channel_snapshot(api.as_ref(), channel, &workspace_id, note_id.as_ref())
            .await
    };
    let frame = subscriptions::build_snapshot_push(&subscription_id, 0, &snapshot);
    if out_tx.send_bulk(frame).await.is_err() {
        return;
    }
    let mut seq: u64 = 1;
    while let Some(batch) = subscription.recv().await {
        for event in batch {
            let delta = if channel == Channel::Task {
                subscriptions::task_delta(api.as_ref(), &workspace_id, &event, &mut spec_links)
                    .await
            } else {
                subscriptions::channel_delta(
                    api.as_ref(),
                    channel,
                    &workspace_id,
                    note_id.as_ref(),
                    &event,
                )
                .await
            };
            if let Some(delta) = delta {
                let frame = subscriptions::build_delta_push(&subscription_id, seq, &delta);
                if out_tx.send_bulk(frame).await.is_err() {
                    return;
                }
                seq += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request rejected at the outstanding-RPC cap answers `-32011 "Server
    /// overloaded"` echoing its id, and the connection stays open.
    #[tokio::test]
    async fn overload_rejection_echoes_the_request_id() {
        let (tx, mut rx) = outbound_channel();
        let open = reject_overloaded(
            "agent.list",
            Some(json!("req-7")),
            &tx,
            Overloaded {
                newly_saturated: true,
            },
        )
        .await;
        assert!(open, "the connection must stay open");
        // The overload error is a response frame → priority lane.
        let frame = rx.priority.try_recv().expect("frame queued");
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], json!("req-7"));
        assert_eq!(v["error"]["code"], json!(OVERLOAD_ERROR_CODE));
        assert_eq!(v["error"]["message"], OVERLOAD_ERROR_MESSAGE);
    }

    /// A notification (no `id`) is dropped silently at the cap — JSON-RPC
    /// notifications never get a response (PROTOCOL §9).
    #[tokio::test]
    async fn overload_rejection_drops_notifications_without_a_frame() {
        let (tx, mut rx) = outbound_channel();
        let open = reject_overloaded(
            "agent.list",
            None,
            &tx,
            Overloaded {
                newly_saturated: false,
            },
        )
        .await;
        assert!(open, "the connection must stay open");
        assert!(
            rx.priority.try_recv().is_err() && rx.bulk.try_recv().is_err(),
            "notifications get no response"
        );
    }

    /// Only frames that survive envelope validation are gated by the limiter;
    /// unparseable and invalid frames stay on the inline path so the router
    /// keeps owning `-32700` / `-32600` even at the cap.
    #[test]
    fn only_valid_envelopes_are_limiter_gated() {
        let parse = |raw: &str| serde_json::from_str::<Value>(raw).ok();
        assert!(is_dispatchable(
            parse(r#"{"jsonrpc":"2.0","id":1,"method":"agent.list"}"#).as_ref()
        ));
        assert!(
            is_dispatchable(parse(r#"{"jsonrpc":"2.0","method":"agent.list"}"#).as_ref()),
            "a valid notification still dispatches"
        );
        for invalid in [
            "not json",
            "[1,2,3]",
            r#"{"jsonrpc":"1.0","method":"agent.list"}"#,
            r#"{"id":1,"method":"agent.list"}"#,
            r#"{"jsonrpc":"2.0","id":1}"#,
            r#"{"jsonrpc":"2.0","id":1,"method":""}"#,
            r#"{"jsonrpc":"2.0","id":{"a":1},"method":"agent.list"}"#,
        ] {
            assert!(
                !is_dispatchable(parse(invalid).as_ref()),
                "must not be limiter-gated: {invalid}"
            );
        }
    }

    /// Pins the pre-check ↔ router equivalence: for every frame,
    /// `is_dispatchable` returns true iff `handle_message` does NOT answer
    /// with the parse/envelope errors (`-32700`/`-32600`) it owns.
    #[tokio::test]
    async fn is_dispatchable_matches_handle_message_envelope_errors() {
        struct NoopApi;
        impl WorkspaceApi for NoopApi {}

        for (raw, dispatchable) in [
            // Valid request (unknown method still dispatches: -32601, not -32600).
            (r#"{"jsonrpc":"2.0","id":1,"method":"agent.list"}"#, true),
            (r#"{"jsonrpc":"2.0","id":1,"method":"nope.method"}"#, true),
            // Valid notification (dispatched; no response frame at all).
            (r#"{"jsonrpc":"2.0","method":"agent.list"}"#, true),
            // id: null present is a valid request id.
            (r#"{"jsonrpc":"2.0","id":null,"method":"agent.list"}"#, true),
            ("not json", false),
            ("[1,2,3]", false),
            (r#""just a string""#, false),
            (r#"{"jsonrpc":"1.0","id":1,"method":"agent.list"}"#, false),
            // Invalid notification-shaped frame: still answered with -32600.
            (r#"{"jsonrpc":"1.0","method":"agent.list"}"#, false),
            (r#"{"id":1,"method":"agent.list"}"#, false),
            (r#"{"jsonrpc":"2.0","id":1}"#, false),
            (r#"{"jsonrpc":"2.0","id":1,"method":""}"#, false),
            (r#"{"jsonrpc":"2.0","id":1,"method":7}"#, false),
            (
                r#"{"jsonrpc":"2.0","id":{"a":1},"method":"agent.list"}"#,
                false,
            ),
            (
                r#"{"jsonrpc":"2.0","id":true,"method":"agent.list"}"#,
                false,
            ),
        ] {
            let parsed = serde_json::from_str::<Value>(raw).ok();
            assert_eq!(
                is_dispatchable(parsed.as_ref()),
                dispatchable,
                "is_dispatchable: {raw}"
            );
            let envelope_error = match handle_message(&NoopApi, raw).await {
                None => false,
                Some(frame) => {
                    let v: Value = serde_json::from_str(&frame).expect("valid json response");
                    matches!(v["error"]["code"].as_i64(), Some(-32700) | Some(-32600))
                }
            };
            assert_eq!(
                dispatchable, !envelope_error,
                "handle_message envelope-error mismatch: {raw}"
            );
        }
    }

    /// A closed outbound channel is reported so the read loop can end.
    #[tokio::test]
    async fn overload_rejection_reports_a_closed_channel() {
        let (tx, rx) = outbound_channel();
        drop(rx);
        assert!(
            !reject_overloaded(
                "agent.list",
                Some(json!(1)),
                &tx,
                Overloaded {
                    newly_saturated: true,
                },
            )
            .await
        );
        assert!(
            !reject_overloaded(
                "agent.list",
                None,
                &tx,
                Overloaded {
                    newly_saturated: false,
                },
            )
            .await
        );
    }

    /// The writer-facing drain contract: `OutboundReceiver::recv` empties the
    /// priority lane before yielding any bulk frame, even when the bulk frames
    /// were queued first.
    #[tokio::test]
    async fn outbound_recv_drains_priority_before_bulk() {
        let (tx, mut rx) = outbound_channel();
        tx.send_bulk("bulk-1".to_string()).await.unwrap();
        tx.send_bulk("bulk-2".to_string()).await.unwrap();
        tx.send_priority("prio-1".to_string()).await.unwrap();
        tx.send_priority("prio-2".to_string()).await.unwrap();
        let mut order = Vec::new();
        for _ in 0..4 {
            order.push(rx.recv().await.expect("frame"));
        }
        assert_eq!(order, ["prio-1", "prio-2", "bulk-1", "bulk-2"]);
    }

    /// FIFO order holds within each lane, and interleaved sends drain
    /// priority-first at every step.
    #[tokio::test]
    async fn outbound_recv_keeps_fifo_within_each_lane() {
        let (tx, mut rx) = outbound_channel();
        tx.send_priority("p1".to_string()).await.unwrap();
        tx.send_bulk("b1".to_string()).await.unwrap();
        tx.send_priority("p2".to_string()).await.unwrap();
        assert_eq!(rx.recv().await.as_deref(), Some("p1"));
        assert_eq!(rx.recv().await.as_deref(), Some("p2"));
        assert_eq!(rx.recv().await.as_deref(), Some("b1"));
        // A priority frame queued while only bulk remains still wins the next
        // recv.
        tx.send_bulk("b2".to_string()).await.unwrap();
        tx.send_priority("p3".to_string()).await.unwrap();
        assert_eq!(rx.recv().await.as_deref(), Some("p3"));
        assert_eq!(rx.recv().await.as_deref(), Some("b2"));
    }

    /// `recv` returns `None` only after every sender is dropped AND both lanes
    /// are fully drained; frames buffered at close time are not lost.
    #[tokio::test]
    async fn outbound_recv_drains_buffered_frames_after_close() {
        let (tx, mut rx) = outbound_channel();
        tx.send_bulk("b1".to_string()).await.unwrap();
        tx.send_priority("p1".to_string()).await.unwrap();
        drop(tx);
        assert_eq!(rx.recv().await.as_deref(), Some("p1"));
        assert_eq!(rx.recv().await.as_deref(), Some("b1"));
        assert_eq!(rx.recv().await, None);
        assert_eq!(rx.recv().await, None, "recv stays terminal after None");
    }

    /// `recv` wakes for a frame that arrives while it is parked (both lanes
    /// empty) — the writer must not deadlock waiting on an idle connection.
    #[tokio::test]
    async fn outbound_recv_wakes_on_late_frames() {
        let (tx, mut rx) = outbound_channel();
        let waiter = tokio::spawn(async move {
            let first = rx.recv().await;
            let second = rx.recv().await;
            (first, second)
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        tx.send_bulk("b1".to_string()).await.unwrap();
        tx.send_priority("p1".to_string()).await.unwrap();
        let (first, second) = waiter.await.unwrap();
        // Both frames are delivered; the parked recv takes whichever lane
        // wakes it first, then the follow-up recv drains the other.
        let mut got = vec![first.unwrap(), second.unwrap()];
        got.sort();
        assert_eq!(got, ["b1", "p1"]);
    }

    /// A dropped receiver is visible to senders on both lanes (the read loop's
    /// "connection closed" signal).
    #[tokio::test]
    async fn outbound_sender_reports_closed_when_receiver_drops() {
        let (tx, rx) = outbound_channel();
        assert!(!tx.is_closed());
        drop(rx);
        assert!(tx.is_closed());
        assert!(tx.send_priority("p".to_string()).await.is_err());
        assert!(tx.send_bulk("b".to_string()).await.is_err());
        assert!(
            tx.priority_sender().send("p".to_string()).await.is_err(),
            "the raw priority sender observes the close too"
        );
    }
}
