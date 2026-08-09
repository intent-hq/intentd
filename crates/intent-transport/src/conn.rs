//! Transport-agnostic connection orchestration (§6, §16).
//!
//! The per-connection subscription registry and frame routing shared by every
//! listener (UDS in [`crate::listener`], WSS in [`crate::ws`]). Both transports
//! read newline/`Text` frames, run the `events.` fast-path before the JSON-RPC
//! dispatcher, push `events.event` notifications over the same connection, and
//! drop all subscriptions when the connection closes — so the wire result is
//! identical regardless of transport. The only difference is framing, which the
//! transports handle by draining an outbound `mpsc::Sender<String>`.

use intent_core::events::{NOTE_CREATED, NOTE_DELETED, NOTE_UPDATED};
use intent_core::{AgentId, ClientId, NoteId, WorkspaceApi, WorkspaceId};
use intent_services::{EventBus, Subscription, SubscriptionFilter};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::browser;
use crate::client;
use crate::control::{self, SystemControl};
use crate::drafts;
use crate::events::{self, FastPath};
use crate::forward::{self, ForwardRegistry};
use crate::host;
use crate::panic_guard;
use crate::reverse::ReverseChannel;
use crate::router::handle_message;
use crate::rpc_limit::{RpcLimiter, OVERLOAD_ERROR_CODE, OVERLOAD_ERROR_MESSAGE};
use crate::subscriptions::{self, Channel, SubFastPath};

/// Capacity of the per-connection outbound frame queue (responses + pushed
/// notifications are serialized through one writer so they never interleave).
pub(crate) const OUTBOUND_CAPACITY: usize = 256;

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
/// frame through a cloned `out_tx`, so a long-running request (e.g.
/// `host.exec`) cannot delay responses to other requests on the same connection.
/// Out-of-order responses are fine: JSON-RPC correlates by `id`.
///
/// Every detached spawn claims a slot from the daemon-wide `limiter`
/// (`server.maxOutstandingRpcs`) first; when the cap is reached the request is
/// rejected immediately with `-32011 "Server overloaded"` (notifications are
/// dropped silently) rather than queued. The permit is moved into the spawned
/// task, so the slot is released when the task ends — panic unwinds included.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_frame(
    raw: &str,
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    out_tx: &mpsc::Sender<String>,
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
                    Some(frame) => out_tx.send(frame).await.is_ok(),
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
                    Some(frame) => out_tx.send(frame).await.is_ok(),
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
                    Some(frame) => out_tx.send(frame).await.is_ok(),
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
            let Ok(permit) = limiter.try_acquire() else {
                return reject_overloaded(&method, rpc_id, out_tx).await;
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
                        let _ = out.send(frame).await;
                    }
                })
                .await
            });
            return true;
        }
        if let Some(req) = browser::classify(value) {
            // Slow path: `browser.exec` awaits an FE-served reverse RPC on this
            // same connection (§12.4), so run it off the read loop for the same
            // reason as `host::classify` — inline would block frame reads until
            // the reverse timeout.
            let Ok(permit) = limiter.try_acquire() else {
                return reject_overloaded(&method, rpc_id, out_tx).await;
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
                        let _ = out.send(frame).await;
                    }
                })
                .await
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
                Some(frame) => out_tx.send(frame).await.is_ok(),
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
                Some(frame) => out_tx.send(frame).await.is_ok(),
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
                Some(frame) => out_tx.send(frame).await.is_ok(),
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
    let (rpc_id, method) = parsed
        .as_ref()
        .map(panic_guard::request_identity)
        .unwrap_or_else(|| (Some(Value::Null), String::new()));
    let Ok(permit) = limiter.try_acquire() else {
        return reject_overloaded(&method, rpc_id, out_tx).await;
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
                let _ = out_tx.send(response).await;
            }
        })
        .await
    });
    true
}

/// Reject one over-limit slow-path frame (`server.maxOutstandingRpcs`): a
/// request echoes `-32011 "Server overloaded"` with its `id`, a notification
/// (no `id`) is dropped without a response per PROTOCOL §9. Returns `false`
/// only when the outbound channel is closed.
async fn reject_overloaded(
    method: &str,
    rpc_id: Option<Value>,
    out_tx: &mpsc::Sender<String>,
) -> bool {
    tracing::warn!(
        method,
        "rejecting RPC: outstanding slow-path RPC limit reached"
    );
    match rpc_id {
        Some(id) => out_tx
            .send(events::error_frame(
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

/// Handle a classified `note.subscribe` / `note.unsubscribe` request (TB-0 §1).
/// Subscribe wires the bus subscription FIRST (so concurrent mutations are
/// captured), enqueues the `{ subscriptionId }` response, then spawns the
/// forwarder that emits the snapshot (seq 0) and tails deltas. Returns `false`
/// when the outbound channel is closed.
async fn handle_sub_fast_path(
    sub: SubFastPath,
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    out_tx: &mpsc::Sender<String>,
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
                // Enqueue the response before spawning the forwarder so it can
                // never be preceded by a `subscription.push` notification (§3.4).
                if id.present {
                    let frame = events::success_frame(
                        id.echo,
                        json!({ "subscriptionId": subscription_id }),
                    );
                    if out_tx.send(frame).await.is_err() {
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
                    if out_tx.send(frame).await.is_err() {
                        return false;
                    }
                }
                let handle = tokio::spawn(forward_chat_subscription(
                    api.clone(),
                    AgentId::from(agent_id),
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
                        if out_tx.send(frame).await.is_err() {
                            return false;
                        }
                    }
                    let handle = tokio::spawn(forward_channel_subscription(
                        api.clone(),
                        channel,
                        workspace_id
                            .map(WorkspaceId::from)
                            .unwrap_or_else(|| WorkspaceId::from(String::new())),
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
                    return out_tx.send(frame).await.is_ok();
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
    out_tx: mpsc::Sender<String>,
) {
    let snapshot = match api.list_notes(&workspace_id).await {
        Ok(notes) => serde_json::to_value(notes).unwrap_or_else(|_| Value::Array(Vec::new())),
        Err(_) => Value::Array(Vec::new()),
    };
    let frame = subscriptions::build_snapshot_push(&subscription_id, 0, &snapshot);
    if out_tx.send(frame).await.is_err() {
        return;
    }
    let mut seq: u64 = 1;
    while let Some(batch) = subscription.recv().await {
        for event in batch {
            if let Some(delta) =
                subscriptions::note_delta(api.as_ref(), &workspace_id, &event).await
            {
                let frame = subscriptions::build_delta_push(&subscription_id, seq, &delta);
                if out_tx.send(frame).await.is_err() {
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
async fn forward_chat_subscription(
    api: Arc<dyn WorkspaceApi>,
    agent_id: AgentId,
    mut subscription: Subscription,
    subscription_id: String,
    out_tx: mpsc::Sender<String>,
) {
    let snapshot = subscriptions::chat_snapshot(api.as_ref(), &agent_id).await;
    let frame = subscriptions::build_snapshot_push(&subscription_id, 0, &snapshot);
    if out_tx.send(frame).await.is_err() {
        return;
    }
    let mut state = subscriptions::ChatDeltaState::new(&agent_id);
    // Mid-turn resume (CS-0 D5): if the snapshot carried an in-flight message,
    // seed the delta state from it so the next chunk continues the streamed text
    // (full-text deltas) instead of restarting from empty.
    state.seed_from_snapshot(&snapshot);
    let mut seq: u64 = 1;
    while let Some(batch) = subscription.recv().await {
        for event in batch {
            // Cross-agent isolation: only this agent's stream events belong to
            // this subscription.
            if event.session_id.as_deref() != Some(agent_id.as_str()) {
                continue;
            }
            if let Some(delta) = state.delta(api.as_ref(), &event).await {
                let frame = subscriptions::build_delta_push(&subscription_id, seq, &delta);
                seq += 1;
                if out_tx.send(frame).await.is_err() {
                    return;
                }
            }
        }
    }
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
/// [`subscriptions::channel_delta`]. Owns `seq` for strict monotonicity; aborted
/// by [`ConnSub`] on unsubscribe / disconnect.
async fn forward_channel_subscription(
    api: Arc<dyn WorkspaceApi>,
    channel: Channel,
    workspace_id: WorkspaceId,
    note_id: Option<NoteId>,
    mut subscription: Subscription,
    subscription_id: String,
    out_tx: mpsc::Sender<String>,
) {
    let snapshot =
        subscriptions::channel_snapshot(api.as_ref(), channel, &workspace_id, note_id.as_ref())
            .await;
    let frame = subscriptions::build_snapshot_push(&subscription_id, 0, &snapshot);
    if out_tx.send(frame).await.is_err() {
        return;
    }
    let mut seq: u64 = 1;
    while let Some(batch) = subscription.recv().await {
        for event in batch {
            if let Some(delta) = subscriptions::channel_delta(
                api.as_ref(),
                channel,
                &workspace_id,
                note_id.as_ref(),
                &event,
            )
            .await
            {
                let frame = subscriptions::build_delta_push(&subscription_id, seq, &delta);
                if out_tx.send(frame).await.is_err() {
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
        let (tx, mut rx) = mpsc::channel::<String>(4);
        let open = reject_overloaded("agent.list", Some(json!("req-7")), &tx).await;
        assert!(open, "the connection must stay open");
        let frame = rx.try_recv().expect("frame queued");
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
        let (tx, mut rx) = mpsc::channel::<String>(4);
        let open = reject_overloaded("agent.list", None, &tx).await;
        assert!(open, "the connection must stay open");
        assert!(rx.try_recv().is_err(), "notifications get no response");
    }

    /// A closed outbound channel is reported so the read loop can end.
    #[tokio::test]
    async fn overload_rejection_reports_a_closed_channel() {
        let (tx, rx) = mpsc::channel::<String>(4);
        drop(rx);
        assert!(!reject_overloaded("agent.list", Some(json!(1)), &tx).await);
        assert!(!reject_overloaded("agent.list", None, &tx).await);
    }
}
