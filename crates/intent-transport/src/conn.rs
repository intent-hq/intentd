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

use crate::client;
use crate::control::{self, SystemControl};
use crate::drafts;
use crate::events::{self, FastPath};
use crate::forward::{self, ForwardRegistry};
use crate::host;
use crate::reverse::ReverseChannel;
use crate::router::handle_message;
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
/// then intercept the `system.*` control methods (UDS only), the `host.status`
/// capability probe (both transports), the `forward.*` port-forwarding methods,
/// and the `events.` fast-path, else hand to the JSON-RPC dispatcher. `control`
/// is `Some` only on a transport that exposes the control surface (the UDS
/// listener); `forwards`/`reverse` are the connection's port-forward registry
/// and reverse-RPC channel; `client_id` is the connection's logical-client
/// binding, set by `client.hello` and consumed by `drafts.*` (§16); `is_local`
/// reflects that connection's resolved locality (§5.14). Returns `false` when
/// the outbound channel is closed.
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
    client_id: &mut Option<ClientId>,
    is_local: bool,
) -> bool {
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        // A reply to a daemon-initiated reverse request (FE-served intents such
        // as `host.openExternal`, §12.4) — route it to the awaiting caller and
        // never treat it as a client request.
        if reverse.route_response(&value) {
            return true;
        }
        if let Some(control) = control {
            if let Some(req) = control::classify(&value) {
                return match control::handle(req, control.as_ref(), is_local) {
                    Some(frame) => out_tx.send(frame).await.is_ok(),
                    None => true,
                };
            }
        }
        if let Some(req) = host::classify(&value) {
            // `openInEditor` may await an FE-served reverse RPC on this very
            // connection (§5.14), so it must run off the read loop — handling
            // it inline would block frame reads and the client's reverse reply
            // could never be routed (deadlock until the reverse timeout).
            if matches!(req.method, host::HostMethod::OpenInEditor) {
                let api = Arc::clone(api);
                let bus = bus.clone();
                let out = out_tx.clone();
                let reverse = reverse.clone();
                tokio::spawn(async move {
                    if let Some(frame) =
                        host::handle(req, api.as_ref(), Some(&bus), is_local, &reverse).await
                    {
                        let _ = out.send(frame).await;
                    }
                });
                return true;
            }
            return match host::handle(req, api.as_ref(), Some(bus), is_local, reverse).await {
                Some(frame) => out_tx.send(frame).await.is_ok(),
                None => true,
            };
        }
        if let Some(req) = forward::classify(&value) {
            return match forward::handle(req, forwards, is_local).await {
                Some(frame) => out_tx.send(frame).await.is_ok(),
                None => true,
            };
        }
        if let Some(req) = client::classify(&value) {
            return match client::handle(req, api.as_ref(), client_id, is_local).await {
                Some(frame) => out_tx.send(frame).await.is_ok(),
                None => true,
            };
        }
        if let Some(req) = drafts::classify(&value) {
            return match drafts::handle(req, api.as_ref(), client_id).await {
                Some(frame) => out_tx.send(frame).await.is_ok(),
                None => true,
            };
        }
        if let Some(sub) = subscriptions::classify(&value) {
            return handle_sub_fast_path(sub, api, bus, out_tx, subs).await;
        }
        if let Some(fast_path) = events::classify(&value) {
            return handle_fast_path(fast_path, bus, out_tx, subs).await;
        }
    }
    match handle_message(api.as_ref(), raw).await {
        Some(response) => out_tx.send(response).await.is_ok(),
        None => true,
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
        // tails the `agent:stream:*` family; the forwarder isolates it to one
        // agent by `sessionId == agentId`.
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
/// `messages[]` object (CS-0 D3) — then tails the `agent:stream:*` family
/// FILTERED to this agent (`sessionId == agentId`, cross-agent isolation).
///
/// The forwarder owns the filtered-subscription lifecycle (aborted by
/// [`ConnSub`] on unsubscribe / disconnect), the seq-0 snapshot, AND the
/// monotonic per-subscription delta `seq` (1, 2, …). Each tailed `agent:stream:*`
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
