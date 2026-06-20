//! Transport-agnostic connection orchestration (§6, §16).
//!
//! The per-connection subscription registry and frame routing shared by every
//! listener (UDS in [`crate::listener`], WSS in [`crate::ws`]). Both transports
//! read newline/`Text` frames, run the `events.` fast-path before the JSON-RPC
//! dispatcher, push `events.event` notifications over the same connection, and
//! drop all subscriptions when the connection closes — so the wire result is
//! identical regardless of transport. The only difference is framing, which the
//! transports handle by draining an outbound `mpsc::Sender<String>`.

use intent_core::WorkspaceApi;
use intent_services::{EventBus, Subscription, SubscriptionFilter};
use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::events::{self, FastPath};
use crate::router::handle_message;

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

/// Route one frame: intercept the `events.` fast-path, else hand to the
/// JSON-RPC dispatcher. Returns `false` when the outbound channel is closed.
pub(crate) async fn process_frame(
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
