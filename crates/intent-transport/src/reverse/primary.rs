//! Capability-gated, identity-aware reverse-dispatch registry (REV-2).
//!
//! Target selection for agent-initiated reverse RPCs (currently `browser.exec`
//! called via the MCP `ws.browser.exec` binding, PROTOCOL §5.14/§12.4). When
//! the caller has no ambient client connection to reverse-dispatch on, the
//! daemon resolves a [`ReverseTarget`] against the live connections:
//!
//! - A connection is **eligible** only once its `client.hello` (§5.17)
//!   advertised `capabilities.browserExec === true`. Un-hello'd sockets (iOS,
//!   CLIs, dev tooling) and connections without the capability (FE auxiliary
//!   `JsonRpcClient`s) are never candidates — this is what fixes the REV-1
//!   misrouting where the first arrival won regardless of what it could do.
//! - [`ReverseTarget::Default`] → the **first-connected** eligible connection
//!   (unchanged single-desktop behaviour); none → `NoClient`.
//! - [`ReverseTarget::Client`] / [`ReverseTarget::Pinned`] → the **newest**
//!   eligible connection of that logical `clientId`; none →
//!   `ClientOffline { client_id, name, pinned }`.
//!
//! Every accepted UDS or WSS connection registers its per-connection
//! [`ReverseChannel`] with the shared [`PrimaryReverseRegistry`] and holds the
//! returned [`PrimaryReverseGuard`] for the life of the connection. A
//! successful `client.hello` binds the connection's logical identity onto the
//! entry ([`PrimaryReverseGuard::bind`]; a re-hello updates it). Dropping the
//! guard removes the entry — on normal exit, panic-unwind, and task abort
//! (the WSS heartbeat reaper) alike.
//!
//! Logical-client transitions (`client:connected` when a `clientId` gains its
//! first live connection, `client:disconnected` when it loses its last) are
//! recorded as [`ClientTransition`]s **under the registry lock, in mutation
//! order**, and published by one task
//! ([`PrimaryReverseRegistry::spawn_client_event_publisher`]) that drains the
//! queue sequentially. So a stale `client:disconnected` can never overtake
//! the `client:connected` of a same-client reconnect, and an aborted
//! connection's departure is announced exactly like an explicit close.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use intent_core::{
    now_iso, AgentReverseDispatch, BoxFuture, ClientId, ReverseDispatchError, ReverseTarget,
    WorkspaceApi, WorkspaceId,
};
use serde_json::Value;
use tokio::sync::mpsc;

use super::{request_timeout, ReverseChannel};

/// Global event (empty `workspaceId`, like `settings:changed`) published when
/// a logical client gains its first live hello'd connection (REV-2, §6).
pub const CLIENT_CONNECTED: &str = "client:connected";

/// Global event published when a logical client loses its last live hello'd
/// connection (REV-2, §6).
pub const CLIENT_DISCONNECTED: &str = "client:disconnected";

/// Which listener accepted a registered connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReverseTransport {
    Uds,
    Wss,
}

impl ReverseTransport {
    /// Wire spelling (`"uds"` / `"wss"`) for `client.list`-style payloads.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ReverseTransport::Uds => "uds",
            ReverseTransport::Wss => "wss",
        }
    }
}

/// The logical-client identity a connection established via `client.hello`
/// (§5.17). `capabilities` is the hello's `capabilities` object verbatim
/// (`{}` when omitted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReverseClientIdentity {
    pub client_id: ClientId,
    pub name: Option<String>,
    pub capabilities: Value,
}

impl ReverseClientIdentity {
    /// Whether this identity advertised `capabilities.browserExec === true`.
    #[must_use]
    pub fn browser_exec(&self) -> bool {
        self.capabilities
            .get("browserExec")
            .and_then(Value::as_bool)
            == Some(true)
    }

    /// `{ clientId, name?, capabilities }` — the `client:connected` /
    /// `client:disconnected` event payload.
    #[must_use]
    pub fn event_data(&self) -> Value {
        let mut data = serde_json::Map::new();
        data.insert("clientId".into(), self.client_id.as_str().into());
        if let Some(name) = &self.name {
            data.insert("name".into(), name.clone().into());
        }
        data.insert("capabilities".into(), self.capabilities.clone());
        Value::Object(data)
    }
}

/// One logical-client transition, recorded under the registry lock in
/// mutation order (see the module docs) and published as a `client:*` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientTransition {
    /// The `clientId` gained its first live hello'd connection
    /// (⇒ `client:connected`).
    Connected(ReverseClientIdentity),
    /// The `clientId` lost its last live hello'd connection — explicit close,
    /// re-hello under another `clientId`, panic, or task abort
    /// (⇒ `client:disconnected`).
    Disconnected(ReverseClientIdentity),
}

impl ClientTransition {
    /// The event type this transition publishes as.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        match self {
            ClientTransition::Connected(_) => CLIENT_CONNECTED,
            ClientTransition::Disconnected(_) => CLIENT_DISCONNECTED,
        }
    }

    /// The identity carried by the transition.
    #[must_use]
    pub fn identity(&self) -> &ReverseClientIdentity {
        match self {
            ClientTransition::Connected(identity) | ClientTransition::Disconnected(identity) => {
                identity
            }
        }
    }
}

/// One logical client as seen by [`PrimaryReverseRegistry::live_clients`]:
/// every hello'd connection sharing a `clientId`, grouped. `name` and
/// `capabilities` come from the newest connection's hello; `connected_at` is
/// the ISO-8601 registration time of the oldest live connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveClient {
    pub client_id: ClientId,
    pub name: Option<String>,
    pub capabilities: Value,
    pub connections: usize,
    /// One entry per live connection, oldest first.
    pub transports: Vec<ReverseTransport>,
    pub connected_at: String,
}

/// The client a [`ReverseTarget`] resolved to (see
/// [`PrimaryReverseRegistry::resolve`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedClient {
    pub client_id: ClientId,
    pub name: Option<String>,
}

/// Registry of live reverse channels ordered by arrival. Cheap to clone
/// (`Arc` inside).
#[derive(Clone, Default)]
pub struct PrimaryReverseRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    entries: Mutex<VecDeque<Entry>>,
    next_id: AtomicU64,
    /// Ordered transition queue. Every `send` happens while `entries` is
    /// locked, so queue order is exactly registry-mutation order; the sender
    /// never blocks, so it is safe from `Drop` (abort / unwind paths).
    transitions: mpsc::UnboundedSender<ClientTransition>,
    /// The queue's consumer end, parked until a publisher claims it via
    /// [`PrimaryReverseRegistry::take_transitions`].
    transition_rx: Mutex<Option<mpsc::UnboundedReceiver<ClientTransition>>>,
}

impl Default for Inner {
    fn default() -> Self {
        let (transitions, transition_rx) = mpsc::unbounded_channel();
        Self {
            entries: Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(0),
            transitions,
            transition_rx: Mutex::new(Some(transition_rx)),
        }
    }
}

struct Entry {
    /// Monotonic registration sequence — the arrival order.
    id: u64,
    channel: ReverseChannel,
    transport: ReverseTransport,
    connected_at: String,
    /// `None` until the connection's first successful `client.hello`.
    identity: Option<ReverseClientIdentity>,
}

impl Entry {
    fn is_eligible(&self) -> bool {
        self.identity
            .as_ref()
            .is_some_and(ReverseClientIdentity::browser_exec)
    }

    fn has_client(&self, client_id: &ClientId) -> bool {
        self.identity
            .as_ref()
            .is_some_and(|i| &i.client_id == client_id)
    }
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<Entry>> {
        self.entries
            .lock()
            .expect("primary reverse entries poisoned")
    }

    /// Remove entry `id` and return its channel. When the entry was the last
    /// live connection of its logical client, a `Disconnected` transition is
    /// queued before the lock is released.
    fn remove(&self, id: u64) -> Option<ReverseChannel> {
        let mut entries = self.lock();
        let pos = entries.iter().position(|e| e.id == id)?;
        let entry = entries.remove(pos)?;
        if let Some(identity) = entry.identity {
            if !entries.iter().any(|e| e.has_client(&identity.client_id)) {
                let _ = self
                    .transitions
                    .send(ClientTransition::Disconnected(identity));
            }
        }
        Some(entry.channel)
    }
}

/// Resolve `target` against `entries` (arrival order) to the channel that
/// should receive the request.
fn resolve_entry<'a>(
    entries: &'a VecDeque<Entry>,
    target: &ReverseTarget,
) -> Result<&'a Entry, ReverseDispatchError> {
    match target {
        ReverseTarget::Default => entries
            .iter()
            .find(|e| e.is_eligible())
            .ok_or(ReverseDispatchError::NoClient),
        ReverseTarget::Client(client_id) | ReverseTarget::Pinned(client_id) => entries
            .iter()
            .rev()
            .find(|e| e.is_eligible() && e.has_client(client_id))
            .ok_or_else(|| ReverseDispatchError::ClientOffline {
                client_id: client_id.clone(),
                name: entries
                    .iter()
                    .rev()
                    .filter(|e| e.has_client(client_id))
                    .find_map(|e| e.identity.as_ref().and_then(|i| i.name.clone())),
                pinned: matches!(target, ReverseTarget::Pinned(_)),
            }),
    }
}

impl PrimaryReverseRegistry {
    /// Build an empty registry (no clients connected).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `channel` (accepted by `transport`) as a live connection and
    /// return a guard whose drop removes the entry (RAII: the caller holds it
    /// for the connection's lifetime). The connection is not an eligible
    /// reverse target until [`PrimaryReverseGuard::bind`] attaches a
    /// `client.hello` identity advertising `browserExec`.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn register(
        &self,
        channel: ReverseChannel,
        transport: ReverseTransport,
    ) -> PrimaryReverseGuard {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.lock().push_back(Entry {
            id,
            channel,
            transport,
            connected_at: now_iso(),
            identity: None,
        });
        PrimaryReverseGuard {
            registry: Some(self.inner.clone()),
            id,
        }
    }

    /// The channel a [`ReverseTarget::Default`] dispatch would use right now
    /// (the first-connected eligible connection), or `None` when no eligible
    /// client is connected. A cheap clone of the entry's channel; the entry
    /// stays registered.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn primary(&self) -> Option<ReverseChannel> {
        resolve_entry(&self.inner.lock(), &ReverseTarget::Default)
            .ok()
            .map(|e| e.channel.clone())
    }

    /// Resolve `target` without dispatching — the probe behind
    /// `workspace.getBrowserClient`. Same rules and errors as `dispatch`.
    ///
    /// # Errors
    ///
    /// `NoClient` when `Default` finds no eligible connection; `ClientOffline`
    /// when the named client has no live eligible connection.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn resolve(&self, target: &ReverseTarget) -> Result<ResolvedClient, ReverseDispatchError> {
        let entries = self.inner.lock();
        let entry = resolve_entry(&entries, target)?;
        let identity = entry
            .identity
            .as_ref()
            .expect("an eligible entry always carries an identity");
        Ok(ResolvedClient {
            client_id: identity.client_id.clone(),
            name: identity.name.clone(),
        })
    }

    /// Live hello'd connections grouped by `clientId`, ordered by each
    /// client's first connection. Un-hello'd connections are omitted;
    /// ineligible (no `browserExec`) clients are included — this is the
    /// `client.list` projection, not the eligibility set.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn live_clients(&self) -> Vec<LiveClient> {
        let entries = self.inner.lock();
        let mut clients: Vec<LiveClient> = Vec::new();
        for entry in entries.iter() {
            let Some(identity) = &entry.identity else {
                continue;
            };
            match clients
                .iter_mut()
                .find(|c| c.client_id == identity.client_id)
            {
                Some(client) => {
                    client.connections += 1;
                    client.transports.push(entry.transport);
                    // Newest hello wins for the display fields.
                    client.name.clone_from(&identity.name);
                    client.capabilities.clone_from(&identity.capabilities);
                }
                None => clients.push(LiveClient {
                    client_id: identity.client_id.clone(),
                    name: identity.name.clone(),
                    capabilities: identity.capabilities.clone(),
                    connections: 1,
                    transports: vec![entry.transport],
                    connected_at: entry.connected_at.clone(),
                }),
            }
        }
        clients
    }

    /// Number of live registrations, hello'd or not (test / diagnostic aid
    /// only).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Claim the consumer end of the transition queue. Only the first call
    /// gets it (`None` afterwards): the queue has exactly one consumer so the
    /// publish order stays the mutation order. Transitions recorded before
    /// the claim are retained, not dropped.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn take_transitions(&self) -> Option<mpsc::UnboundedReceiver<ClientTransition>> {
        self.inner
            .transition_rx
            .lock()
            .expect("primary reverse transition receiver poisoned")
            .take()
    }

    /// Spawn the single task that publishes queued transitions as
    /// `client:connected` / `client:disconnected` events (global, `data:
    /// { clientId, name?, capabilities }`) on `api`, one at a time in
    /// registry-mutation order. Every listener sharing the registry calls this
    /// on start; the first call spawns the publisher and later calls are
    /// no-ops. Must be called from within a tokio runtime.
    pub fn spawn_client_event_publisher(&self, api: Arc<dyn WorkspaceApi>) {
        let Some(mut rx) = self.take_transitions() else {
            return;
        };
        tokio::spawn(async move {
            while let Some(transition) = rx.recv().await {
                publish_client_event(api.as_ref(), &transition).await;
            }
        });
    }

    /// Whether the registry currently has no live entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AgentReverseDispatch for PrimaryReverseRegistry {
    fn is_connected(&self) -> bool {
        self.primary().is_some()
    }

    fn dispatch<'a>(
        &'a self,
        method: &'a str,
        params: Value,
        target: ReverseTarget,
    ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
        let channel = resolve_entry(&self.inner.lock(), &target).map(|e| e.channel.clone());
        Box::pin(async move {
            let channel = channel?;
            let timeout = request_timeout(method, &params);
            channel.request(method, params, timeout).await.map_err(|e| {
                ReverseDispatchError::Transport {
                    code: e.code,
                    message: e.message,
                }
            })
        })
    }
}

/// RAII handle returned by [`PrimaryReverseRegistry::register`]; dropping it
/// removes the registration so the connection is idempotently deregistered
/// when its task returns (normal exit, panic, or abort).
pub struct PrimaryReverseGuard {
    registry: Option<Arc<Inner>>,
    id: u64,
}

/// Publish `transition` as its `client:*` event. Global (empty
/// `workspaceId`) so subscribers that omit a `workspaceId` filter still
/// receive it; best-effort like every other change event.
async fn publish_client_event(api: &dyn WorkspaceApi, transition: &ClientTransition) {
    let event_type = transition.event_type();
    if let Err(e) = api
        .publish_event(intent_core::PublishEvent {
            workspace_id: WorkspaceId::from_string(String::new()),
            event_type: event_type.to_string(),
            data: transition.identity().event_data(),
        })
        .await
    {
        tracing::warn!(error = %e, event_type, "failed to publish client event");
    }
}

impl PrimaryReverseGuard {
    /// Attach (or replace, on re-hello) this connection's logical identity
    /// after a successful `client.hello`. Queues the resulting logical-client
    /// transitions (a `Disconnected` for a previous `clientId` this
    /// connection was the last of, then a `Connected` for a new `clientId`
    /// with no other live connection) under the registry lock.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn bind(&self, identity: ReverseClientIdentity) {
        let Some(inner) = &self.registry else {
            return;
        };
        let mut entries = inner.lock();
        let Some(pos) = entries.iter().position(|e| e.id == self.id) else {
            return;
        };
        let previous = entries[pos].identity.replace(identity.clone());
        let same_client = previous
            .as_ref()
            .is_some_and(|p| p.client_id == identity.client_id);
        if same_client {
            return;
        }
        let live_elsewhere = |client_id: &ClientId| {
            entries
                .iter()
                .any(|e| e.id != self.id && e.has_client(client_id))
        };
        if let Some(previous) = previous {
            if !live_elsewhere(&previous.client_id) {
                let _ = inner
                    .transitions
                    .send(ClientTransition::Disconnected(previous));
            }
        }
        if !live_elsewhere(&identity.client_id) {
            let _ = inner
                .transitions
                .send(ClientTransition::Connected(identity));
        }
    }
}

impl Drop for PrimaryReverseGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.registry.take() {
            if let Some(channel) = inner.remove(self.id) {
                channel.close();
            }
        }
    }
}

#[cfg(test)]
mod tests;
