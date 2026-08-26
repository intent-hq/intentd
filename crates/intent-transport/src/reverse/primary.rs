//! First-client-sticky reverse-dispatch registry (REV-1).
//!
//! Interim policy for agent-initiated reverse RPCs (currently `browser.exec`
//! called via the MCP `ws.browser.exec` binding, PROTOCOL §5.14/§12.4). When
//! the caller has no ambient client connection to reverse-dispatch on, the
//! daemon routes the request to the **first-registered live client**; when
//! that client disconnects, the next-registered client takes over. This is a
//! deliberate stopgap ahead of an explicit target-selection surface (REV-2).
//!
//! Every accepted UDS or WSS connection registers its per-connection
//! [`ReverseChannel`] with the shared [`PrimaryReverseRegistry`] and holds the
//! returned [`PrimaryReverseGuard`] for the life of the connection; the guard
//! removes the entry on drop, so the failover order is exactly the connection
//! arrival order.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use intent_core::{AgentReverseDispatch, BoxFuture, ReverseDispatchError};
use serde_json::Value;

use super::{request_timeout, ReverseChannel};

/// Registry of live reverse channels ordered by arrival. Cheap to clone
/// (`Arc` inside).
#[derive(Clone, Default)]
pub struct PrimaryReverseRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    entries: Mutex<VecDeque<Entry>>,
    next_id: AtomicU64,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(0),
        }
    }
}

struct Entry {
    id: u64,
    channel: ReverseChannel,
}

impl PrimaryReverseRegistry {
    /// Build an empty registry (no clients connected).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `channel` as a live reverse target and return a guard whose
    /// drop removes the entry (RAII: the caller holds it for the connection's
    /// lifetime).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn register(&self, channel: ReverseChannel) -> PrimaryReverseGuard {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .entries
            .lock()
            .expect("primary reverse entries poisoned")
            .push_back(Entry { id, channel });
        PrimaryReverseGuard {
            registry: Some(self.inner.clone()),
            id,
        }
    }

    /// The current sticky primary channel, or `None` when no clients are
    /// connected. A cheap clone of the entry's channel; the entry stays
    /// registered.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn primary(&self) -> Option<ReverseChannel> {
        self.inner
            .entries
            .lock()
            .expect("primary reverse entries poisoned")
            .front()
            .map(|e| e.channel.clone())
    }

    /// Number of live registrations (test / diagnostic aid only).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .entries
            .lock()
            .expect("primary reverse entries poisoned")
            .len()
    }

    /// Whether the registry currently has no live entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AgentReverseDispatch for PrimaryReverseRegistry {
    fn is_connected(&self) -> bool {
        !self.is_empty()
    }

    fn dispatch<'a>(
        &'a self,
        method: &'a str,
        params: Value,
    ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
        let channel = self.primary();
        Box::pin(async move {
            let Some(channel) = channel else {
                return Err(ReverseDispatchError::NoClient);
            };
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

impl Drop for PrimaryReverseGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.registry.take() {
            let mut entries = inner
                .entries
                .lock()
                .expect("primary reverse entries poisoned");
            if let Some(pos) = entries.iter().position(|e| e.id == self.id) {
                entries.remove(pos);
            }
        }
    }
}

#[cfg(test)]
mod tests;
