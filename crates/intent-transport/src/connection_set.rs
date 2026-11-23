//! An optional set every accepted client connection joins, so the owner of
//! the set can close them all at once.
//!
//! The orchestrator hands each listener its tenant's set: the tenant's
//! self-fence closes the set, which closes every connection and refuses new
//! ones. `intentd` installs none, and its listeners behave as before.

use std::sync::Arc;

/// Where accepted connections register their close.
pub trait ConnectionSet: Send + Sync {
    /// Register a connection's `close`. Returns the guard that forgets the
    /// connection when it is dropped (the connection ended on its own), or
    /// `None` when the set is already closed; `close` has then already run.
    fn register(&self, close: Box<dyn FnOnce() + Send>) -> Option<ConnectionGuard>;

    /// Whether the set is closed: the listeners then refuse a new
    /// connection before serving it (WSS answers `503` before the upgrade,
    /// the local socket drops it unread), so no frame of a connection
    /// accepted after the close reaches the router.
    fn is_closed(&self) -> bool;
}

/// Forgets a registered connection when dropped.
#[must_use = "dropping the guard forgets the connection at once"]
pub struct ConnectionGuard(Option<Box<dyn FnOnce() + Send + Sync>>);

impl ConnectionGuard {
    /// A guard that runs `forget` when dropped.
    pub fn new(forget: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self(Some(Box::new(forget)))
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(forget) = self.0.take() {
            forget();
        }
    }
}

impl std::fmt::Debug for ConnectionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnectionGuard")
    }
}

/// A shared [`ConnectionSet`], as the listeners take it.
#[derive(Clone)]
pub struct ConnectionHook(Arc<dyn ConnectionSet>);

impl ConnectionHook {
    /// Wrap `set`.
    pub fn new(set: Arc<dyn ConnectionSet>) -> Self {
        Self(set)
    }

    /// Register a connection's `close` (see [`ConnectionSet::register`]).
    pub(crate) fn register(
        &self,
        close: impl FnOnce() + Send + 'static,
    ) -> Option<ConnectionGuard> {
        self.0.register(Box::new(close))
    }

    /// Whether the set is closed (see [`ConnectionSet::is_closed`]).
    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

impl std::fmt::Debug for ConnectionHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnectionHook")
    }
}

#[cfg(all(test, unix))]
mod tests;
