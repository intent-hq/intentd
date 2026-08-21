//! Per-request cancellation keyed by `requestId` (§14.3).
//!
//! Each in-flight search registers a [`CancelToken`] under its `requestId`;
//! `search.cancel` flips the token's flag so the running walk/search observes it
//! and stops early. Cancellation is best-effort and idempotent: cancelling an
//! unknown or already-finished `requestId` is a no-op (the registry reports
//! `false` and the wire surface still returns success).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// A shared cancellation flag handed to a running search. Cloning shares the
/// same underlying flag, so a cancel from another task is observed by the walk.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    /// Mint a fresh, un-cancelled token.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Request cancellation; subsequent [`CancelToken::is_cancelled`] calls
    /// (including ones already in-flight) observe `true`.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Maps a search `requestId` to its [`CancelToken`]. Cheap to clone (shares the
/// inner map) so the services layer can hold one registry across all searches.
#[derive(Clone, Default)]
pub struct CancelRegistry {
    inner: Arc<Mutex<HashMap<String, CancelToken>>>,
}

impl CancelRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `request_id`, returning the token the search should poll. A
    /// re-registered id replaces any prior token (a fresh search supersedes a
    /// finished one).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn register(&self, request_id: &str) -> CancelToken {
        let token = CancelToken::new();
        self.inner
            .lock()
            .expect("cancel registry poisoned")
            .insert(request_id.to_string(), token.clone());
        token
    }

    /// Cancel the search registered under `request_id`. Returns `true` when a
    /// live token was found and flipped, `false` for an unknown/finished id.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn cancel(&self, request_id: &str) -> bool {
        match self
            .inner
            .lock()
            .expect("cancel registry poisoned")
            .get(request_id)
        {
            Some(token) => {
                token.cancel();
                true
            }
            None => false,
        }
    }

    /// Drop the token for `request_id` once its search has finished.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn unregister(&self, request_id: &str) {
        self.inner
            .lock()
            .expect("cancel registry poisoned")
            .remove(request_id);
    }
}

/// Mint a fresh `requestId` for searches that omit one (`srch-<uuidv4>`).
pub fn mint_request_id() -> String {
    format!("srch-{}", uuid::Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_unknown_is_false() {
        let reg = CancelRegistry::new();
        assert!(!reg.cancel("nope"));
    }

    #[test]
    fn register_then_cancel_flips_token() {
        let reg = CancelRegistry::new();
        let token = reg.register("srch-1");
        assert!(!token.is_cancelled());
        assert!(reg.cancel("srch-1"));
        assert!(token.is_cancelled());
        reg.unregister("srch-1");
        assert!(!reg.cancel("srch-1"));
    }

    #[test]
    fn minted_ids_are_prefixed_and_unique() {
        let a = mint_request_id();
        let b = mint_request_id();
        assert!(a.starts_with("srch-"));
        assert_ne!(a, b);
    }
}
