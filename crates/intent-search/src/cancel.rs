//! Per-request cancellation keyed by `requestId` (§14.3).
//!
//! Each in-flight search registers a [`CancelToken`] under its `requestId`;
//! `search.cancel` flips the token's flag so the running walk/search observes it
//! and stops early. Cancellation is best-effort and idempotent: cancelling an
//! unknown or already-finished `requestId` is a no-op (the registry reports
//! `false` and the wire surface still returns success).
//!
//! A registration may carry an **owner** tag ([`CancelRegistry::register_as`]);
//! a cancel presented with an owner ([`CancelRegistry::cancel_as`]) flips only
//! a token registered under that same owner, while an owner-less cancel flips
//! any. This is how a collaborator's `search.cancel` is confined to searches
//! it started even though `requestId`s are visible to every workspace
//! subscriber through the streamed `search:*` events.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// A shared cancellation flag handed to a running search. Cloning shares the
/// same underlying flag, so a cancel from another task is observed by the walk.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    /// Mint a fresh, un-cancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Request cancellation; subsequent [`CancelToken::is_cancelled`] calls
    /// (including ones already in-flight) observe `true`.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// One registration: the token plus the owner tag it was registered under.
#[derive(Clone, Debug)]
struct Entry {
    token: CancelToken,
    owner: Option<String>,
}

/// Maps a search `requestId` to its [`CancelToken`]. Cheap to clone (shares the
/// inner map) so the services layer can hold one registry across all searches.
#[derive(Clone, Default)]
pub struct CancelRegistry {
    inner: Arc<Mutex<HashMap<String, Entry>>>,
}

impl CancelRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `request_id` without an owner ([`Self::register_as`] with
    /// `None`), returning the token the search should poll.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn register(&self, request_id: &str) -> CancelToken {
        self.register_as(request_id, None)
    }

    /// Register `request_id` under `owner`, returning the token the search
    /// should poll. A re-registered id replaces any prior token and owner (a
    /// fresh search supersedes a finished one).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn register_as(&self, request_id: &str, owner: Option<String>) -> CancelToken {
        let token = CancelToken::new();
        self.inner.lock().expect("cancel registry poisoned").insert(
            request_id.to_string(),
            Entry {
                token: token.clone(),
                owner,
            },
        );
        token
    }

    /// Cancel the search registered under `request_id` regardless of owner
    /// ([`Self::cancel_as`] with `None`). Returns `true` when a live token was
    /// found and flipped, `false` for an unknown/finished id.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn cancel(&self, request_id: &str) -> bool {
        self.cancel_as(request_id, None)
    }

    /// Cancel the search registered under `request_id` on behalf of `owner`:
    /// `None` flips any live token; `Some(owner)` flips only a token that was
    /// registered under that same owner. Returns `true` when a token was
    /// flipped, `false` for an unknown/finished id **or** an owner mismatch
    /// (indistinguishable by design — nothing about another owner's search is
    /// disclosed).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn cancel_as(&self, request_id: &str, owner: Option<&str>) -> bool {
        match self
            .inner
            .lock()
            .expect("cancel registry poisoned")
            .get(request_id)
        {
            Some(entry) if owner.is_none() || entry.owner.as_deref() == owner => {
                entry.token.cancel();
                true
            }
            Some(_) | None => false,
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
#[must_use]
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
    fn owned_registrations_are_cancelled_only_by_their_owner_or_unowned_cancels() {
        let reg = CancelRegistry::new();
        let alice = reg.register_as("srch-a", Some("alice".into()));
        let unowned = reg.register("srch-u");

        // Another owner: silent no-op, token untouched.
        assert!(!reg.cancel_as("srch-a", Some("bob")));
        assert!(!alice.is_cancelled());
        // An owned cancel never reaches an unowned (administrator) search.
        assert!(!reg.cancel_as("srch-u", Some("alice")));
        assert!(!unowned.is_cancelled());
        // The owner cancels its own.
        assert!(reg.cancel_as("srch-a", Some("alice")));
        assert!(alice.is_cancelled());
        // An unowned cancel flips anything.
        let bob = reg.register_as("srch-b", Some("bob".into()));
        assert!(reg.cancel("srch-b"));
        assert!(bob.is_cancelled());
        assert!(reg.cancel_as("srch-u", None));
        assert!(unowned.is_cancelled());
        // Re-registering replaces the owner too.
        let taken = reg.register_as("srch-a", Some("bob".into()));
        assert!(!reg.cancel_as("srch-a", Some("alice")));
        assert!(!taken.is_cancelled());
    }

    #[test]
    fn minted_ids_are_prefixed_and_unique() {
        let a = mint_request_id();
        let b = mint_request_id();
        assert!(a.starts_with("srch-"));
        assert_ne!(a, b);
    }
}
