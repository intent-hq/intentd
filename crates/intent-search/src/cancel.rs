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
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

/// A shared cancellation flag handed to a running search. Cloning shares the
/// same underlying flag, so a cancel from another task is observed by the walk.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<Flag>);

/// The flag and the tasks waiting for it ([`CancelToken::cancelled`]).
#[derive(Debug, Default)]
struct Flag {
    set: AtomicBool,
    waiting: Mutex<Vec<Waker>>,
}

impl CancelToken {
    /// Mint a fresh, un-cancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation; subsequent [`CancelToken::is_cancelled`] calls
    /// (including ones already in-flight) observe `true`, and every
    /// [`CancelToken::cancelled`] future completes.
    ///
    /// # Panics
    ///
    /// Panics if the waiter mutex is poisoned (a prior panic while holding the lock).
    pub fn cancel(&self) {
        self.0.set.store(true, Ordering::SeqCst);
        let waiting = std::mem::take(&mut *self.0.waiting.lock().expect("cancel token poisoned"));
        for waker in waiting {
            waker.wake();
        }
    }

    /// Whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.set.load(Ordering::SeqCst)
    }

    /// Completes once cancellation has been requested, for a search that
    /// waits on another task (a search on a workspace's home) rather than
    /// polling the flag.
    ///
    /// # Panics
    ///
    /// Panics if the waiter mutex is poisoned (a prior panic while holding the lock).
    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + '_ {
        std::future::poll_fn(move |cx| {
            if self.is_cancelled() {
                return Poll::Ready(());
            }
            {
                let mut waiting = self.0.waiting.lock().expect("cancel token poisoned");
                if !waiting.iter().any(|waker| waker.will_wake(cx.waker())) {
                    waiting.push(cx.waker().clone());
                }
            }
            // A cancel between the first check and the registration has
            // already taken the wakers: look again.
            if self.is_cancelled() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }
}

/// One registration: the token plus the owner tag it was registered under.
#[derive(Clone, Debug)]
struct Entry {
    token: CancelToken,
    owner: Option<String>,
    /// Cancelled before its search registered ([`CancelRegistry::cancel_early`]):
    /// the search registering next under this id starts cancelled.
    early: bool,
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
        let previous = self.inner.lock().expect("cancel registry poisoned").insert(
            request_id.to_string(),
            Entry {
                token: token.clone(),
                owner,
                early: false,
            },
        );
        if previous.is_some_and(|entry| entry.early) {
            token.cancel();
        }
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

    /// Cancel the search `request_id` on behalf of whoever started it, also
    /// when it has not registered yet: that search then starts cancelled.
    /// Undo the early mark with [`Self::forget_early`] once that search can
    /// no longer start.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn cancel_early(&self, request_id: &str) {
        let mut inner = self.inner.lock().expect("cancel registry poisoned");
        if let Some(entry) = inner.get(request_id) {
            entry.token.cancel();
            return;
        }
        let token = CancelToken::new();
        token.cancel();
        inner.insert(
            request_id.to_string(),
            Entry {
                token,
                owner: None,
                early: true,
            },
        );
    }

    /// Drop an early mark [`Self::cancel_early`] left for `request_id`
    /// when no search took it; a registered search is left alone.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub fn forget_early(&self, request_id: &str) {
        let mut inner = self.inner.lock().expect("cancel registry poisoned");
        if inner.get(request_id).is_some_and(|entry| entry.early) {
            inner.remove(request_id);
        }
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

    #[test]
    fn an_early_cancel_cancels_the_search_registering_next_and_is_forgotten() {
        let reg = CancelRegistry::new();
        reg.cancel_early("srch-early");
        let token = reg.register_as("srch-early", Some("alice".into()));
        assert!(token.is_cancelled(), "registered after the cancel");
        // The search took the mark: forgetting leaves the search alone.
        reg.forget_early("srch-early");
        assert!(reg.cancel_as("srch-early", Some("alice")));
        reg.unregister("srch-early");

        // A mark no search took is forgotten; a later search runs.
        reg.cancel_early("srch-late");
        reg.forget_early("srch-late");
        assert!(!reg.register("srch-late").is_cancelled());

        // A registered search is cancelled at once.
        let running = reg.register("srch-running");
        reg.cancel_early("srch-running");
        assert!(running.is_cancelled());
    }

    #[test]
    fn cancelled_completes_when_the_token_is_cancelled() {
        use std::sync::atomic::AtomicUsize;
        use std::task::{Context, Wake};

        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let woken = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(woken.clone());
        let mut cx = Context::from_waker(&waker);
        let token = CancelToken::new();
        let mut cancelled = std::pin::pin!(token.cancelled());
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        token.clone().cancel();
        assert_eq!(woken.0.load(Ordering::SeqCst), 1, "woken once");
        assert!(cancelled.as_mut().poll(&mut cx).is_ready());
        // Already cancelled: ready at once.
        assert!(std::pin::pin!(token.cancelled()).poll(&mut cx).is_ready());
    }
}
