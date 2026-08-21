//! Single-flight coalescing for working-tree status scans, keyed by canonical
//! worktree path.
//!
//! `git.status` and `accept-changes.getStatus` both pay a full libgit2
//! working-tree scan ([`intent_git::status::status`]). Under an event burst the
//! FE can land dozens of those concurrently for the same worktree, each
//! re-walking the same tree. Concurrent scans for one worktree coalesce onto a
//! single blocking-pool scan whose result is shared; distinct worktrees never
//! serialize against each other. The leader registers the flight, runs the
//! scan, and publishes the shared result; followers await it over a `watch`
//! channel. A leader that vanishes without publishing (cancelled RPC, panicked
//! scan) drops its guard, which unregisters the flight and closes the channel
//! so followers retry — the next joiner is elected leader. Results are never
//! retained past the flight, so a failed scan is not cached.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_core::GitStatus;
use tokio::sync::watch;

/// The identity a status scan coalesces on: the canonical worktree path.
pub(crate) type StatusKey = PathBuf;

/// The result shape shared across coalesced callers. The status is `Arc`ed so
/// followers clone the value, not re-run the scan; errors travel as their
/// message ([`intent_core::Error`] is not `Clone`) and followers surface them
/// as `Error::Internal` — variant-faithful in practice, since every scan error
/// is already `Error::Internal` (libgit2 failures map through `map_git_err`).
pub(crate) type SharedStatusResult = std::result::Result<Arc<GitStatus>, String>;

type Slot = Option<SharedStatusResult>;

/// The in-flight status-scan registry. Shared across [`crate::Services`]
/// clones so every handle observes the same flights.
#[derive(Default)]
pub(crate) struct StatusSingleFlight {
    inflight: Mutex<HashMap<StatusKey, Arc<watch::Sender<Slot>>>>,
}

/// The caller's role in a flight: the leader runs the scan and publishes; a
/// follower awaits the leader's published result.
pub(crate) enum Join {
    Leader(FlightGuard),
    Follower(watch::Receiver<Slot>),
}

impl StatusSingleFlight {
    /// Join the flight for `key`: the first caller becomes the leader (and
    /// must run the scan); every later caller while the flight is registered
    /// becomes a follower.
    pub(crate) fn join(self: &Arc<Self>, key: &StatusKey) -> Join {
        let mut map = self.inflight.lock().unwrap();
        if let Some(tx) = map.get(key) {
            return Join::Follower(tx.subscribe());
        }
        let (tx, _rx) = watch::channel(None);
        let tx = Arc::new(tx);
        map.insert(key.clone(), Arc::clone(&tx));
        Join::Leader(FlightGuard {
            flights: Arc::clone(self),
            key: key.clone(),
            tx,
        })
    }

    /// Number of followers currently awaiting the flight for `key`.
    #[cfg(test)]
    pub(crate) fn waiters(&self, key: &StatusKey) -> usize {
        self.inflight
            .lock()
            .unwrap()
            .get(key)
            .map_or(0, |tx| tx.receiver_count())
    }
}

/// The leader's registration handle. [`FlightGuard::finish`] publishes the
/// shared result to followers; dropping the guard (with or without finishing)
/// unregisters the flight. An unfinished drop closes the channel, waking
/// followers to retry.
pub(crate) struct FlightGuard {
    flights: Arc<StatusSingleFlight>,
    key: StatusKey,
    tx: Arc<watch::Sender<Slot>>,
}

impl FlightGuard {
    /// Publish the scan result to every follower. `send_replace` stores the
    /// value even with zero receivers (a plain `send` would fail and publish
    /// nothing), and it happens before the `Drop` impl unregisters the flight,
    /// so a request landing in between subscribes to an already-resolved
    /// channel instead of hanging or retrying.
    pub(crate) fn finish(self, result: SharedStatusResult) {
        self.tx.send_replace(Some(result));
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.flights.inflight.lock().unwrap().remove(&self.key);
    }
}

/// The canonical key for `worktree`: the resolved path when it exists, else the
/// path as given (so two spellings of the same worktree still coalesce, and an
/// unresolvable path degrades to no coalescing rather than a wrong share).
pub(crate) fn status_key(worktree: &std::path::Path) -> StatusKey {
    std::fs::canonicalize(worktree).unwrap_or_else(|_| worktree.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status() -> GitStatus {
        intent_git::status::empty_status()
    }

    /// The first joiner leads; later joiners follow and receive the leader's
    /// published result. The flight unregisters once the guard drops.
    #[tokio::test]
    async fn leader_publishes_and_followers_share_the_result() {
        let flights = Arc::new(StatusSingleFlight::default());
        let key = PathBuf::from("/tmp/wt-a");
        let Join::Leader(guard) = flights.join(&key) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&key) else {
            panic!("second joiner must follow");
        };
        let mut published = status();
        published.branch = "feature/x".to_string();
        guard.finish(Ok(Arc::new(published)));
        let slot = rx
            .wait_for(std::option::Option::is_some)
            .await
            .expect("published");
        let shared = slot.clone().unwrap().expect("ok result");
        assert_eq!(shared.branch, "feature/x");
        drop(slot);
        assert!(flights.inflight.lock().unwrap().is_empty(), "unregistered");
    }

    /// Distinct worktrees never share a flight (they must not serialize).
    #[tokio::test]
    async fn distinct_worktrees_lead_independently() {
        let flights = Arc::new(StatusSingleFlight::default());
        let _a = flights.join(&PathBuf::from("/tmp/wt-a"));
        assert!(matches!(
            flights.join(&PathBuf::from("/tmp/wt-b")),
            Join::Leader(_)
        ));
    }

    /// A failed scan is shared with the current followers but never retained:
    /// the flight unregisters, so the next caller leads a fresh scan.
    #[tokio::test]
    async fn failed_scan_is_shared_then_not_cached() {
        let flights = Arc::new(StatusSingleFlight::default());
        let key = PathBuf::from("/tmp/wt-a");
        let Join::Leader(guard) = flights.join(&key) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&key) else {
            panic!("second joiner must follow");
        };
        guard.finish(Err("boom".to_string()));
        let slot = rx
            .wait_for(std::option::Option::is_some)
            .await
            .expect("published");
        assert_eq!(slot.clone().unwrap().unwrap_err(), "boom");
        drop(slot);
        assert!(matches!(flights.join(&key), Join::Leader(_)), "retried");
    }

    /// A leader dropped without publishing closes the channel (followers see
    /// an error and retry) and frees the key for a new leader.
    #[tokio::test]
    async fn dropped_leader_wakes_followers_to_retry() {
        let flights = Arc::new(StatusSingleFlight::default());
        let key = PathBuf::from("/tmp/wt-a");
        let Join::Leader(guard) = flights.join(&key) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&key) else {
            panic!("second joiner must follow");
        };
        drop(guard);
        assert!(
            rx.wait_for(std::option::Option::is_some).await.is_err(),
            "closed"
        );
        assert!(matches!(flights.join(&key), Join::Leader(_)));
    }
}
