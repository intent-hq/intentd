//! Single-flight coalescing for the full `accept-changes.getStatus` build,
//! keyed by workspace id (intent-hq/monorepo#1693 Finding B).
//!
//! [`crate::git_status_cache`] / [`crate::git_status_singleflight`] already
//! coalesce the working-tree scan that `build_git_status_value_with` pays for
//! (monorepo#1648), but the remaining per-call work — remote/trunk resolve,
//! ahead/behind, and the bounded history walk — still ran once per concurrent
//! `ac_get_status` caller. Under a burst (multiple FE panels polling the same
//! workspace) that repeats the full build for callers asking the same
//! question at the same time. Concurrent `ac_get_status` calls for one
//! workspace now coalesce onto a single blocking-pool build whose result is
//! shared; distinct workspaces never serialize against each other.
//!
//! Coalescing only, deliberately no result cache or TTL: a call that arrives
//! after the in-flight build has settled (flight unregistered) always starts
//! a fresh build, so a read landing right after a mutation observes the
//! mutation rather than a stale snapshot. The leader registers the flight,
//! runs the build, and publishes the shared result; followers await it over a
//! `watch` channel. A leader that vanishes without publishing (cancelled RPC,
//! panicked build) drops its guard, which unregisters the flight and closes
//! the channel so followers retry — the next joiner is elected leader.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use intent_core::WorkspaceId;
use tokio::sync::watch;

/// The identity an `accept-changes.getStatus` build coalesces on: the
/// workspace id (one workspace has exactly one worktree).
pub(crate) type AcStatusKey = WorkspaceId;

/// The result shape shared across coalesced callers. The payload is `Arc`ed
/// so followers clone the JSON value, not re-run the build; errors travel as
/// their message ([`intent_core::Error`] is not `Clone`) and followers
/// surface them as `Error::Internal` — variant-faithful in practice, since
/// every build error is already `Error::Internal`.
pub(crate) type SharedAcStatusResult = std::result::Result<Arc<serde_json::Value>, String>;

type Slot = Option<SharedAcStatusResult>;

/// The in-flight `accept-changes.getStatus` build registry. Shared across
/// [`crate::Services`] clones so every handle observes the same flights.
#[derive(Default)]
pub(crate) struct AcStatusSingleFlight {
    inflight: Mutex<HashMap<AcStatusKey, Arc<watch::Sender<Slot>>>>,
}

/// The caller's role in a flight: the leader runs the build and publishes; a
/// follower awaits the leader's published result.
pub(crate) enum Join {
    Leader(FlightGuard),
    Follower(watch::Receiver<Slot>),
}

impl AcStatusSingleFlight {
    /// Join the flight for `key`: the first caller becomes the leader (and
    /// must run the build); every later caller while the flight is
    /// registered becomes a follower.
    pub(crate) fn join(self: &Arc<Self>, key: &AcStatusKey) -> Join {
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
    pub(crate) fn waiters(&self, key: &AcStatusKey) -> usize {
        self.inflight
            .lock()
            .unwrap()
            .get(key)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }
}

/// The leader's registration handle. [`FlightGuard::finish`] publishes the
/// shared result to followers; dropping the guard (with or without
/// finishing) unregisters the flight — the flight never outlives one build,
/// so the next caller always starts a fresh one. An unfinished drop closes
/// the channel, waking followers to retry.
pub(crate) struct FlightGuard {
    flights: Arc<AcStatusSingleFlight>,
    key: AcStatusKey,
    tx: Arc<watch::Sender<Slot>>,
}

impl FlightGuard {
    /// Publish the build result to every follower. `send_replace` stores the
    /// value even with zero receivers (a plain `send` would fail and publish
    /// nothing), and it happens before the `Drop` impl unregisters the
    /// flight, so a request landing in between subscribes to an
    /// already-resolved channel instead of hanging or retrying.
    pub(crate) fn finish(self, result: SharedAcStatusResult) {
        self.tx.send_replace(Some(result));
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.flights.inflight.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first joiner leads; later joiners follow and receive the leader's
    /// published result. The flight unregisters once the guard drops, so no
    /// result is retained past the flight (coalescing only, no cache).
    #[tokio::test]
    async fn leader_publishes_and_followers_share_the_result() {
        let flights = Arc::new(AcStatusSingleFlight::default());
        let ws = WorkspaceId::new();
        let Join::Leader(guard) = flights.join(&ws) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&ws) else {
            panic!("second joiner must follow");
        };
        guard.finish(Ok(Arc::new(serde_json::json!({ "branch": "feature/x" }))));
        let slot = rx
            .wait_for(std::option::Option::is_some)
            .await
            .expect("published");
        let shared = slot.clone().unwrap().expect("ok result");
        assert_eq!(*shared, serde_json::json!({ "branch": "feature/x" }));
        drop(slot);
        assert!(flights.inflight.lock().unwrap().is_empty(), "unregistered");
    }

    /// After a flight settles (guard dropped post-`finish`), the next joiner
    /// for the same key leads a brand-new build rather than replaying the
    /// prior result — a call arriving after a mutation observes fresh state.
    #[tokio::test]
    async fn settled_flight_starts_a_fresh_build() {
        let flights = Arc::new(AcStatusSingleFlight::default());
        let ws = WorkspaceId::new();
        let Join::Leader(guard) = flights.join(&ws) else {
            panic!("first joiner must lead");
        };
        guard.finish(Ok(Arc::new(serde_json::json!({ "aheadOfTrunk": 0 }))));
        assert!(
            matches!(flights.join(&ws), Join::Leader(_)),
            "post-settle join must lead a fresh build"
        );
    }

    /// A failed build is shared with the current followers but never
    /// retained: the flight unregisters, so the next caller leads a fresh
    /// build (retry works).
    #[tokio::test]
    async fn failed_build_is_shared_then_not_cached() {
        let flights = Arc::new(AcStatusSingleFlight::default());
        let ws = WorkspaceId::new();
        let Join::Leader(guard) = flights.join(&ws) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&ws) else {
            panic!("second joiner must follow");
        };
        guard.finish(Err("boom".to_string()));
        let slot = rx
            .wait_for(std::option::Option::is_some)
            .await
            .expect("published");
        assert_eq!(slot.clone().unwrap().unwrap_err(), "boom");
        drop(slot);
        assert!(matches!(flights.join(&ws), Join::Leader(_)), "retried");
    }

    /// A leader dropped without publishing closes the channel (followers see
    /// an error and retry) and frees the key for a new leader.
    #[tokio::test]
    async fn dropped_leader_wakes_followers_to_retry() {
        let flights = Arc::new(AcStatusSingleFlight::default());
        let ws = WorkspaceId::new();
        let Join::Leader(guard) = flights.join(&ws) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&ws) else {
            panic!("second joiner must follow");
        };
        drop(guard);
        assert!(
            rx.wait_for(std::option::Option::is_some).await.is_err(),
            "closed"
        );
        assert!(matches!(flights.join(&ws), Join::Leader(_)));
    }

    /// Distinct workspaces never share a flight (they must not serialize).
    #[tokio::test]
    async fn distinct_workspaces_lead_independently() {
        let flights = Arc::new(AcStatusSingleFlight::default());
        let a = WorkspaceId::new();
        let b = WorkspaceId::new();
        let _a = flights.join(&a);
        assert!(matches!(flights.join(&b), Join::Leader(_)));
    }
}
