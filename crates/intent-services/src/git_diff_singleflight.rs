//! Single-flight coalescing for `git.diffs` walks plus the per-workspace
//! rate limiter for the "slow worktree hunk walk" WARN.
//!
//! Concurrent `git.diffs` calls with an identical request identity
//! (`workspace_id`, `paths`, `staged`, `commit_hash`, `git_root_id`) coalesce
//! onto one blocking-pool libgit2 walk whose result is shared; non-identical
//! requests run independently. The leader registers the flight, runs the walk,
//! and publishes the shared result; followers await it over a `watch` channel.
//! A leader that vanishes without publishing (cancelled RPC, panicked walk)
//! drops its guard, which unregisters the flight and closes the channel so
//! followers retry — the next joiner is elected leader.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::{WorkspaceGitRootId, WorkspaceId};
use tokio::sync::watch;

/// The full request identity a `git.diffs` call coalesces on. The
/// `git_root_id` component keeps a registered-root walk (monorepo#2053) from
/// coalescing with a primary-worktree walk on the same workspace.
pub(crate) type DiffKey = (
    WorkspaceId,
    Option<Vec<String>>,
    bool,
    Option<String>,
    Option<WorkspaceGitRootId>,
);

/// The result shape shared across coalesced callers. The payload is `Arc`ed so
/// followers clone the JSON value, not re-run the walk; errors travel as their
/// message ([`intent_core::Error`] is not `Clone`) and followers surface them
/// as `Error::Internal` — variant-faithful in practice, since every walk error
/// is already `Error::Internal` (libgit2 failures map through `map_git_err`).
pub(crate) type SharedDiffResult = std::result::Result<Arc<serde_json::Value>, String>;

type Slot = Option<SharedDiffResult>;

/// The in-flight `git.diffs` walk registry. Shared across [`crate::Services`]
/// clones so every handle observes the same flights.
#[derive(Default)]
pub(crate) struct DiffSingleFlight {
    inflight: Mutex<HashMap<DiffKey, Arc<watch::Sender<Slot>>>>,
}

/// The caller's role in a flight: the leader runs the walk and publishes; a
/// follower awaits the leader's published result.
pub(crate) enum Join {
    Leader(FlightGuard),
    Follower(watch::Receiver<Slot>),
}

impl DiffSingleFlight {
    /// Join the flight for `key`: the first caller becomes the leader (and
    /// must run the walk); every later caller while the flight is registered
    /// becomes a follower.
    pub(crate) fn join(self: &Arc<Self>, key: &DiffKey) -> Join {
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
    pub(crate) fn waiters(&self, key: &DiffKey) -> usize {
        self.inflight
            .lock()
            .unwrap()
            .get(key)
            .map(|tx| tx.receiver_count())
            .unwrap_or(0)
    }
}

/// The leader's registration handle. [`FlightGuard::finish`] publishes the
/// shared result to followers; dropping the guard (with or without finishing)
/// unregisters the flight. An unfinished drop closes the channel, waking
/// followers to retry.
pub(crate) struct FlightGuard {
    flights: Arc<DiffSingleFlight>,
    key: DiffKey,
    tx: Arc<watch::Sender<Slot>>,
}

impl FlightGuard {
    /// Publish the walk result to every follower. `send_replace` stores the
    /// value even with zero receivers (a plain `send` would fail and publish
    /// nothing), and it happens before the `Drop` impl unregisters the
    /// flight, so a request landing in between subscribes to an
    /// already-resolved channel instead of hanging or retrying.
    pub(crate) fn finish(self, result: SharedDiffResult) {
        self.tx.send_replace(Some(result));
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.flights.inflight.lock().unwrap().remove(&self.key);
    }
}

/// Per-workspace rate limiter for the `git.diffs` "slow worktree hunk walk"
/// WARN: at most one WARN per workspace per [`SLOW_WALK_WARN_WINDOW`];
/// subsequent slow walks within the window are demoted to DEBUG by the caller.
#[derive(Default)]
pub(crate) struct SlowWalkWarnLimiter {
    last_warn: Mutex<HashMap<WorkspaceId, Instant>>,
}

/// Minimum spacing between "slow worktree hunk walk" WARNs per workspace.
pub(crate) const SLOW_WALK_WARN_WINDOW: Duration = Duration::from_secs(60);

impl SlowWalkWarnLimiter {
    /// `true` when the caller should log at WARN (and the window restarts);
    /// `false` while a prior WARN for the workspace is still within the window.
    pub(crate) fn should_warn(&self, workspace_id: &WorkspaceId) -> bool {
        self.should_warn_within(workspace_id, SLOW_WALK_WARN_WINDOW)
    }

    fn should_warn_within(&self, workspace_id: &WorkspaceId, window: Duration) -> bool {
        let mut last_warn = self.last_warn.lock().unwrap();
        let now = Instant::now();
        // Evict expired entries so the map stays bounded by the set of
        // workspaces that warned within the current window (workspaces come
        // and go; a grow-only map would leak).
        last_warn.retain(|_, prev| now.duration_since(*prev) < window);
        match last_warn.get(workspace_id) {
            Some(_) => false,
            None => {
                last_warn.insert(workspace_id.clone(), now);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(ws: &WorkspaceId) -> DiffKey {
        (ws.clone(), None, false, None, None)
    }

    /// The first joiner leads; later joiners follow and receive the leader's
    /// published result. The flight unregisters once the guard drops.
    #[tokio::test]
    async fn leader_publishes_and_followers_share_the_result() {
        let flights = Arc::new(DiffSingleFlight::default());
        let ws = WorkspaceId::new();
        let Join::Leader(guard) = flights.join(&key(&ws)) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&key(&ws)) else {
            panic!("second joiner must follow");
        };
        guard.finish(Ok(Arc::new(serde_json::json!([{ "path": "a" }]))));
        let slot = rx
            .wait_for(std::option::Option::is_some)
            .await
            .expect("published");
        let shared = slot.clone().unwrap().expect("ok result");
        assert_eq!(*shared, serde_json::json!([{ "path": "a" }]));
        drop(slot);
        assert!(flights.inflight.lock().unwrap().is_empty(), "unregistered");
    }

    /// `finish` publishes even with zero receivers (`send_replace`; a plain
    /// `send` fails without receivers), so a request that subscribes between
    /// the publish and the guard drop reads the resolved slot instead of
    /// blocking until close and retrying.
    #[tokio::test]
    async fn finish_publishes_with_zero_receivers() {
        let flights = Arc::new(DiffSingleFlight::default());
        let ws = WorkspaceId::new();
        let Join::Leader(guard) = flights.join(&key(&ws)) else {
            panic!("first joiner must lead");
        };
        let tx = Arc::clone(&guard.tx);
        guard.finish(Ok(Arc::new(serde_json::json!([]))));
        assert!(
            tx.borrow().is_some(),
            "resolved slot published without receivers"
        );
    }

    /// Distinct request identities never share a flight.
    #[tokio::test]
    async fn distinct_keys_lead_independently() {
        let flights = Arc::new(DiffSingleFlight::default());
        let ws = WorkspaceId::new();
        let _a = flights.join(&(ws.clone(), None, false, None, None));
        assert!(matches!(
            flights.join(&(ws.clone(), None, true, None, None)),
            Join::Leader(_)
        ));
    }

    /// A leader dropped without publishing closes the channel (followers see
    /// an error and retry) and frees the key for a new leader.
    #[tokio::test]
    async fn dropped_leader_wakes_followers_to_retry() {
        let flights = Arc::new(DiffSingleFlight::default());
        let ws = WorkspaceId::new();
        let Join::Leader(guard) = flights.join(&key(&ws)) else {
            panic!("first joiner must lead");
        };
        let Join::Follower(mut rx) = flights.join(&key(&ws)) else {
            panic!("second joiner must follow");
        };
        drop(guard);
        assert!(
            rx.wait_for(std::option::Option::is_some).await.is_err(),
            "closed"
        );
        assert!(matches!(flights.join(&key(&ws)), Join::Leader(_)));
    }

    /// One WARN per workspace per window; other workspaces are independent,
    /// and an elapsed window re-arms the WARN.
    #[test]
    fn slow_walk_warn_rate_limits_per_workspace() {
        let limiter = SlowWalkWarnLimiter::default();
        let a = WorkspaceId::new();
        let b = WorkspaceId::new();
        assert!(limiter.should_warn_within(&a, Duration::from_secs(60)));
        assert!(!limiter.should_warn_within(&a, Duration::from_secs(60)));
        assert!(limiter.should_warn_within(&b, Duration::from_secs(60)));
        // A zero window means every prior WARN is already outside it.
        assert!(limiter.should_warn_within(&a, Duration::ZERO));
        assert!(limiter.should_warn_within(&a, Duration::ZERO));
        // Expired entries are evicted, so the map stays bounded: after the
        // zero-window calls only the freshly re-inserted `a` remains.
        assert_eq!(limiter.last_warn.lock().unwrap().len(), 1);
    }
}
