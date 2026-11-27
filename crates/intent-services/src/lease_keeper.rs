//! The token-usage scan's lease (`intent_store::LeaseKeeper`): one lease on
//! [`TOKEN_USAGE_SCAN_SCOPE`], so processes sharing a tenant sweep once per
//! interval between them. On `SQLite` (desktop) the lease is always this
//! process's.

pub use intent_store::{LeaseFence, LeaseKeeper};

/// The lease scope of the token-usage reconciliation sweep.
pub(crate) const TOKEN_USAGE_SCAN_SCOPE: &str = "token-usage-scan";

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::Services;
    use intent_core::WorkspaceId;
    use intent_store::Store;

    const TTL: Duration = Duration::from_secs(600);

    async fn services() -> (Services, Store, WorkspaceId, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&crate::tests::workspace(&ws))
            .await
            .unwrap();
        let svc = Services::new_with_file_secrets(
            store.clone(),
            intent_core::FileSecretStore::with_path(dir.path().join("secrets.json")),
        );
        (svc, store, ws, dir)
    }

    fn timeout() -> Duration {
        intentd_test_support::test_timeout(Duration::from_secs(10))
    }

    #[tokio::test]
    async fn a_tick_runs_only_while_this_process_holds_the_lease() {
        let (svc, store, ws, _dir) = services().await;
        let other = store
            .acquire_lease(TOKEN_USAGE_SCAN_SCOPE, "", TTL)
            .await
            .unwrap()
            .unwrap();
        let mut held = None;
        assert!(
            !svc.token_usage_scan_tick(&mut held, TTL).await,
            "another holder: skip the tick"
        );
        assert!(held.is_none());
        assert!(store
            .get_workspace(&ws)
            .await
            .unwrap()
            .token_usage
            .is_none());

        store.release_lease(other).await.unwrap();
        assert!(svc.token_usage_scan_tick(&mut held, TTL).await);
        assert!(held.is_some(), "the holder keeps its lease between ticks");
        assert!(store
            .get_workspace(&ws)
            .await
            .unwrap()
            .token_usage
            .is_some());
        assert!(
            store
                .acquire_lease(TOKEN_USAGE_SCAN_SCOPE, "", TTL)
                .await
                .unwrap()
                .is_none(),
            "others skip while it holds the lease"
        );
        let token = held.as_ref().unwrap().token();
        assert!(
            svc.token_usage_scan_tick(&mut held, TTL).await,
            "the holder's next tick runs under the same lease"
        );
        assert_eq!(held.as_ref().unwrap().token(), token);

        svc.release_token_usage_lease(held.take()).await;
        assert!(
            store
                .acquire_lease(TOKEN_USAGE_SCAN_SCOPE, "", TTL)
                .await
                .unwrap()
                .is_some(),
            "a clean stop frees the lease at once"
        );
    }

    #[tokio::test]
    async fn a_lost_lease_is_dropped_and_acquired_again_when_free() {
        let (svc, store, _ws, _dir) = services().await;
        let mut held = None;
        assert!(
            svc.hold_token_usage_lease(&mut held, Duration::from_millis(40))
                .await
        );
        let keeper = held.as_ref().unwrap();
        // Another holder takes the lapsed lease (renewals held off).
        let other = {
            let _lock = keeper.lease().await;
            tokio::time::timeout(timeout(), async {
                loop {
                    if let Some(l) = store
                        .acquire_lease(TOKEN_USAGE_SCAN_SCOPE, "", TTL)
                        .await
                        .unwrap()
                    {
                        break l;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the lease lapses")
        };
        tokio::time::timeout(timeout(), keeper.fired())
            .await
            .expect("the keeper notices");
        assert!(
            !svc.hold_token_usage_lease(&mut held, TTL).await,
            "lost and held elsewhere: skip"
        );
        assert!(held.is_none(), "the lost lease is dropped");
        store.release_lease(other).await.unwrap();
        assert!(svc.hold_token_usage_lease(&mut held, TTL).await);
        assert_eq!(held.as_ref().unwrap().token(), 3, "a fresh lease");
    }

    /// A settings shutdown that lands mid-sweep (as a graceful drain's
    /// `close_admission` does before it aborts the loops) ends the sweep
    /// before the next workspace, and the loop ends on its own and releases
    /// the lease, so another process can take the work at once.
    #[tokio::test]
    async fn a_stop_during_a_sweep_releases_the_lease() {
        let (svc, store, first, _dir) = services().await;
        let second = WorkspaceId::new();
        store
            .insert_workspace(&crate::tests::workspace(&second))
            .await
            .unwrap();
        let commit = crate::periodic_shutdown_tests::hold(&svc, "token");
        let task = svc.spawn_token_usage_scan_loop_with_ttl(Duration::from_millis(1), TTL);
        // The sweep is in progress: one workspace's tally committed, held.
        crate::periodic_shutdown_tests::entered(&commit).await;
        assert!(
            store
                .acquire_lease(TOKEN_USAGE_SCAN_SCOPE, "", TTL)
                .await
                .unwrap()
                .is_none(),
            "the sweeping loop holds the lease"
        );
        svc.begin_settings_shutdown();
        commit.release();
        tokio::time::timeout(timeout(), task)
            .await
            .expect("the loop ends on its own once settings shutdown began")
            .expect("the loop was not aborted");
        assert!(
            store
                .acquire_lease(TOKEN_USAGE_SCAN_SCOPE, "", TTL)
                .await
                .unwrap()
                .is_some(),
            "the loop released its lease"
        );
        let mut scanned = 0;
        for ws in [&first, &second] {
            scanned += usize::from(store.get_workspace(ws).await.unwrap().token_usage.is_some());
        }
        assert_eq!(scanned, 1, "the sweep stopped before the next workspace");
    }
}
