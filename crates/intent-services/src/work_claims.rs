//! Periodic work under a store work claim (`Store::claim_work`).
//!
//! A periodic loop that several processes of one tenant may run keeps one
//! claim on its work: each tick renews the claim it holds or takes a fresh
//! one, and skips the run when another process holds it. A long run renews
//! once half the TTL has passed and stops when the claim is lost. The holder
//! keeps the claim between ticks, so exactly one process runs the work per
//! interval; a clean stop releases it and a crash lets it lapse after the
//! TTL. On `SQLite` (desktop) the claim is always this process's.

use std::time::Duration;

use intent_store::WorkClaim;

use crate::Services;

/// The work kind of the token-usage reconciliation sweep.
pub(crate) const TOKEN_USAGE_SCAN_WORK: &str = "token-usage-scan";

impl Services {
    /// Before a run: renews the claim in `held` or takes a fresh one on
    /// (`kind`, `""`) for `ttl`. `true` when this process holds the claim
    /// and should run. A lost claim is dropped and claimed again at once; a
    /// failed renewal keeps it (renewing next tick tells whether it is still
    /// ours) and skips this run.
    pub(crate) async fn hold_work_claim(
        &self,
        held: &mut Option<WorkClaim>,
        kind: &str,
        ttl: Duration,
    ) -> bool {
        if let Some(claim) = held.as_mut() {
            match self.store.renew_work_claim(claim).await {
                Ok(true) => return true,
                Ok(false) => {
                    tracing::info!(kind, "work claim lost; claiming again");
                    *held = None;
                }
                Err(e) => {
                    tracing::warn!(kind, error = %e, "work claim renewal failed; skipping this run");
                    return false;
                }
            }
        }
        match self.store.claim_work(kind, "", ttl).await {
            Ok(Some(claim)) => {
                *held = Some(claim);
                true
            }
            Ok(None) => {
                tracing::debug!(kind, "work claimed by another process; skipping this run");
                false
            }
            Err(e) => {
                tracing::warn!(kind, error = %e, "work claim failed; skipping this run");
                false
            }
        }
    }

    /// During a run: renews the claim in `held` once half its TTL has
    /// passed. `false` means stop the run: the claim was lost (and dropped)
    /// or its renewal failed. A run without a claim (`None`) always goes on.
    pub(crate) async fn keep_work_claim(&self, held: &mut Option<WorkClaim>) -> bool {
        let Some(claim) = held.as_mut() else {
            return true;
        };
        if !claim.needs_renewal() {
            return true;
        }
        match self.store.renew_work_claim(claim).await {
            Ok(true) => true,
            Ok(false) => {
                tracing::info!(kind = claim.kind(), "work claim lost mid-run; stopping");
                *held = None;
                false
            }
            Err(e) => {
                tracing::warn!(
                    kind = claim.kind(),
                    error = %e,
                    "work claim renewal failed mid-run; stopping"
                );
                false
            }
        }
    }

    /// Frees the claim in `held` at a clean stop, so another process can
    /// take the work without waiting for the TTL. Best effort: a failed
    /// release lapses after the TTL.
    pub(crate) async fn release_held_work_claim(&self, held: Option<WorkClaim>) {
        let Some(claim) = held else { return };
        let kind = claim.kind().to_owned();
        if let Err(e) = self.store.release_work_claim(claim).await {
            tracing::debug!(kind, error = %e, "work claim release failed; it lapses by TTL");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// Waits (without a fixed sleep) until `store` grants a claim on the
    /// token-usage work, i.e. until a short claim on it lapsed.
    async fn take_once_free(store: &Store) -> WorkClaim {
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            async {
                loop {
                    if let Some(c) = store
                        .claim_work(TOKEN_USAGE_SCAN_WORK, "", TTL)
                        .await
                        .unwrap()
                    {
                        break c;
                    }
                    tokio::task::yield_now().await;
                }
            },
        )
        .await
        .expect("the short claim lapses")
    }

    #[tokio::test]
    async fn a_tick_runs_only_while_this_process_holds_the_claim() {
        let (svc, store, ws, _dir) = services().await;
        let other = store
            .claim_work(TOKEN_USAGE_SCAN_WORK, "", TTL)
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

        store.release_work_claim(other).await.unwrap();
        assert!(svc.token_usage_scan_tick(&mut held, TTL).await);
        assert!(held.is_some(), "the holder keeps its claim between ticks");
        assert!(store
            .get_workspace(&ws)
            .await
            .unwrap()
            .token_usage
            .is_some());
        assert!(
            store
                .claim_work(TOKEN_USAGE_SCAN_WORK, "", TTL)
                .await
                .unwrap()
                .is_none(),
            "others skip while it holds the claim"
        );
        assert!(
            svc.token_usage_scan_tick(&mut held, TTL).await,
            "the holder's next tick renews and runs"
        );

        svc.release_held_work_claim(held.take()).await;
        assert!(
            store
                .claim_work(TOKEN_USAGE_SCAN_WORK, "", TTL)
                .await
                .unwrap()
                .is_some(),
            "a clean stop frees the work at once"
        );
    }

    #[tokio::test]
    async fn a_lost_claim_is_dropped_and_claimed_again() {
        let (svc, store, _ws, _dir) = services().await;
        let short = Duration::from_millis(1);
        let mut held = None;
        assert!(
            svc.hold_work_claim(&mut held, TOKEN_USAGE_SCAN_WORK, short)
                .await
        );
        // The claim lapses and another holder takes it.
        let other = take_once_free(&store).await;
        assert!(
            !svc.keep_work_claim(&mut held).await,
            "mid-run: a lost claim stops the run"
        );
        assert!(held.is_none(), "and is dropped");
        assert!(
            !svc.hold_work_claim(&mut held, TOKEN_USAGE_SCAN_WORK, TTL)
                .await
        );
        store.release_work_claim(other).await.unwrap();
        assert!(
            svc.hold_work_claim(&mut held, TOKEN_USAGE_SCAN_WORK, TTL)
                .await
        );
    }

    #[tokio::test]
    async fn a_lost_claim_at_tick_start_is_replaced_when_free() {
        let (svc, store, _ws, _dir) = services().await;
        let mut held = store
            .claim_work(TOKEN_USAGE_SCAN_WORK, "", Duration::from_millis(1))
            .await
            .unwrap();
        // Another holder takes the lapsed claim and frees it again: the row
        // no longer names `held`, so renewal fails and a fresh claim wins.
        let other = take_once_free(&store).await;
        store.release_work_claim(other).await.unwrap();
        assert!(
            svc.hold_work_claim(&mut held, TOKEN_USAGE_SCAN_WORK, TTL)
                .await
        );
        assert_eq!(held.as_ref().unwrap().ttl(), TTL, "a fresh claim");
    }

    #[tokio::test]
    async fn a_run_without_a_claim_goes_on() {
        let (svc, _store, _ws, _dir) = services().await;
        assert!(svc.keep_work_claim(&mut None).await);
        svc.release_held_work_claim(None).await;
    }

    /// A settings shutdown that lands mid-sweep (as a graceful drain's
    /// `close_admission` does before it aborts the loops) ends the sweep
    /// before the next workspace, and the loop ends on its own and releases
    /// the claim, so another process can take the work at once.
    #[tokio::test]
    async fn a_stop_during_a_sweep_releases_the_claim() {
        let (svc, store, first, _dir) = services().await;
        let second = WorkspaceId::new();
        store
            .insert_workspace(&crate::tests::workspace(&second))
            .await
            .unwrap();
        let (entered, release) = crate::periodic_shutdown_tests::hold(&svc, "token");
        let task = svc.spawn_token_usage_scan_loop_with_ttl(Duration::from_millis(1), TTL);
        // The sweep is in progress: one workspace's tally committed, held.
        crate::periodic_shutdown_tests::entered(entered).await;
        assert!(
            store
                .claim_work(TOKEN_USAGE_SCAN_WORK, "", TTL)
                .await
                .unwrap()
                .is_none(),
            "the sweeping loop holds the claim"
        );
        svc.begin_settings_shutdown();
        release.send(()).unwrap();
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            task,
        )
        .await
        .expect("the loop ends on its own once settings shutdown began")
        .expect("the loop was not aborted");
        assert!(
            store
                .claim_work(TOKEN_USAGE_SCAN_WORK, "", TTL)
                .await
                .unwrap()
                .is_some(),
            "the loop released its claim"
        );
        let mut scanned = 0;
        for ws in [&first, &second] {
            scanned += usize::from(store.get_workspace(ws).await.unwrap().token_usage.is_some());
        }
        assert_eq!(scanned, 1, "the sweep stopped before the next workspace");
    }
}
