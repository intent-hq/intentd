//! One script budget for every 63/64-record acquisition-budget control.
//!
//! These controls fill one real acquisition ledger with 63 or 64 snapshot
//! calls inside a single `workspace_api` script. On a loaded host one call can
//! take ~0.9s (load average 31–49: 34 of 63 calls finished in 30s), so the
//! production 30-second script budget cut the loop short. That timeout reply
//! looks like the refusal a full ledger returns, so a control could fail for
//! no reason (63) or pass on the timer instead of the ledger (64).
//!
//! The budget is a fixed allowance, not `INTENTD_TEST_TIMEOUT_MULTIPLIER`
//! scaled: the hang guard above it must stay under the controls' nextest kill
//! (`.config/nextest.toml`, 540s), and a scaled script budget would cross it.
//! It is also deliberately not a default of intent-acp's `test-hooks` feature:
//! Cargo unifies that feature across a workspace build, so a scaled default
//! would silently change the production 30s for every test binary built with
//! intent-services (and the daemon the e2e suites spawn), including the tests
//! that pin it. Each control opts in here instead, and [`guard`] refuses to
//! run one whose served server still carries any other budget.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

use intent_acp::WorkspaceMcpServer;

/// The `workspace_api` budget a 63/64 control's server must carry.
pub(crate) const QUOTA_SCRIPT_BUDGET: Duration = Duration::from_secs(150);

/// The outer wait around one control script: above [`QUOTA_SCRIPT_BUDGET`] so
/// the script's own limit is what a slow run reaches, and unscaled so it
/// always fires before the 540s nextest kill (two passes per control).
// fixed-deadline: allow — stays under the 540s quota-control nextest terminate budget
pub(crate) const QUOTA_HANG_GUARD: Duration = Duration::from_secs(180);

/// The transport ceiling for a guarded control's reply (TCP read, fixture
/// RPC): above [`QUOTA_HANG_GUARD`] so the guard, which names the cause,
/// always fires first.
// fixed-deadline: allow — stays under the 540s quota-control nextest terminate budget
pub(crate) const QUOTA_TRANSPORT_WAIT: Duration = Duration::from_secs(210);

/// Give `server` the quota script budget.
pub(crate) fn budget(server: WorkspaceMcpServer) -> WorkspaceMcpServer {
    server.with_workspace_api_timeout(QUOTA_SCRIPT_BUDGET)
}

/// Run one 63/64 control script whose serving server carries `served`.
///
/// Fails loudly, rather than letting the timer stand in for the ledger, when
/// the served server does not carry [`QUOTA_SCRIPT_BUDGET`], when the call
/// outlives [`QUOTA_HANG_GUARD`], or when it ran long enough to reach the
/// script budget.
pub(crate) async fn guard<T>(subcase: &str, served: Duration, call: impl Future<Output = T>) -> T {
    assert_eq!(
        served, QUOTA_SCRIPT_BUDGET,
        "{subcase}: a 63/64 acquisition control must serve its script with the \
         quota budget (test_support::quota_script), not {served:?}"
    );
    let started = Instant::now();
    // fixed-deadline: allow — stays under the 540s quota-control nextest terminate budget
    let result = tokio::time::timeout(QUOTA_HANG_GUARD, call).await;
    let elapsed = started.elapsed();
    eprintln!(
        "quota-script subcase={subcase} budget_ms={} elapsed_ms={} returned={}",
        served.as_millis(),
        elapsed.as_millis(),
        result.is_ok()
    );
    let Ok(result) = result else {
        panic!(
            "{subcase}: quota script hung past its {served:?} script budget; \
             the outer hang guard expired after {elapsed:?}"
        );
    };
    assert!(
        elapsed < served,
        "{subcase}: quota script hit its {served:?} time limit after {elapsed:?}; \
         its reply reflects the timer, not the acquisition ledger"
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[should_panic(expected = "not 30s")]
    async fn guard_refuses_the_unscaled_production_budget() {
        guard("unscaled", Duration::from_secs(30), async {}).await;
    }

    #[tokio::test]
    #[should_panic(expected = "time limit")]
    async fn guard_fails_a_run_that_reaches_its_budget() {
        tokio::time::pause();
        guard("timer", QUOTA_SCRIPT_BUDGET, async {
            tokio::time::sleep(QUOTA_SCRIPT_BUDGET).await;
        })
        .await;
    }

    #[tokio::test]
    async fn guard_returns_a_ledger_outcome_within_budget() {
        assert_eq!(guard("ok", QUOTA_SCRIPT_BUDGET, async { 7 }).await, 7);
    }
}
