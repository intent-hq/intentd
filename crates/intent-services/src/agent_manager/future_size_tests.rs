//! Future-size budgets for the agent turn-start path.
//!
//! The turn-start future (`ensure_started_owned`: spawn, session open,
//! identity repairs) is large. Awaiting it — or any other large future —
//! inline in the spawn-retry loop or the turn worker copies its whole state
//! into the caller's future, and deep send/interrupt paths then overflow the
//! stack. That surfaced only as unrelated-looking SIGABRT "stack overflow"
//! failures late in full test runs (intent-hq/intent#6658,
//! intent-hq/intent#6741, twice during intent-hq/intentd#2453).
//!
//! These tests measure the unpolled futures with `size_of_val` (the pattern in
//! `intent-sourcecontrol`'s `traffic.rs`) so the growth fails here, at the
//! moment it is introduced, with the fix in the message.
//!
//! Re-measure: set a budget to 0 and run
//! `cargo nextest run -p intent-services --lib future_size`; the failure
//! message prints the current size. Budgets are debug-profile sizes (the test
//! profile, where the overflows happened) plus headroom.

use super::*;
use crate::events::EventBus;
use crate::test_support::test_tempdir;
use intent_store::Store;

/// `ensure_started_owned` itself (measured `28_912` bytes). Large by design —
/// its callers box it; this cap only catches runaway growth of the body.
const ENSURE_STARTED_BUDGET: usize = 36 * 1024;
/// `retry_spawn_owned` (measured `6_864` bytes): holds `ensure_started_owned`
/// behind `Box::pin`. Awaiting it inline grows this to ~29 KiB.
const RETRY_SPAWN_BUDGET: usize = 9 * 1024;
/// `run_message_worker` (measured `27_520` bytes): holds `retry_spawn_owned`
/// and the monitor turn behind `Box::pin`. Inlining the whole turn-start
/// chain grows this to ~40 KiB.
const MESSAGE_WORKER_BUDGET: usize = 34 * 1024;

fn assert_within_budget(name: &str, size: usize, budget: usize) {
    assert!(
        size <= budget,
        "{name} future is {size} bytes, over its {budget}-byte budget. A new inline \
         `.await` on the agent turn-start path grew it, which overflows the stack on deep \
         send/interrupt paths (intent-hq/intent#6658, intent-hq/intent#6741). Wrap the \
         large await in `Box::pin(...)`. Raise the budget in \
         agent_manager/future_size_tests.rs only if the growth is intentional and bounded."
    );
}

async fn manager() -> (tempfile::TempDir, Arc<AgentManager>) {
    let dir = test_tempdir("intentd-future-size-");
    let store = Store::open(&dir.path().join("store.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store).with_event_bus(bus.clone());
    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
    (dir, Arc::new(AgentManager::new(services, sink, 8)))
}

#[tokio::test]
async fn turn_start_future_sizes_stay_within_budget() {
    let (_dir, mgr) = manager().await;
    let agent = AgentId::from("future-size-agent");
    let ws = WorkspaceId::from("future-size-workspace");

    // Never polled: only the generated state-machine layout is measured.
    let ensure_started = std::mem::size_of_val(&mgr.ensure_started_owned(&agent, &ws));
    let retry_spawn = std::mem::size_of_val(&retry_spawn_owned(&mgr, &agent, &ws));
    let worker = std::mem::size_of_val(&run_message_worker(
        mgr.clone(),
        agent.clone(),
        ws.clone(),
        String::new(),
        TurnOptions::default(),
        false,
        TurnAdmission(0),
    ));

    assert_within_budget(
        "AgentManager::ensure_started_owned",
        ensure_started,
        ENSURE_STARTED_BUDGET,
    );
    assert_within_budget("retry_spawn_owned", retry_spawn, RETRY_SPAWN_BUDGET);
    assert_within_budget("run_message_worker", worker, MESSAGE_WORKER_BUDGET);
    // Keeps the retry budget meaningful: it only catches an un-boxed
    // `ensure_started_owned` while that future is much larger than the budget.
    assert!(
        RETRY_SPAWN_BUDGET * 2 < ensure_started,
        "ensure_started_owned ({ensure_started} bytes) is no longer much larger than \
         RETRY_SPAWN_BUDGET ({RETRY_SPAWN_BUDGET}); an un-boxed await would slip under \
         it. Re-measure and tighten the budgets"
    );
}
