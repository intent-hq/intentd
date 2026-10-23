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
//! Budgets are debug-profile sizes (the test profile, where the overflows
//! happened) measured on `x86_64` Linux, plus headroom; release builds lay
//! futures out differently, so the test only runs with debug assertions.
//! Re-measure: run `cargo nextest run -p intent-services --lib future_size`
//! with a budget lowered to 0; the failure message lists every size.
//!
//! Removing only the `boxed_await(|| retry_spawn_owned(..))` in the worker does
//! not grow the worker today (its size is set by other awaits), so
//! `RETRY_SPAWN_BUDGET` is the guard for the inner `ensure_started_owned` box.
//!
//! Stored future size is only half of the stack story: in a debug build each
//! inline await also costs about twice the awaited future's size in the
//! caller's POLL FRAME, which `size_of_val` cannot see. That half is guarded
//! by `agent_manager::tests::turn_path_fits_reduced_stack`; see
//! `crate::stack_frame`.

use super::tests::manager;
use super::*;

/// `ensure_started_owned` itself (measured `7_824` bytes). Its callers box
/// it; this cap only catches runaway growth of the body.
const ENSURE_STARTED_BUDGET: usize = 10 * 1024;
/// `retry_spawn_owned` (measured `568` bytes): holds `ensure_started_owned`
/// behind `boxed_await`. Awaiting it inline grows this to ~8 KiB.
const RETRY_SPAWN_BUDGET: usize = 2 * 1024;
/// `run_message_worker` (measured `16_256` bytes): holds `retry_spawn_owned`,
/// the monitor turn and its other large awaits behind `boxed_await`.
const MESSAGE_WORKER_BUDGET: usize = 20 * 1024;

#[cfg(debug_assertions)]
#[tokio::test]
async fn turn_start_future_sizes_stay_within_budget() {
    let (_db, mgr) = manager().await;
    let mgr = Arc::new(mgr);
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

    let measured = [
        (
            "AgentManager::ensure_started_owned",
            ensure_started,
            ENSURE_STARTED_BUDGET,
        ),
        ("retry_spawn_owned", retry_spawn, RETRY_SPAWN_BUDGET),
        ("run_message_worker", worker, MESSAGE_WORKER_BUDGET),
    ];
    let over: Vec<String> = measured
        .iter()
        .filter(|(_, size, budget)| size > budget)
        .map(|(name, size, budget)| format!("{name} future is {size} bytes (budget {budget})"))
        .collect();
    assert!(
        over.is_empty(),
        "{}. An inline `.await` of a large future on the agent turn-start path grew it, which \
         overflows the stack on deep send/interrupt paths (intent-hq/intent#6658, \
         intent-hq/intent#6741): build the large future off-frame with \
         `crate::stack_frame::boxed_await(|| ...)`. If no await was \
         added (new locals, larger types, a toolchain bump), re-measure and raise the budget \
         in agent_manager/future_size_tests.rs only if the growth is intentional and bounded. \
         All sizes: {measured:?}",
        over.join("; ")
    );
    // Keeps the retry budget meaningful: it only catches an un-boxed
    // `ensure_started_owned` while that future is much larger than the budget.
    assert!(
        RETRY_SPAWN_BUDGET * 2 < ensure_started,
        "ensure_started_owned ({ensure_started} bytes) is no longer much larger than \
         RETRY_SPAWN_BUDGET ({RETRY_SPAWN_BUDGET}); an un-boxed await would slip under \
         it. Re-measure and tighten the budgets"
    );
}
