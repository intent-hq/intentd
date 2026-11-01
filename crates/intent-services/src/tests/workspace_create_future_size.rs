//! Future-size budgets for `workspace.create` (`Services::create_workspace`).
//!
//! The create future runs close to the default stack limit in debug builds.
//! Growing it — most recently by rebinding the large `WorkspaceCreate` input
//! inside the `async move` block (intent-hq/intentd#2819) — showed up only as
//! an unrelated-looking "stack overflow" SIGABRT of
//! `wss_workspace_create_context_links_round_trip_and_validation` late in a
//! full test run (intent-hq/intent#6741). This test measures the unpolled
//! futures so the growth fails here, fast, with the fix in the message. Same
//! pattern as `agent_manager::future_size_tests` (turn-start path).
//!
//! Two sizes are measured:
//! - the create block: the `async move` future `create_workspace` hands to
//!   `Services::execution_call`, recorded there before it is boxed
//!   (`execution_call::take_last_inner_future_size`). One rebinding of the
//!   input adds about one `WorkspaceCreate` to it.
//! - the `execution_call` wrapper: the returned `BoxFuture`, read through the
//!   `dyn Future` vtable with `size_of_val(&*boxed)`. The wrapper boxes the
//!   create block once; un-boxing it again (holding the block inline) would
//!   store the block twice and grow the wrapper by about twice its size.
//!
//! Budgets are debug-profile sizes (the test profile, where the overflow
//! happened) measured on `x86_64` Linux plus headroom. Release builds and
//! other targets lay futures out differently, so the module only builds for
//! debug `x86_64` Linux (gated in `tests.rs`), the platform CI runs.
//! Re-measure on an `x86_64` Linux host (elsewhere the module is compiled
//! out): run `make test-select E='test(/future_size/)'` with a budget lowered
//! to 0; the failure message prints every size.
//!
//! Stored size is only half of the stack story: each inline await also costs
//! poll-frame stack that `size_of_val` cannot see (see `crate::stack_frame`).

use intent_core::{WorkspaceApi, WorkspaceCreate};
use intent_store::Store;

use super::{TempDb, WorkspacesRoot};
use crate::Services;

/// The create block (measured `7_680` bytes since its large awaits are built
/// off-frame by `boxed_await`, down from `17_744`; `WorkspaceCreate` is
/// `1_200`). Headroom stays below one `WorkspaceCreate` so a stray rebinding
/// or clone of the request inside the block trips it.
const CREATE_BLOCK_BUDGET: usize = 8 * 1024;
/// The `execution_call` wrapper around it (measured `2_544` bytes): holds the
/// create block behind one `Box::pin`. Holding it inline again grows this by
/// about `2 * 7_680`.
const EXECUTION_CALL_WRAPPER_BUDGET: usize = 4 * 1024;

#[tokio::test]
async fn workspace_create_future_size_stays_within_budget() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_root = WorkspacesRoot::new();
    let services = Services::new_isolated(store).with_workspaces_root(ws_root.path().to_path_buf());

    // Never polled: only the generated state-machine layouts are measured. No
    // `repositoryCheckout`, so building the future has no side effects.
    // Clear any size recorded by setup so only this call is measured.
    crate::execution_call::take_last_inner_future_size();
    let create = services.create_workspace(WorkspaceCreate::default(), Some("future-size".into()));
    let wrapper = std::mem::size_of_val(&*create);
    let block = crate::execution_call::take_last_inner_future_size();
    drop(create);
    assert!(
        block > 0,
        "create_workspace no longer hands its create block to Services::execution_call when \
         called, so the block size was not recorded; update this test to measure the block"
    );
    let input = std::mem::size_of::<WorkspaceCreate>();

    let measured = [
        ("workspace.create block", block, CREATE_BLOCK_BUDGET),
        (
            "execution_call wrapper",
            wrapper,
            EXECUTION_CALL_WRAPPER_BUDGET,
        ),
    ];
    let over: Vec<String> = measured
        .iter()
        .filter(|(_, size, budget)| size > budget)
        .map(|(name, size, budget)| format!("{name} future is {size} bytes (budget {budget})"))
        .collect();
    assert!(
        over.is_empty(),
        "{}. The workspace.create future grew, and it already runs close to the debug stack \
         limit (overflowed wss_workspace_create_context_links_round_trip_and_validation: \
         intent-hq/intent#6741, intent-hq/intentd#2819). Fix: move large awaits off the stack \
         with `Box::pin(..)` (or `crate::stack_frame::boxed_await(|| ..)`), and do not copy, \
         clone or rebind the `WorkspaceCreate` request ({input} bytes) inside the future — \
         mutate `input` in place. A large wrapper means `Services::execution_call` holds its \
         inner future inline again: keep the `Box::pin(future)` there. If the growth is \
         intentional and bounded (new locals, larger types, a toolchain bump), re-measure and \
         raise the budget in src/tests/workspace_create_future_size.rs, keeping the create \
         block's headroom below one request. All sizes: {measured:?}",
        over.join("; ")
    );
    // Keeps the block budget meaningful: headroom must stay below one copy of
    // the request, or a stray rebinding would slip under it.
    assert!(
        CREATE_BLOCK_BUDGET - block < input,
        "workspace.create block shrank to {block} bytes, leaving at least one WorkspaceCreate \
         ({input} bytes) of headroom under CREATE_BLOCK_BUDGET ({CREATE_BLOCK_BUDGET}), so a \
         stray rebinding would no longer trip it. Re-measure and tighten the budget"
    );
    // Keeps the wrapper budget meaningful: it only catches an inline create
    // block while that block is much larger than the budget.
    assert!(
        EXECUTION_CALL_WRAPPER_BUDGET < block,
        "workspace.create block ({block} bytes) is no longer larger than \
         EXECUTION_CALL_WRAPPER_BUDGET ({EXECUTION_CALL_WRAPPER_BUDGET}); an inline block in \
         the wrapper would slip under it. Re-measure and tighten the budgets"
    );
}
