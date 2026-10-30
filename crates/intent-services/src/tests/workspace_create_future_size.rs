//! Future-size budget for `workspace.create` (`Services::create_workspace`).
//!
//! The create future runs close to the default stack limit in debug builds.
//! Growing it — most recently by rebinding the large `WorkspaceCreate` input
//! inside the `async move` block (intent-hq/intentd#2819) — showed up only as
//! an unrelated-looking "stack overflow" SIGABRT of
//! `wss_workspace_create_context_links_round_trip_and_validation` late in a
//! full test run (intent-hq/intent#6741). This test measures the unpolled
//! future so the growth fails here, fast, with the fix in the message. Same
//! pattern as `agent_manager::future_size_tests` (turn-start path).
//!
//! `create_workspace` returns a `BoxFuture`; `size_of_val(&*boxed)` reads the
//! concrete size of the boxed state machine (the `execution_call` wrapper that
//! holds the create `async move` block) through the `dyn Future` vtable. The
//! wrapper stores the inner future twice, so every byte added to the create
//! block shows up about twice here: one rebinding of the input adds
//! `2 * size_of::<WorkspaceCreate>()`.
//!
//! The budget is the debug-profile size (the test profile, where the overflow
//! happened) measured on `x86_64` Linux, plus headroom smaller than one
//! rebinding (`2 * size_of::<WorkspaceCreate>()`). Release builds and other
//! targets lay futures out differently, so the module only builds for debug
//! `x86_64` Linux (gated in `tests.rs`), the platform CI runs. Re-measure: run
//! `make test-select E='test(/future_size/)'` with the budget lowered to 0;
//! the failure message prints the size.

use intent_core::{WorkspaceApi, WorkspaceCreate};
use intent_store::Store;

use super::{TempDb, WorkspacesRoot};
use crate::Services;

/// `Services::create_workspace` (measured `38_032` bytes; `WorkspaceCreate`
/// is `1_200`, so a stray rebinding adds `2_400`). Headroom stays below one
/// rebinding so it trips the budget.
const CREATE_WORKSPACE_BUDGET: usize = 38 * 1024;

#[tokio::test]
async fn workspace_create_future_size_stays_within_budget() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_root = WorkspacesRoot::new();
    let services = Services::new_isolated(store).with_workspaces_root(ws_root.path().to_path_buf());

    // Never polled: only the generated state-machine layout is measured. No
    // `repositoryCheckout`, so building the future has no side effects.
    let create = services.create_workspace(WorkspaceCreate::default(), Some("future-size".into()));
    let size = std::mem::size_of_val(&*create);
    drop(create);
    let input = std::mem::size_of::<WorkspaceCreate>();

    assert!(
        size <= CREATE_WORKSPACE_BUDGET,
        "Services::create_workspace future is {size} bytes (budget {CREATE_WORKSPACE_BUDGET}). \
         The workspace.create future grew, and it already runs close to the debug stack limit \
         (overflowed wss_workspace_create_context_links_round_trip_and_validation: \
         intent-hq/intent#6741, intent-hq/intentd#2819). Fix: move large awaits off the stack \
         with `Box::pin(..)` (or `crate::stack_frame::boxed_await(|| ..)`), and do not copy, \
         clone or rebind the `WorkspaceCreate` request ({input} bytes) inside the future — \
         mutate `input` in place. If the growth is intentional and bounded (new locals, larger \
         types, a toolchain bump), re-measure and raise CREATE_WORKSPACE_BUDGET in \
         src/tests/workspace_create_future_size.rs, keeping the headroom below one rebinding \
         (2 x the request)."
    );
    // Keeps the budget meaningful: a rebinding of the request inside the
    // create block adds about `2 * input` here (the wrapper holds the inner
    // future twice), so headroom must stay below that or it would slip under.
    assert!(
        CREATE_WORKSPACE_BUDGET - size < 2 * input,
        "Services::create_workspace future shrank to {size} bytes, leaving at least one \
         rebinding of WorkspaceCreate (2 x {input} bytes) of headroom under \
         CREATE_WORKSPACE_BUDGET ({CREATE_WORKSPACE_BUDGET}), so a stray rebinding would no \
         longer trip it. Re-measure and tighten the budget"
    );
}
