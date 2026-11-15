//! Stack guard for [`super::dispatch`].
//!
//! In a debug build a poll frame reserves room for the locals of every
//! `match` arm. When `dispatch` was one ~4,900-line match its poll frame was
//! ~900 KiB, so every RPC reserved it and the repository-resource WSS tests
//! ran within 32 KiB of the 2 MiB test stack (intentd#3399 was ejected from
//! the merge queue twice). `dispatch` now routes each namespace to its own
//! function, awaited off-frame, so a call reserves only one namespace's frame.
//!
//! This test drives every router method through `dispatch` on a thread with
//! a small stack. It aborts with a stack overflow when a namespace function
//! grows too large: split that namespace, or move a large arm into its own
//! function awaited through `crate::stack_frame::boxed_await`.

use std::time::Duration;

use intent_core::WorkspaceApi;
use serde_json::{Map, Value};

use super::dispatch;
use crate::catalog::ROUTER_METHODS;

/// Fits the largest namespace frame with headroom; the single-match
/// `dispatch` needed about 900 KiB.
const DISPATCH_STACK_BYTES: usize = 384 * 1024;

/// Every method falls back to the trait's default (an error or an empty
/// result), so only the router's own frames use the stack.
struct DefaultApi;

impl WorkspaceApi for DefaultApi {}

#[test]
fn every_router_method_dispatches_on_a_small_stack() {
    std::thread::Builder::new()
        .name("dispatch-stack-guard".into())
        .stack_size(DISPATCH_STACK_BYTES)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async {
                let params = Map::<String, Value>::new();
                for method in ROUTER_METHODS {
                    let _ = tokio::time::timeout(
                        intentd_test_support::test_timeout(Duration::from_secs(5)),
                        dispatch(&DefaultApi, method, &params),
                    )
                    .await;
                }
            });
        })
        .expect("spawn the guard thread")
        .join()
        .expect("dispatch completed on a small stack");
}
