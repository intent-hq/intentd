//! Stack-headroom guard for `workspace.create` (`Services::create_workspace`).
//!
//! In debug builds every inline `.await` puts about twice the awaited future
//! in the caller's poll frame (see `crate::stack_frame`). The create path sits
//! under the transport's own deep poll frames (`router::dispatch` alone is
//! ~900 KB in debug), so growth here showed up only as an unrelated-looking
//! "has overflowed its stack" abort of
//! `wss_workspace_create_context_links_round_trip_and_validation`
//! (intent-hq/intent#6741, intent-hq/intentd#2819), and only in some builds:
//! the plain debug build needed ~4 KiB more than the 2 MiB default while CI's
//! instrumented build fit ~4 KiB under it.
//!
//! This test runs the same create cases as that wss test through `Services`
//! on a thread with a deliberately small stack, so a poll-frame regression on
//! the create path fails here, fast and in every debug build. The stored
//! future size is a separate budget (`size_of_val` cannot see poll frames).

use std::sync::Arc;

use intent_core::{Caller, ContextLink, ContextLinkKind, Error, WorkspaceApi, WorkspaceCreate};
use intent_store::Store;

use super::{TempDb, WorkspacesRoot};
use crate::test_support::stub_forge::StubForge;
use crate::{EventBus, Services};

/// Stack for the guard thread on `x86_64` Linux, where CI runs it. The
/// scenario needs about 324 KB there (debug, rustc 1.96) with the create
/// path's large awaits built off-frame by [`crate::stack_frame::boxed_await`];
/// before that it needed ~540 KB, mostly the create op's own 280 KB poll
/// frame. The ~36 KB of headroom is enough for run-to-run noise and small
/// toolchain or dependency drift, but not for one large future awaited inline
/// again (re-inlining the initial agent's `send_message` adds ~42 KB). Other
/// targets lay frames out differently, so they get a loose budget that only
/// catches gross regressions.
///
/// A stack overflow here (the test process aborts) means a large future is
/// awaited inline on the create path: build it with `boxed_await` instead of
/// raising this budget. Re-measure by bisecting this constant.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const CREATE_PATH_STACK_BUDGET: usize = 352 * 1024;
#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
const CREATE_PATH_STACK_BUDGET: usize = 1024 * 1024;

#[test]
fn workspace_create_runs_within_reduced_stack() {
    std::thread::Builder::new()
        .name("workspace-create-stack-guard".into())
        .stack_size(CREATE_PATH_STACK_BUDGET)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime")
                .block_on(intent_core::with_caller(
                    Caller::Daemon,
                    Box::pin(workspace_create_scenario()),
                ));
        })
        .expect("spawn the reduced-stack thread")
        .join()
        .expect("the scenario's assertions hold");
}

async fn workspace_create_scenario() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_root = WorkspacesRoot::new();
    // A forge stub so the PR context link takes the PR-aware branch (the
    // deepest point of the wss scenario) without network access.
    let services = Services::new_isolated(store.clone())
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(EventBus::new(store.clone()))
        .with_source_control(Arc::new(StubForge::default()));
    let links = vec![
        ContextLink {
            kind: ContextLinkKind::Issue,
            url: "https://github.com/intent-hq/intent/issues/42".to_string(),
            owner: "intent-hq".to_string(),
            repo: "intent".to_string(),
            number: 42,
        },
        ContextLink {
            kind: ContextLinkKind::Pr,
            url: "https://github.com/intent-hq/intentd/pull/7".to_string(),
            owner: "intent-hq".to_string(),
            repo: "intentd".to_string(),
            number: 7,
        },
    ];

    let created = services
        .create_workspace(
            WorkspaceCreate {
                title: Some("Stack guard links".to_string()),
                context_links: Some(links.clone()),
                ..Default::default()
            },
            Some("stack-guard-links".to_string()),
        )
        .await
        .expect("create with contextLinks")
        .workspace;
    assert_eq!(created.context_links.as_deref(), Some(&links[..]));

    let plain = services
        .create_workspace(
            WorkspaceCreate {
                title: Some("Stack guard plain".to_string()),
                ..Default::default()
            },
            Some("stack-guard-plain".to_string()),
        )
        .await
        .expect("create without contextLinks")
        .workspace;
    assert!(plain.context_links.is_none());

    let bad = services
        .create_workspace(
            WorkspaceCreate {
                title: Some("Stack guard invalid".to_string()),
                context_links: Some(vec![ContextLink {
                    url: String::new(),
                    ..links[1].clone()
                }]),
                ..Default::default()
            },
            Some("stack-guard-invalid".to_string()),
        )
        .await
        .expect_err("an empty contextLinks url rejects");
    assert!(
        matches!(&bad, Error::InvalidParams(msg) if msg.contains("contextLinks[0].url")),
        "expected InvalidParams naming contextLinks[0].url, got {bad:?}"
    );
}
