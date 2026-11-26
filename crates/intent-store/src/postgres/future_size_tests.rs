//! The `postgres` feature must not grow `Store` or its futures.
//!
//! A `Store` future holds whichever backend future it awaits, so a Postgres
//! future held inline makes it as large as that future even on a `SQLite`
//! store, and every caller's future and debug poll frame grows with it. In
//! `intent-services` that added up to 100-300 KiB per frame and overflowed
//! 30 tests, only in builds with the feature (`dispatch!` now awaits Postgres
//! futures through [`super::boxed_call`], and `Backend` boxes the
//! `PgStore`). These tests measure a `SQLite` store and unpolled futures on
//! it: each must stay within a few words of its `SQLite` counterpart.

use std::mem::{size_of, size_of_val};

use intent_core::{AgentId, WorkspaceId};

use crate::{AgentQueueRow, Backend, SqliteStore, Store};

/// Room for the wrapper's own state: its arguments, `&self`, the backend
/// borrow and the boxed Postgres arm's pointer. A Postgres future held
/// inline exceeds it by kilobytes.
const WRAPPER_SLACK: usize = 256;

#[test]
fn store_stays_at_sqlite_size_with_the_postgres_feature() {
    // Every `Services` clone and many futures hold one or more `Store`s; an
    // inline `PgStore` grew the workspace.create block by 848 bytes.
    let (store, sqlite) = (size_of::<Store>(), size_of::<SqliteStore>());
    assert!(
        store <= sqlite + 16,
        "Store is {store} bytes, SqliteStore {sqlite}: keep Backend::Postgres boxed so the \
         postgres feature does not grow every Store"
    );
}

#[tokio::test]
async fn store_futures_stay_at_sqlite_size_with_the_postgres_feature() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(&dir.path().join("store.db"))
        .await
        .expect("open store");
    let Backend::Sqlite(sqlite) = &store.backend else {
        unreachable!("Store::open opens SQLite")
    };
    let agent = AgentId::from("future-size-agent");
    let workspace = WorkspaceId::from("future-size-workspace");
    let rows: Vec<AgentQueueRow> = Vec::new();

    // Never polled: only the generated layouts are measured.
    let measured = [
        (
            "get_agent_session_summary (dispatch!)",
            size_of_val(&store.get_agent_session_summary(&agent)),
            size_of_val(&sqlite.get_agent_session_summary(&agent)),
        ),
        (
            "get_workspace (dispatch!)",
            size_of_val(&store.get_workspace(&workspace)),
            size_of_val(&sqlite.get_workspace(&workspace)),
        ),
        (
            "script_monitor_pending_for_agent (dispatch!)",
            size_of_val(&store.script_monitor_pending_for_agent(&agent)),
            size_of_val(&sqlite.script_monitor_pending_for_agent(&agent)),
        ),
        (
            "sync_agent_queue (hand-written arms)",
            size_of_val(&store.sync_agent_queue(&agent, &rows, &[])),
            size_of_val(&sqlite.replace_agent_queue(&agent, &rows)),
        ),
        (
            "refresh_agent_queue (hand-written arms; SQLite awaits nothing)",
            size_of_val(&store.refresh_agent_queue(&agent, &rows)),
            0,
        ),
    ];
    let over: Vec<String> = measured
        .iter()
        .filter(|(_, wrapper, inner)| *wrapper > inner + WRAPPER_SLACK)
        .map(|(name, wrapper, inner)| {
            format!("Store::{name} is {wrapper} bytes; its SQLite future is {inner}")
        })
        .collect();
    assert!(
        over.is_empty(),
        "{}. A Store method holds its Postgres future inline: await it through \
         `crate::postgres::boxed_call(|| backend.method(args))` (dispatch!'s \
         `backend.method(args).await` form does). Held inline, it grows every caller's \
         future and debug poll frame and overflows intent-services test stacks in builds \
         with the postgres feature. All sizes (Store, SQLite): {measured:?}",
        over.join("; ")
    );
}
