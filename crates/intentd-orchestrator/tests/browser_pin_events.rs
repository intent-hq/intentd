//! `workspace.setBrowserClient` on a Postgres tenant shared by two processes
//! (stateless replicas groundwork): each `Services` runs on its own store of
//! the tenant, with its own event bus, as two daemons would. The in-process
//! pin gate does not span processes, so the pin row and its
//! `workspace:updated` delta must commit in one transaction: then the
//! event log's seq order is the pin rows' commit order, and the last delta
//! in seq order is the committed pin.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    with_caller, Caller, ClientHostInfo, ClientId, Workspace, WorkspaceApi, WorkspaceId,
};
use intent_services::feature_api::EventBus;
use intent_services::{CompletionClassifyPark, Services};
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};
use intentd_test_support::test_timeout;
use serde_json::Value;

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let pool = postgres::connect(&url, 8).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    Some(pool)
}

async fn open(pool: &PgPool, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions::default(),
        process,
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open the tenant store")
}

/// A process's `Services` over `store`, with its own bus.
fn process(store: Store) -> Services {
    Services::new_isolated(store.clone()).with_event_bus(EventBus::new(store))
}

/// The pin each `workspace:updated` pin delta of `ws` announced, in seq
/// order (`null` for a cleared pin).
async fn pin_deltas(store: &Store, ws: &WorkspaceId) -> Vec<Value> {
    store
        .events_after(0, -1)
        .await
        .expect("read the event log")
        .into_iter()
        .map(|(event, _)| serde_json::to_value(event).unwrap())
        .filter(|event| {
            event["type"] == intent_core::events::WORKSPACE_UPDATED
                && event["workspaceId"] == ws.as_str()
        })
        .filter_map(|event| event["data"]["changes"].get("browserClientId").cloned())
        .collect()
}

/// A pins `desktop-a` and is held once its pin is written and its delta
/// decided; B clears the pin inside that window, so B's write commits
/// last. The log must end on B's delta (`null`), as the row does. With the
/// delta published after the write in its own transaction, A's delta
/// commits after B's and the log ends on a pin the row no longer has.
///
/// A reads the pin back before migrating claimed tabs, sees B's clear and
/// migrates nothing (an isolated `Services` has no runtime to migrate
/// with), so its call succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_last_pin_delta_in_seq_order_is_the_committed_pin_across_processes() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "browser-pin-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let ws = WorkspaceId("ws-pin".into());
    let now = intent_core::now_iso();
    store_a
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "pin".into(),
            branch: "main".into(),
            created_at: now.clone(),
            updated_at: now,
            ..Workspace::default()
        })
        .await
        .expect("seed the workspace");
    for id in ["desktop-a"] {
        store_a
            .upsert_client(
                &ClientId::from_string(id),
                Some(id),
                None,
                &ClientHostInfo::default(),
            )
            .await
            .expect("seed a hello'd client");
    }

    let park = Arc::new(CompletionClassifyPark::default());
    let a = process(store_a.clone()).with_browser_pin_park_for_test(park.clone());
    let b = process(store_b.clone());

    let pin_a = tokio::spawn({
        let ws = ws.clone();
        with_caller(Caller::Daemon, async move {
            a.set_workspace_browser_client(ws, Some(ClientId::from_string("desktop-a")))
                .await
        })
    });
    tokio::time::timeout(
        test_timeout(Duration::from_secs(30)),
        park.entered.notified(),
    )
    .await
    .expect("A reaches the window before its delta is published");
    with_caller(
        Caller::Daemon,
        b.set_workspace_browser_client(ws.clone(), None),
    )
    .await
    .expect("B clears the pin inside A's window");
    park.release.notify_one();
    let pinned_a = tokio::time::timeout(test_timeout(Duration::from_secs(30)), pin_a)
        .await
        .expect("A finishes")
        .unwrap();

    let committed = store_b
        .workspace_browser_client(&ws)
        .await
        .unwrap()
        .map_or(Value::Null, |c| c.as_str().into());
    assert_eq!(committed, Value::Null, "B's clear committed last");
    let deltas = pin_deltas(&store_b, &ws).await;
    assert_eq!(deltas.len(), 2, "one durable delta per pin: {deltas:?}");
    assert_eq!(
        deltas.last(),
        Some(&committed),
        "the last delta in seq order is the committed pin: {deltas:?}"
    );
    pinned_a.expect("A's pin");
}
