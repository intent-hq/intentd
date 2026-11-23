//! The default tenant's client listeners: `intentd`'s own local socket and
//! WSS listener, with `intentd`'s auth, serving the tenant's `Services`.
//! Every connection joins the tenant's client connections, which its fence
//! closes.
//!
//! The `SQLite` tests cover the listeners alone. The Postgres test (needs
//! `--features postgres` and `INTENT_STORE_TEST_PG_URL`, set by the
//! `store-postgres` CI job; skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`) serves a real tenant and fires its fence.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{refused_status, result, until, ws_ends, ws_rpc, wss, Uds};
use intent_app::tenant::ClientConnections;
use intent_services::{EventBus, Services};
use intent_transport::auth::TOKEN_ACCOUNT;
use intent_transport::PrimaryReverseRegistry;
use intentd_orchestrator::listen::{start_client_listeners, ListenConfig};
use serde_json::{json, Value};

fn config(dir: &Path, insecure: bool) -> ListenConfig {
    ListenConfig {
        socket_path: dir.join("o.sock"),
        bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
        ws_port: 0,
        tls_dir: dir.join("tls"),
        insecure,
    }
}

async fn sqlite_tenant(dir: &Path) -> (Services, EventBus, Arc<PrimaryReverseRegistry>) {
    let store = intent_store::Store::open(&dir.join("db.sqlite"))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let reverse = Arc::new(PrimaryReverseRegistry::new());
    let services = Services::new_isolated(store)
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(reverse.clone())
        .with_workspaces_root(dir.join("workspaces"));
    (services, bus, reverse)
}

fn names(listed: &Value, key: &str, field: &str) -> Vec<String> {
    listed[key]
        .as_array()
        .unwrap_or_else(|| panic!("no {key} in {listed}"))
        .iter()
        .map(|item| item[field].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn wss_takes_the_tenant_owner_token_and_the_socket_needs_none() {
    let dir = tempfile::tempdir().unwrap();
    let (services, bus, reverse) = sqlite_tenant(dir.path()).await;
    let connections = ClientConnections::default();
    let listeners = start_client_listeners(
        &config(dir.path(), false),
        &services,
        &bus,
        reverse,
        &connections,
    )
    .await
    .unwrap();
    let port = listeners.ws_port();
    assert_ne!(port, 0);
    let fingerprint = listeners.fingerprint().expect("TLS is on").to_owned();
    let token = services
        .secret_source()
        .load(TOKEN_ACCOUNT)
        .unwrap()
        .expect("the owner token is kept in the tenant's secret store");

    for wrong in [None, Some("0".repeat(64))] {
        let refused = wss(port, &fingerprint, wrong.as_deref())
            .await
            .expect_err("no or a wrong token is refused");
        assert_eq!(refused_status(&refused), Some(401), "{refused:?}");
    }

    let mut ws = wss(port, &fingerprint, Some(&token)).await.unwrap();
    let hello = ws_rpc(&mut ws, 1, "client.hello", json!({ "clientId": "web-a" })).await;
    result(&hello);
    let created = ws_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({ "title": "Listener WS" }),
    )
    .await;
    let workspace = result(&created)["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let note = ws_rpc(
        &mut ws,
        3,
        "note.create",
        json!({ "workspaceId": workspace, "title": "Over WSS" }),
    )
    .await;
    result(&note);

    let mut uds = Uds::connect(listeners.socket_path()).await;
    let listed = uds.rpc(1, "workspace.list", json!({})).await;
    assert_eq!(
        names(result(&listed), "workspaces", "id"),
        std::slice::from_ref(&workspace)
    );
    let notes = uds
        .rpc(2, "note.list", json!({ "workspaceId": workspace }))
        .await;
    assert!(
        names(result(&notes), "notes", "title").contains(&"Over WSS".to_owned()),
        "{notes}"
    );
    let pairing = uds.rpc(3, "server.pairingInfo", json!({})).await;
    assert_eq!(result(&pairing)["port"], json!(port), "{pairing}");
    assert_eq!(result(&pairing)["token"], json!(token), "{pairing}");
    let remote = ws_rpc(&mut ws, 4, "server.pairingInfo", json!({})).await;
    assert!(
        remote.get("error").is_some(),
        "pairing is local-only: {remote}"
    );

    until("both connections to join the tenant", || {
        connections.len() == 2
    })
    .await;
    listeners.stop().await;
}

#[tokio::test]
async fn closing_the_tenant_connections_ends_both_listeners_clients() {
    let dir = tempfile::tempdir().unwrap();
    let (services, bus, reverse) = sqlite_tenant(dir.path()).await;
    let connections = ClientConnections::default();
    let listeners = start_client_listeners(
        &config(dir.path(), false),
        &services,
        &bus,
        reverse,
        &connections,
    )
    .await
    .unwrap();
    let fingerprint = listeners.fingerprint().unwrap().to_owned();
    let token = services
        .secret_source()
        .load(TOKEN_ACCOUNT)
        .unwrap()
        .unwrap();
    let mut ws = wss(listeners.ws_port(), &fingerprint, Some(&token))
        .await
        .unwrap();
    let mut uds = Uds::connect(listeners.socket_path()).await;
    result(&uds.rpc(1, "workspace.list", json!({})).await);
    until("both connections to join the tenant", || {
        connections.len() == 2
    })
    .await;

    assert_eq!(connections.close_all(), 2);
    ws_ends(&mut ws).await;
    uds.ends().await;

    let late = wss(listeners.ws_port(), &fingerprint, Some(&token))
        .await
        .expect_err("a fenced tenant admits no connection");
    assert_eq!(refused_status(&late), Some(503), "{late:?}");
    let mut late_uds = Uds::connect(listeners.socket_path()).await;
    late_uds.send(1, "workspace.list", json!({})).await;
    late_uds.ends().await;
    assert!(connections.is_empty());
    listeners.stop().await;
}

#[tokio::test]
async fn the_insecure_dev_seat_serves_plain_ws_without_a_token() {
    let dir = tempfile::tempdir().unwrap();
    let (services, bus, reverse) = sqlite_tenant(dir.path()).await;
    let connections = ClientConnections::default();
    let listeners = start_client_listeners(
        &config(dir.path(), true),
        &services,
        &bus,
        reverse,
        &connections,
    )
    .await
    .unwrap();
    assert_eq!(listeners.fingerprint(), None);
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}/ws", listeners.ws_port()))
            .await
            .unwrap();
    result(&ws_rpc(&mut ws, 1, "workspace.list", json!({})).await);
    until("the connection to join the tenant", || {
        connections.len() == 1
    })
    .await;
    assert_eq!(connections.close_all(), 1);
    ws_ends(&mut ws).await;
    listeners.stop().await;
}

#[tokio::test]
async fn a_socket_that_cannot_bind_fails_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let (services, bus, reverse) = sqlite_tenant(dir.path()).await;
    let mut config = config(dir.path(), true);
    config.socket_path = dir.path().join("missing").join("file").join("o.sock");
    std::fs::write(dir.path().join("missing"), "a file, not a directory").unwrap();
    let error = start_client_listeners(
        &config,
        &services,
        &bus,
        reverse,
        &ClientConnections::default(),
    )
    .await
    .err()
    .expect("the socket cannot bind");
    assert!(error.contains("local socket"), "{error}");
}

#[cfg(feature = "postgres")]
mod postgres {
    use intent_store::postgres::{self, PgPool};
    use intent_store::OrchestratorOnly;
    use intentd_orchestrator::serve::{serve_tenant, LeaseTiming};

    use super::*;
    use common::refused_late;

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

    /// A client of a Postgres tenant lists workspaces and creates and lists notes
    /// over WSS and the local socket, the results come from Postgres, and
    /// the tenant's fence closes both connections and refuses new ones.
    #[tokio::test]
    async fn a_postgres_tenant_serves_clients_until_its_fence_closes_them() {
        let Some(pool) = pool().await else { return };
        let dir = tempfile::tempdir().unwrap();
        let tenant = format!("listener-{}", uuid::Uuid::new_v4().simple());
        let mut served = serve_tenant(
            pool.clone(),
            &tenant,
            dir.path(),
            &format!("orchestrator-test-{tenant}"),
            LeaseTiming::default(),
            OrchestratorOnly::for_tests(),
        )
        .await
        .expect("serve the tenant");
        let listeners = served.listen(&config(dir.path(), false)).await.unwrap();
        let fingerprint = listeners.fingerprint().unwrap().to_owned();
        let token = served
            .services()
            .secret_source()
            .load(TOKEN_ACCOUNT)
            .unwrap()
            .unwrap();

        let mut ws = wss(listeners.ws_port(), &fingerprint, Some(&token))
            .await
            .unwrap();
        let dispatch = served
            .services()
            .feature_reverse_dispatch()
            .cloned()
            .expect("the tenant's Services carry its reverse registry");
        assert!(!dispatch.is_connected());
        result(
            &ws_rpc(
                &mut ws,
                1,
                "client.hello",
                json!({ "clientId": "web-pg", "capabilities": { "browserExec": true } }),
            )
            .await,
        );
        assert!(
            dispatch.is_connected(),
            "agent browser actions reach the listener's desktop"
        );
        // In orchestrator mode `workspace.create` needs a home machine, which
        // this test has none of, so the workspace is put in the tenant's store
        // directly; everything after that goes over the wire.
        let workspace = format!("ws-{}", uuid::Uuid::new_v4().simple());
        let now = intent_core::now_iso();
        served
            .services()
            .store()
            .insert_workspace(&intent_core::Workspace {
                id: intent_core::WorkspaceId(workspace.clone()),
                title: "On Postgres".into(),
                branch: "main".into(),
                created_at: now.clone(),
                updated_at: now,
                ..Default::default()
            })
            .await
            .expect("seed the workspace");
        result(
            &ws_rpc(
                &mut ws,
                3,
                "note.create",
                json!({ "workspaceId": workspace, "title": "Kept in Postgres" }),
            )
            .await,
        );
        let listed = ws_rpc(&mut ws, 4, "workspace.list", json!({})).await;
        assert!(
            names(result(&listed), "workspaces", "id").contains(&workspace),
            "{listed}"
        );

        let mut uds = Uds::connect(listeners.socket_path()).await;
        let notes = uds
            .rpc(1, "note.list", json!({ "workspaceId": workspace }))
            .await;
        assert!(
            names(result(&notes), "notes", "title").contains(&"Kept in Postgres".to_owned()),
            "{notes}"
        );
        let row: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM note WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(&tenant)
        .bind(&workspace)
        .fetch_one(&pool)
        .await
        .expect("count the tenant's notes");
        assert!(row >= 1, "the note is a row of the tenant in Postgres");
        until("both connections to join the tenant", || {
            served.runtime().connections().len() == 2
        })
        .await;

        served.fence().fire("test: the lease is lost");
        let report = tokio::time::timeout(common::wait(), served.stopped())
            .await
            .expect("the fence stops the tenant");
        assert!(report.overran.is_empty(), "{:?}", report.overran);
        ws_ends(&mut ws).await;
        uds.ends().await;
        assert!(served.runtime().connections().is_closed());

        // The listeners stop with the fence: nothing answers any more.
        let socket = listeners.socket_path().to_path_buf();
        until("the local socket to go", || !socket.exists()).await;
        let late = wss(listeners.ws_port(), &fingerprint, Some(&token))
            .await
            .expect_err("a fenced tenant admits no connection");
        assert!(refused_late(&late), "{late:?}");
        assert!(tokio::net::UnixStream::connect(&socket).await.is_err());
        listeners.stop().await;
    }

    /// Serve `tenant` as `instance`, with its files under `data_dir`.
    async fn open_as(
        pool: &PgPool,
        tenant: &str,
        data_dir: &Path,
        instance: &str,
    ) -> intent_core::Result<intentd_orchestrator::serve::ServedTenant> {
        serve_tenant(
            pool.clone(),
            tenant,
            data_dir,
            instance,
            LeaseTiming::default(),
            OrchestratorOnly::for_tests(),
        )
        .await
    }

    /// A takeover as a standby sees it: B's claim is refused while A serves;
    /// once A stops and releases its lease, B claims the tenant, binds its
    /// own listeners, and a client reconnecting to B with the same owner
    /// token (the tenant's, shared through the tenant's secret store) reads
    /// what it wrote through A.
    #[tokio::test]
    async fn a_released_tenant_is_taken_over_with_its_clients_token_and_data() {
        let Some(pool) = pool().await else { return };
        let dir = tempfile::tempdir().unwrap();
        let tenant = format!("takeover-{}", uuid::Uuid::new_v4().simple());
        let listen_as = |name: &str| ListenConfig {
            socket_path: dir.path().join(format!("{name}.sock")),
            bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
            ws_port: 0,
            tls_dir: dir.path().join("tls"),
            insecure: false,
        };

        let mut a = open_as(&pool, &tenant, dir.path(), &format!("a-{tenant}"))
            .await
            .expect("A serves the tenant");
        let a_listeners = a.listen(&listen_as("a")).await.unwrap();
        let fingerprint = a_listeners.fingerprint().unwrap().to_owned();
        let token = a
            .services()
            .secret_source()
            .load(TOKEN_ACCOUNT)
            .unwrap()
            .unwrap();
        let workspace = format!("ws-{}", uuid::Uuid::new_v4().simple());
        let now = intent_core::now_iso();
        a.services()
            .store()
            .insert_workspace(&intent_core::Workspace {
                id: intent_core::WorkspaceId(workspace.clone()),
                title: "Taken over".into(),
                branch: "main".into(),
                created_at: now.clone(),
                updated_at: now,
                ..Default::default()
            })
            .await
            .expect("seed the workspace");
        let mut ws = wss(a_listeners.ws_port(), &fingerprint, Some(&token))
            .await
            .unwrap();
        result(
            &ws_rpc(
                &mut ws,
                1,
                "note.create",
                json!({ "workspaceId": workspace, "title": "Written through A" }),
            )
            .await,
        );

        let refused = open_as(&pool, &tenant, dir.path(), &format!("b-{tenant}"))
            .await
            .err()
            .expect("B cannot claim a tenant A holds");
        assert!(
            intentd_orchestrator::held_elsewhere(&refused),
            "a standby retries this: {refused}"
        );

        a.fence().fire("test: A stops");
        let report = tokio::time::timeout(common::wait(), a.stopped())
            .await
            .expect("A stops");
        assert!(report.overran.is_empty(), "{:?}", report.overran);
        assert!(
            a.heartbeat().release().await.unwrap(),
            "A releases its lease"
        );
        ws_ends(&mut ws).await;

        let b = open_as(&pool, &tenant, dir.path(), &format!("b-{tenant}"))
            .await
            .expect("B claims the released tenant at once");
        let b_listeners = b.listen(&listen_as("b")).await.unwrap();
        assert_eq!(
            b_listeners.fingerprint(),
            Some(fingerprint.as_str()),
            "one TLS directory, one certificate"
        );
        let mut ws = wss(b_listeners.ws_port(), &fingerprint, Some(&token))
            .await
            .expect("A's owner token opens B");
        let notes = ws_rpc(&mut ws, 1, "note.list", json!({ "workspaceId": workspace })).await;
        assert!(
            names(result(&notes), "notes", "title").contains(&"Written through A".to_owned()),
            "{notes}"
        );

        b.fence().fire("test: done");
        ws_ends(&mut ws).await;
        a_listeners.stop().await;
        b_listeners.stop().await;
    }
}
