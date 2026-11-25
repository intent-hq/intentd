//! What the orchestrator's listeners give a tenant beyond the wire: the
//! `system.status` host and disk fields, the Assistant's `ws.rpc` loopback,
//! and invite links that name the orchestrator's public address.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{result, ws_rpc, wss, Uds};
use intent_app::tenant::ClientConnections;
use intent_core::agent_rpc::{AgentRpcCallSpec, AgentRpcRequest};
use intent_core::{AgentId, WorkspaceApi as _, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_transport::auth::TOKEN_ACCOUNT;
use intent_transport::PrimaryReverseRegistry;
use intentd_orchestrator::listen::{start_client_listeners, ClientListeners, ListenConfig};
use serde_json::{json, Value};

const PUBLIC_HOST: &str = "orchestrator.example.test";

fn config(dir: &Path, public_hosts: &[&str]) -> ListenConfig {
    ListenConfig {
        socket_path: dir.join("o.sock"),
        bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
        ws_port: 0,
        tls_dir: dir.join("tls"),
        insecure: false,
        public_hosts: public_hosts.iter().map(|h| (*h).to_owned()).collect(),
    }
}

async fn listening(dir: &Path, public_hosts: &[&str]) -> (Services, ClientListeners) {
    let store = intent_store::Store::open(&dir.join("db.sqlite"))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let reverse = Arc::new(PrimaryReverseRegistry::new());
    let services = Services::new_isolated(store)
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(reverse.clone())
        .with_workspaces_root(dir.join("workspaces"));
    let listeners = start_client_listeners(
        &config(dir, public_hosts),
        &services,
        &bus,
        reverse,
        &ClientConnections::default(),
    )
    .await
    .unwrap();
    (services, listeners)
}

async fn owner_wss(services: &Services, listeners: &ClientListeners) -> common::Wss {
    let token = services
        .secret_source()
        .load(TOKEN_ACCOUNT)
        .unwrap()
        .unwrap();
    let fingerprint = listeners.fingerprint().unwrap();
    let mut ws = wss(listeners.ws_port(), fingerprint, Some(&token))
        .await
        .unwrap();
    result(&ws_rpc(&mut ws, 1, "client.hello", json!({ "clientId": "web-x" })).await);
    ws
}

/// The status read reports the host's name and the volume the tenant's
/// workspaces live on, as `intentd`'s does.
#[tokio::test]
async fn system_status_reports_the_hostname_and_the_workspaces_disk() {
    let dir = tempfile::tempdir().unwrap();
    let (_services, listeners) = listening(dir.path(), &[]).await;
    let mut uds = Uds::connect(listeners.socket_path()).await;
    result(
        &uds.rpc(1, "client.hello", json!({ "clientId": "web-a" }))
            .await,
    );

    let deadline = tokio::time::Instant::now() + common::wait();
    let status = loop {
        let status = result(&uds.rpc(2, "system.status", json!({})).await).clone();
        if status.get("workspacesDiskTotalBytes").is_some() {
            break status;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no workspaces disk in {status}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(
        status["hostname"],
        json!(intent_transport::local_hostname()),
        "{status}"
    );
    assert!(status["localIps"].is_array(), "{status}");
    let total = status["workspacesDiskTotalBytes"].as_u64().unwrap();
    let available = status["workspacesDiskAvailableBytes"].as_u64().unwrap();
    assert!(total > 0 && available <= total, "{status}");
    listeners.stop().await;
}

/// The Assistant's `ws.rpc` runs a read on the tenant through a loopback
/// connection, with the orchestrator's status control.
#[tokio::test]
async fn the_assistant_ws_rpc_reads_the_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let (services, listeners) = listening(dir.path(), &[]).await;
    let call = |method: &str| AgentRpcRequest {
        calls: vec![AgentRpcCallSpec {
            method: method.to_owned(),
            params: json!({}),
            backend: None,
        }],
        batch: false,
        title: None,
    };
    let budget = std::time::Duration::from_secs(10);
    let status = services
        .agent_rpc_call(
            WorkspaceId::chief(),
            Some(AgentId::from("chief-agent")),
            call("system.status"),
            budget,
        )
        .await
        .expect("ws.rpc is attached");
    assert_eq!(status["ok"], json!(true), "{status}");
    assert_eq!(status["result"]["running"], json!(true), "{status}");
    assert_eq!(
        status["result"]["port"],
        json!(listeners.ws_port()),
        "the loopback reads the orchestrator's status: {status}"
    );
    let listed = services
        .agent_rpc_call(
            WorkspaceId::chief(),
            Some(AgentId::from("chief-agent")),
            call("workspace.list"),
            budget,
        )
        .await
        .unwrap();
    assert_eq!(listed["ok"], json!(true), "{listed}");
    assert!(listed["result"]["workspaces"].is_array(), "{listed}");

    listeners.stop().await;
}

fn query<'a>(url: &'a str, key: &str) -> Option<&'a str> {
    url.split_once('?')?
        .1
        .split('&')
        .find_map(|pair| pair.strip_prefix(key)?.strip_prefix('='))
}

/// An invite link names the orchestrator's public address (`host=`), its
/// WSS port and certificate, and `workspace.invite.list` rebuilds the same
/// link.
#[tokio::test]
async fn an_invite_link_names_the_public_address() {
    let dir = tempfile::tempdir().unwrap();
    let (services, listeners) = listening(dir.path(), &[PUBLIC_HOST]).await;
    let mut ws = owner_wss(&services, &listeners).await;
    let created = ws_rpc(&mut ws, 2, "workspace.create", json!({ "title": "Shared" })).await;
    let workspace = result(&created)["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let invite = ws_rpc(
        &mut ws,
        3,
        "workspace.invite.create",
        json!({ "workspaceId": workspace }),
    )
    .await;
    let invite = result(&invite);
    let url = invite["url"].as_str().expect("a link").to_owned();
    assert!(url.starts_with("intent://invite?"), "{url}");
    assert_eq!(query(&url, "host"), Some(PUBLIC_HOST), "{url}");
    assert_eq!(query(&url, "tc"), None, "no tunnel: {url}");
    assert_eq!(
        query(&url, "port"),
        Some(listeners.ws_port().to_string().as_str()),
        "{url}"
    );
    assert_eq!(invite["hosts"], json!([PUBLIC_HOST]), "{invite}");
    assert_eq!(invite["invite"]["url"], json!(url), "{invite}");

    let listed = ws_rpc(
        &mut ws,
        4,
        "workspace.invite.list",
        json!({ "workspaceId": workspace }),
    )
    .await;
    let rows: &Vec<Value> = result(&listed)["invites"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{listed}");
    let listed_url = rows[0]["url"].as_str().expect("the row carries its link");
    assert_eq!(
        listed_url.split("&secret=").next(),
        url.split("&secret=").next(),
        "the listed link is the minted one's envelope: {listed_url} vs {url}"
    );
    listeners.stop().await;
}

/// Without a public address (or a tunnel) an invite cannot be dialed, so
/// none is minted, as on `intentd` with its tunnel down.
#[tokio::test]
async fn without_a_public_address_no_invite_is_minted() {
    let dir = tempfile::tempdir().unwrap();
    let (services, listeners) = listening(dir.path(), &[]).await;
    let mut ws = owner_wss(&services, &listeners).await;
    let created = ws_rpc(&mut ws, 2, "workspace.create", json!({ "title": "Solo" })).await;
    let workspace = result(&created)["workspace"]["id"].clone();
    let refused = ws_rpc(
        &mut ws,
        3,
        "workspace.invite.create",
        json!({ "workspaceId": workspace }),
    )
    .await;
    assert_eq!(
        refused["error"]["data"]["code"],
        json!("tunnel-down"),
        "{refused}"
    );
    listeners.stop().await;
}

#[cfg(feature = "postgres")]
mod postgres {
    use intent_store::postgres::{self, PgPool};
    use intent_store::OrchestratorOnly;
    use intentd_orchestrator::serve::{serve_tenant, LeaseTiming};

    use super::*;

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

    /// A served Postgres tenant: its Assistant's `ws.rpc` reads the
    /// tenant's rows, and its status reports the tenant's workspaces disk.
    // Multi-thread: the tenant's secret store needs it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_postgres_tenant_answers_the_assistant_ws_rpc() {
        let Some(pool) = pool().await else { return };
        let dir = tempfile::tempdir().unwrap();
        let tenant = format!("rpc-{}", uuid::Uuid::new_v4().simple());
        let mut served = serve_tenant(
            pool.clone(),
            &tenant,
            dir.path(),
            &format!("orchestrator-test-{tenant}"),
            LeaseTiming::default(),
            Arc::new(intent_store::postgres::SecretKeyring::new("test", &[0x5a; 32]).unwrap()),
            OrchestratorOnly::for_tests(),
        )
        .await
        .expect("serve the tenant");
        let listeners = served.listen(&config(dir.path(), &[])).await.unwrap();
        let workspace = format!("ws-{}", uuid::Uuid::new_v4().simple());
        let now = intent_core::now_iso();
        served
            .services()
            .store()
            .insert_workspace(&intent_core::Workspace {
                id: WorkspaceId(workspace.clone()),
                title: "In Postgres".into(),
                branch: "main".into(),
                created_at: now.clone(),
                updated_at: now,
                ..Default::default()
            })
            .await
            .expect("seed the workspace");

        let listed = served
            .services()
            .agent_rpc_call(
                WorkspaceId::chief(),
                Some(AgentId::from("chief-agent")),
                AgentRpcRequest {
                    calls: vec![AgentRpcCallSpec {
                        method: "workspace.list".to_owned(),
                        params: json!({}),
                        backend: None,
                    }],
                    batch: false,
                    title: None,
                },
                std::time::Duration::from_secs(10),
            )
            .await
            .expect("ws.rpc is attached");
        assert_eq!(listed["ok"], json!(true), "{listed}");
        let ids: Vec<&str> = listed["result"]["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|w| w["id"].as_str())
            .collect();
        assert!(ids.contains(&workspace.as_str()), "{listed}");

        served.fence().fire("test: done");
        let _ = tokio::time::timeout(common::wait(), served.stopped()).await;
        listeners.stop().await;
    }
}
