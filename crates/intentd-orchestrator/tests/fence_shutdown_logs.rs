//! A clean stop fires the tenant's fence first, then closes its client
//! connections. A connection's `client:disconnected` (and any other write
//! after the fire) is refused by the fence, which is expected: the fire was
//! already logged at WARN with its reason. Those refusals must not be logged
//! as errors or warnings of their own.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(all(feature = "postgres", unix))]

mod common;

use std::sync::{Arc, Mutex};

use common::{result, until, ws_rpc, wss};
use intent_store::postgres::{self, PgPool};
use intent_store::OrchestratorOnly;
use intent_transport::auth::TOKEN_ACCOUNT;
use intentd_orchestrator::listen::ListenConfig;
use intentd_orchestrator::serve::{serve_tenant, LeaseTiming};
use serde_json::json;
use tracing_subscriber::layer::SubscriberExt as _;

/// Every `intent*` event at DEBUG or louder, as `(level, rendered fields)`.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<(tracing::Level, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Captured {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Fields(String);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write as _;
                let _ = write!(self.0, "{}={value:?} ", field.name());
            }
        }
        let meta = event.metadata();
        if *meta.level() > tracing::Level::DEBUG || !meta.target().starts_with("intent") {
            return;
        }
        let mut fields = Fields(format!("{}: ", meta.target()));
        event.record(&mut fields);
        self.0.lock().unwrap().push((*meta.level(), fields.0));
    }
}

impl Captured {
    fn lines(&self) -> Vec<(tracing::Level, String)> {
        self.0.lock().unwrap().clone()
    }
}

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

/// The orchestrator's own stop reason (`main.rs`).
const STOPPING: &str = "the orchestrator is stopping";

// Multi-thread: the tenant's secret store needs it. One test in this binary:
// it owns the process-global subscriber.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clean_stop_logs_no_error_for_the_fences_own_refusals() {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(captured.clone()))
        .expect("the only subscriber of this test binary");
    let Some(pool) = pool().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let tenant = format!("fence-logs-{}", uuid::Uuid::new_v4().simple());
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
    let listeners = served
        .listen(&ListenConfig {
            socket_path: dir.path().join("o.sock"),
            bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
            ws_port: 0,
            tls_dir: dir.path().join("tls"),
            insecure: false,
            public_hosts: Vec::new(),
        })
        .await
        .unwrap();
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
    result(&ws_rpc(&mut ws, 1, "client.hello", json!({ "clientId": "web-f7" })).await);
    until("the connection to join the tenant", || {
        served.runtime().connections().len() == 1
    })
    .await;
    assert!(served.fence().fire(STOPPING));
    let report = tokio::time::timeout(common::wait(), served.stopped())
        .await
        .expect("the fence stops the tenant");
    assert!(report.overran.is_empty(), "{:?}", report.overran);
    // The closed connection's departure meets the fence: wait for its
    // refusal to be logged (at whatever level), then judge the levels.
    until("the refused client:disconnected to be logged", || {
        captured
            .lines()
            .iter()
            .any(|(_, line)| line.contains("client:disconnected") && line.contains("fenced"))
    })
    .await;
    listeners.stop().await;

    let lines = captured.lines();
    let fence_refusals: Vec<_> = lines
        .iter()
        .filter(|(level, line)| *level <= tracing::Level::WARN && line.contains("fenced itself"))
        .collect();
    assert!(
        fence_refusals.is_empty(),
        "the fence's own refusals after a clean stop are not errors or warnings: \
         {fence_refusals:#?}"
    );
    assert!(
        lines
            .iter()
            .any(|(level, line)| *level == tracing::Level::WARN
                && line.contains("tenant fence fired")
                && line.contains(STOPPING)),
        "the fire itself stays a WARN with its reason: {lines:#?}"
    );
}
