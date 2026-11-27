//! The tenant state across an orchestrator stop and start: a graceful stop
//! closes the tenant before its work stops (`closing`: every write is
//! refused) and marks it `closed` once its work stopped, before releasing
//! its lease; a start opens it again, also after a stop that crashed while
//! it was closing.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::time::Duration;

use intent_core::Error;
use intent_store::postgres::{self, PgPool, TenantId};
use intent_store::{OrchestratorOnly, Store, TenantState};
use intentd_orchestrator::serve::{serve_tenant, LeaseTiming, ServedTenant};

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

fn keyring() -> std::sync::Arc<postgres::SecretKeyring> {
    std::sync::Arc::new(postgres::SecretKeyring::new("test", &[0x5a; 32]).unwrap())
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(60))
}

fn run_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

async fn serve(pool: &PgPool, tenant: &str, dir: &std::path::Path, run: &str) -> ServedTenant {
    tokio::time::timeout(
        wait(),
        serve_tenant(
            pool.clone(),
            tenant,
            dir,
            &format!("orchestrator-{run}"),
            LeaseTiming::default(),
            keyring(),
            OrchestratorOnly::for_tests(),
        ),
    )
    .await
    .expect("serving starts")
    .expect("serve the tenant")
}

async fn state(pool: &PgPool, tenant: &str) -> String {
    sqlx::query_scalar("SELECT state FROM tenant_writer WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(pool)
        .await
        .expect("read the tenant state")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_graceful_stop_closes_the_tenant_before_its_work_stops_and_marks_it_closed() {
    let Some(pool) = pool().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let run = run_id();
    let tenant = format!("state-stop-{run}");
    let mut served = serve(&pool, &tenant, dir.path(), &run).await;
    let store = served.services().store().clone();
    store.set_setting("userRules", "\"serving\"").await.unwrap();

    // The order of the orchestrator's stop (`stop_all`).
    tokio::time::timeout(wait(), served.drain(None))
        .await
        .expect("the drain ends");
    assert_eq!(
        state(&pool, &tenant).await,
        "open",
        "the drain's writes land"
    );
    tokio::time::timeout(wait(), served.close())
        .await
        .expect("the close ends");
    assert_eq!(state(&pool, &tenant).await, "closing");
    assert!(
        matches!(
            store.set_setting("userRules", "\"late\"").await,
            Err(Error::TenantClosed(_))
        ),
        "a closing tenant refuses writes before its fence fired"
    );
    assert!(served.fence().fire("stopping"));
    tokio::time::timeout(wait(), served.stopped())
        .await
        .expect("the tenant's work stops");
    let closed = served.mark_closed().await.expect("mark closed");
    assert_eq!(
        (closed.from, closed.state),
        (TenantState::Closing, TenantState::Closed)
    );
    assert!(served.lease().release().await.expect("release"));
    assert_eq!(state(&pool, &tenant).await, "closed");

    // The next start opens it.
    let next = serve(&pool, &tenant, dir.path(), &run_id()).await;
    assert_eq!(state(&pool, &tenant).await, "open");
    next.services()
        .store()
        .set_setting("userRules", "\"next\"")
        .await
        .unwrap();
    assert!(next.fence().fire("test over"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_opens_a_tenant_a_crashed_stop_left_closing() {
    let Some(pool) = pool().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let run = run_id();
    let tenant = format!("state-crash-{run}");
    let closing = Store::postgres_move_tenant_state(
        pool.clone(),
        TenantId::new(&tenant).unwrap(),
        TenantState::Closing,
    )
    .await
    .expect("close the tenant");
    assert_eq!(closing.state, TenantState::Closing);
    let served = serve(&pool, &tenant, dir.path(), &run).await;
    assert_eq!(state(&pool, &tenant).await, "open");
    served
        .services()
        .store()
        .set_setting("userRules", "\"serving\"")
        .await
        .unwrap();
    assert!(served.fence().fire("test over"));
}
