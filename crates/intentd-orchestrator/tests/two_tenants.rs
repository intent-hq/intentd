//! Two real Postgres tenants in one orchestrator process: when A's lease
//! renewals are blocked, A's fence fires and A's work stops, while B keeps its
//! lease, its loops and its writes.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::time::Duration;

use intent_app::tenant;
use intent_core::tenant_fence::FenceStep;
use intent_store::postgres::{self, PgPool};
use intent_store::OrchestratorOnly;
use intentd_orchestrator::serve::{serve_tenant, LeaseTiming};

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

/// The tenant secret keyring; the tests' tenants hold no secrets.
fn keyring() -> std::sync::Arc<postgres::SecretKeyring> {
    std::sync::Arc::new(postgres::SecretKeyring::new("test", &[0x5a; 32]).unwrap())
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(30))
}

async fn settled() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_whose_renewals_stop_is_fenced_alone() {
    let Some(pool) = pool().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let run = uuid_suffix();
    let instance = format!("orchestrator-test-{run}");
    let a_tenant = format!("a-{run}");
    // A short lease for A, so its fence fires soon once renewals stop.
    let short = LeaseTiming {
        ttl: Duration::from_secs(4),
        margin: Duration::from_millis(500),
    };
    let mut a = serve_tenant(
        pool.clone(),
        &a_tenant,
        dir.path(),
        &instance,
        short,
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve A");
    let b = serve_tenant(
        pool.clone(),
        &format!("b-{run}"),
        dir.path(),
        &instance,
        LeaseTiming::default(),
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve B");
    assert_ne!(a.dir(), b.dir());
    assert!(a.dir().starts_with(dir.path().join("tenants")));
    settled().await;
    assert_eq!(b.runtime().loops_running(), tenant::TENANT_LOOPS);
    assert!(
        !a.fence().is_fenced(),
        "A's heartbeat renews while nothing blocks it"
    );

    // Block A's renewals only: hold A's lease row lock in an open
    // transaction, so every renewal waits until the heartbeat gives up.
    let mut blocker = pool.begin().await.expect("begin");
    sqlx::query(
        "SELECT 1 FROM lease WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = '' \
         FOR UPDATE",
    )
    .bind(&a_tenant)
    .execute(&mut *blocker)
    .await
    .expect("lock A's lease row");

    let report = tokio::time::timeout(wait(), a.stopped())
        .await
        .expect("A's fence fires once its renewals stop");

    assert!(report.overran.is_empty(), "{:?}", report.overran);
    assert!(report.finished(FenceStep::WritesSettled).is_some());
    assert!(!intentd_orchestrator::is_fatal(&report));
    assert!(a.fence().is_fenced());
    assert_eq!(a.runtime().tasks_running(), 0, "A's work stopped");
    assert!(a.runtime().connections().is_closed());
    let refused = a
        .services()
        .store()
        .set_setting("orchestrator.test", "after the fence")
        .await
        .expect_err("a fenced tenant writes nothing");
    assert!(
        matches!(refused, intent_core::Error::NotOwner(_)),
        "{refused:?}"
    );

    assert!(!b.fence().is_fenced(), "B keeps its lease");
    assert!(b.lease().lost_reason().is_none(), "B's keeper still renews");
    assert_eq!(
        b.runtime().loops_running(),
        tenant::TENANT_LOOPS,
        "B's loops keep running"
    );
    assert!(!b.runtime().connections().is_closed());
    b.services()
        .store()
        .set_setting("orchestrator.test", "still served")
        .await
        .expect("B still writes");
    blocker.rollback().await.expect("release A's lease row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_another_instance_holds_is_refused_before_any_write() {
    let Some(pool) = pool().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let run = uuid_suffix();
    let tenant = format!("held-{run}");
    let _held = serve_tenant(
        pool.clone(),
        &tenant,
        dir.path(),
        &format!("first-{run}"),
        LeaseTiming::default(),
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("the first instance serves the tenant");
    let other = tempfile::tempdir().unwrap();
    let refused = serve_tenant(
        pool,
        &tenant,
        other.path(),
        &format!("second-{run}"),
        LeaseTiming::default(),
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await;
    assert!(
        matches!(refused, Err(intent_core::Error::NotOwner(_))),
        "a live lease refuses the second instance"
    );
    assert!(
        !intentd_orchestrator::tenant_dir(other.path(), &tenant).exists(),
        "nothing was set up for a tenant this process does not own"
    );
}

fn uuid_suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_owned()
}
