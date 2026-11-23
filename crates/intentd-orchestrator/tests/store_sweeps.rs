//! A served Postgres tenant sweeps its own store: the tenant runtime's event
//! retention and idempotency reaper loops remove the tenant's old ephemeral
//! events and expired idempotency keys, keep its recent rows and lifecycle
//! events, never touch another tenant's rows, and stop with the tenant's
//! fence.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::time::Duration;

use intent_app::tenant;
use intent_services::retention::TenantSweepTiming;
use intent_store::postgres::{self, PgPool, WriterLimits};
use intent_store::OrchestratorOnly;
use intentd_orchestrator::serve::{serve_tenant_with, LeaseTiming};

const OLD: &str = "2020-01-01T00:00:00.000Z";

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

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(30))
}

/// Sweeps every 100 ms, the first retention tick at once.
fn fast() -> TenantSweepTiming {
    TenantSweepTiming {
        retention_start_delay: Duration::ZERO,
        retention_interval: Some(Duration::from_millis(100)),
        reap_interval: Duration::from_millis(100),
    }
}

/// Insert `tenant`'s rows as another session would, holding the tenant's
/// writer lock (the `row_seq` trigger needs it). The rows bypass the
/// tenant's store, so they land even after its fence.
async fn seed(pool: &PgPool, tenant: &str, prefix: &str) {
    let now = intent_core::now_iso();
    let mut tx = pool.begin().await.expect("begin");
    let long = Duration::from_secs(60);
    let limits = WriterLimits {
        wait_budget: long,
        lock_attempt: long,
        statement_timeout: long,
        idle_in_transaction_timeout: long,
    };
    postgres::lock_tenant_writer(&mut tx, tenant, long, &limits)
        .await
        .expect("take the writer lock");
    let sql = format!(
        "INSERT INTO event (tenant_id, id, workspace_id, timestamp, event_type, actor) VALUES \
           ('{tenant}', '{prefix}-old-stream', 'ws', '{OLD}', 'agent:stream:delta', '{{}}'), \
           ('{tenant}', '{prefix}-new-stream', 'ws', '{now}', 'agent:stream:delta', '{{}}'), \
           ('{tenant}', '{prefix}-old-lifecycle', 'ws', '{OLD}', 'workspace:created', '{{}}'); \
         INSERT INTO idempotency_key (tenant_id, workspace_id, idempotency_key, method, \
           result_json, created_at) VALUES \
           ('{tenant}', '', '{prefix}-old-key', 'm', '1', '{OLD}'), \
           ('{tenant}', '', '{prefix}-new-key', 'm', '1', '{now}');"
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(&mut *tx)
        .await
        .expect("seed rows");
    tx.commit().await.expect("commit seed");
}

/// `tenant`'s seeded rows that are still there, sorted.
async fn left(pool: &PgPool, tenant: &str) -> Vec<String> {
    let mut rows: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM event WHERE tenant_id = $1 \
         UNION ALL SELECT idempotency_key FROM idempotency_key WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_all(pool)
    .await
    .expect("read rows");
    rows.sort();
    rows
}

fn kept(prefix: &str) -> Vec<String> {
    let mut rows: Vec<String> = ["new-key", "new-stream", "old-lifecycle"]
        .iter()
        .map(|r| format!("{prefix}-{r}"))
        .collect();
    rows.sort();
    rows
}

fn all(prefix: &str) -> Vec<String> {
    let mut rows = kept(prefix);
    rows.extend(
        ["old-key", "old-stream"]
            .iter()
            .map(|r| format!("{prefix}-{r}")),
    );
    rows.sort();
    rows
}

async fn until_swept(pool: &PgPool, tenant: &str, prefix: &str) {
    tokio::time::timeout(wait(), async {
        while left(pool, tenant).await != kept(prefix) {
            // timing-guard: poll until the tenant's sweep loops have ticked
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the tenant runtime swept {tenant}'s expired rows"));
}

#[tokio::test]
async fn a_served_tenant_sweeps_only_its_own_expired_rows_until_its_fence() {
    let Some(pool) = pool().await else { return };
    let dir = tempfile::tempdir().unwrap();
    let run = uuid::Uuid::new_v4().simple().to_string()[..12].to_owned();
    let subject = format!("sweep-{run}");
    // Another tenant of the same database that no runtime serves here: its
    // rows use the same keys and must survive the subject's sweeps.
    let other = format!("other-{run}");
    seed(&pool, &other, "r").await;

    let mut served = serve_tenant_with(
        pool.clone(),
        &subject,
        dir.path(),
        &format!("orchestrator-test-{run}"),
        LeaseTiming::default(),
        fast(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve the tenant");
    assert_eq!(served.runtime().loops_running(), tenant::TENANT_LOOPS);
    seed(&pool, &subject, "r").await;

    until_swept(&pool, &subject, "r").await;
    assert_eq!(
        left(&pool, &other).await,
        all("r"),
        "the other tenant keeps every row"
    );

    assert!(served.fence().fire("test: lease lost"));
    let report = tokio::time::timeout(wait(), served.stopped())
        .await
        .expect("the fence finishes");
    assert!(report.overran.is_empty(), "{:?}", report.overran);
    assert_eq!(
        served.runtime().loops_running(),
        0,
        "the sweep loops stopped"
    );

    // Expired rows that land after the fence stay: nothing sweeps them.
    seed(&pool, &subject, "late").await;
    // timing-guard: give a still-running loop many intervals to (wrongly) sweep
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut expected = kept("r");
    expected.extend(all("late"));
    expected.sort();
    assert_eq!(left(&pool, &subject).await, expected);
    assert_eq!(left(&pool, &other).await, all("r"));
}
