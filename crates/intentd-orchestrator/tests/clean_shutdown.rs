//! The orchestrator binary on SIGTERM: it stops the tenant, releases its
//! lease and exits 0, so a standby claims the tenant at once with the next
//! epoch instead of waiting out the 30 s TTL.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(all(feature = "postgres", unix))]

use std::process::{Command, Stdio};
use std::time::Duration;

use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId, LEASE_TTL};
use intent_store::{OrchestratorOnly, Store};
use intentd_test_support::GuardedChild;

#[track_caller]
fn url() -> Option<String> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    Some(url)
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(60))
}

/// The tenant's lease row: owner (`None` once released), epoch, and
/// whether it has expired.
async fn lease(pool: &PgPool, tenant: &str) -> Option<(Option<String>, i64, bool)> {
    sqlx::query_as(
        "SELECT holder, token, expires_at <= clock_timestamp() FROM lease \
         WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''",
    )
    .bind(tenant)
    .fetch_optional(pool)
    .await
    .expect("read the tenant lease")
}

#[tokio::test]
async fn sigterm_releases_the_lease_so_a_standby_claims_at_once() {
    let Some(url) = url() else { return };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    let run = uuid::Uuid::new_v4().simple().to_string();
    let tenant = format!("shutdown-{run}");
    let instance = format!("orchestrator-{run}");
    let dir = tempfile::tempdir().unwrap();

    // Killed with its process group if the test fails before it exits.
    let mut child = GuardedChild::spawn(
        Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"))
            .env("INTENTD_POSTGRES_URL", &url)
            .env("INTENTD_POSTGRES_TENANTS", &tenant)
            .env("INTENTD_ORCHESTRATOR_INSTANCE", &instance)
            .env("INTENTD_DATA_DIR", dir.path())
            .env("RUST_LOG", "info")
            // The orchestrator refuses to start without a tenant secret key.
            .env(
                "INTENTD_SECRETS_KEY",
                "WlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlo=",
            )
            .env("INTENTD_SECRETS_KEY_ID", "test")
            .stdin(Stdio::null()),
    )
    .expect("start the orchestrator");
    let pid = child.id();

    let claimed = tokio::time::timeout(wait(), async {
        loop {
            if let Some(row) = lease(&pool, &tenant).await {
                if row.0.as_deref() == Some(instance.as_str()) {
                    return row;
                }
            }
            // timing-guard: poll interval while waiting for the orchestrator to claim the tenant
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let (_, epoch, expired) =
        claimed.unwrap_or_else(|_| panic!("the orchestrator never claimed {tenant}"));
    assert!(!expired, "a fresh claim is live");

    let status = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let exited = child
        .wait_with_timeout(wait())
        .expect("wait for the orchestrator")
        .expect("the orchestrator exits on SIGTERM");
    assert_eq!(exited.code(), Some(0), "a clean stop exits 0: {exited:?}");

    // Released: free and expired now, long before the 30 s TTL, keeping its
    // token so the next claim's epoch is higher.
    assert_eq!(lease(&pool, &tenant).await, Some((None, epoch, true)));
    // Closed before the release: it refuses every write until a start.
    let state: String = sqlx::query_scalar("SELECT state FROM tenant_writer WHERE tenant_id = $1")
        .bind(&tenant)
        .fetch_one(&pool)
        .await
        .expect("read the tenant state");
    assert_eq!(state, "closed", "a clean stop leaves the tenant closed");
    let (standby, next) = Store::open_postgres_owned(
        pool.clone(),
        TenantId::new(&tenant).unwrap(),
        PostgresOptions::default(),
        &format!("standby-{run}"),
        LEASE_TTL,
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("the standby claims the released tenant at once");
    assert_eq!(next.epoch, epoch + 1);
    assert_eq!(
        standby.tenant_state().await.unwrap(),
        intent_store::TenantState::Open,
        "the standby's start opens the tenant"
    );
    standby
        .set_setting("userRules", "\"standby\"")
        .await
        .unwrap();
}
