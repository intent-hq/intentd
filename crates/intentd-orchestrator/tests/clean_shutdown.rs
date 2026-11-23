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

/// The tenant's lease row: owner, epoch, and whether it has expired.
async fn lease(pool: &PgPool, tenant: &str) -> Option<(String, i64, bool)> {
    sqlx::query_as(
        "SELECT owner_instance, epoch, expires_at <= clock_timestamp() FROM tenant_lease \
         WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_optional(pool)
    .await
    .expect("read tenant_lease")
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

    let mut child = Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"))
        .env("INTENTD_POSTGRES_URL", &url)
        .env("INTENTD_POSTGRES_TENANTS", &tenant)
        .env("INTENTD_ORCHESTRATOR_INSTANCE", &instance)
        .env("INTENTD_DATA_DIR", dir.path())
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .spawn()
        .expect("start the orchestrator");

    let claimed = tokio::time::timeout(wait(), async {
        loop {
            if let Some(row) = lease(&pool, &tenant).await {
                if row.0 == instance {
                    return row;
                }
            }
            // timing-guard: poll interval while waiting for the orchestrator to claim the tenant
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let Ok((_, epoch, expired)) = claimed else {
        let _ = child.kill();
        panic!("the orchestrator never claimed {tenant}");
    };
    assert!(!expired, "a fresh claim is live");

    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
    let exited = tokio::time::timeout(
        wait(),
        tokio::task::spawn_blocking(move || child.wait().expect("wait for the orchestrator")),
    )
    .await
    .expect("the orchestrator exits on SIGTERM")
    .unwrap();
    assert_eq!(exited.code(), Some(0), "a clean stop exits 0: {exited:?}");

    // Released: expired now, long before the 30 s TTL, still naming the old
    // owner until someone claims.
    assert_eq!(lease(&pool, &tenant).await, Some((instance, epoch, true)));
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
    standby
        .set_setting("userRules", "\"standby\"")
        .await
        .unwrap();
}
