//! The orchestrator binary started while another instance still holds the
//! tenant's lease: it waits out the lease (bounded by
//! `INTENTD_ORCHESTRATOR_CLAIM_WAIT_SECS`) and then claims, so a restart
//! right after a crash serves without an external supervisor's retry. While
//! the holder keeps renewing it exits 1 at the bound; SIGTERM while waiting
//! exits 0 and leaves the lease alone.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(all(feature = "postgres", unix))]

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use intent_store::postgres::{
    self, LeaseHeartbeat, PgPool, PostgresOptions, TenantId, LEASE_RENEW_INTERVAL, LEASE_TTL,
};
use intent_store::{OrchestratorOnly, Store};
use intentd_orchestrator::CLAIM_WAIT_ENV;
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
    // The lease TTL plus a margin for a loaded runner.
    intentd_test_support::test_timeout(LEASE_TTL + Duration::from_secs(60))
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

/// Start the orchestrator binary serving `tenant` as `instance`, its output
/// in `log`.
fn start(
    url: &str,
    tenant: &str,
    instance: &str,
    dir: &Path,
    log: &Path,
    extra: &[(&str, &str)],
) -> GuardedChild {
    let out = std::fs::File::create(log).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"));
    command
        .env("INTENTD_POSTGRES_URL", url)
        .env("INTENTD_POSTGRES_TENANTS", tenant)
        .env("INTENTD_ORCHESTRATOR_INSTANCE", instance)
        .env("INTENTD_DATA_DIR", dir)
        .env("RUST_LOG", "info")
        .env(
            "INTENTD_SECRETS_KEY",
            "WlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlpaWlo=",
        )
        .env("INTENTD_SECRETS_KEY_ID", "test")
        .stdin(Stdio::null())
        .stdout(out.try_clone().unwrap())
        .stderr(out);
    for (name, value) in extra {
        command.env(name, value);
    }
    GuardedChild::spawn(&mut command).expect("start the orchestrator")
}

/// Wait until `instance` holds `tenant`'s lease; its epoch.
async fn claimed_by(pool: &PgPool, tenant: &str, instance: &str) -> i64 {
    tokio::time::timeout(wait(), async {
        loop {
            if let Some((owner, epoch, false)) = lease(pool, tenant).await {
                if owner == instance {
                    return epoch;
                }
            }
            // timing-guard: poll interval while waiting for a claim
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{instance} never claimed {tenant}"))
}

/// Wait until the binary logged that it waits for the lease.
async fn waiting(log: &Path) {
    tokio::time::timeout(wait(), async {
        loop {
            if std::fs::read_to_string(log)
                .unwrap_or_default()
                .contains("waiting for the tenant lease")
            {
                return;
            }
            // timing-guard: poll interval while waiting for a log line
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the orchestrator never waited: {}",
            std::fs::read_to_string(log).unwrap_or_default()
        )
    });
}

/// Another instance claims `tenant` and keeps renewing until the returned
/// heartbeat is dropped.
async fn hold(pool: &PgPool, tenant: &str, instance: &str) -> (Store, LeaseHeartbeat, i64) {
    let (store, claim) = Store::open_postgres_owned(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions::default(),
        instance,
        LEASE_TTL,
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("the holder claims the tenant");
    let heartbeat = LeaseHeartbeat::spawn(store.clone(), &claim, LEASE_TTL, LEASE_RENEW_INTERVAL);
    (store, heartbeat, claim.epoch)
}

fn sigterm(child: &GuardedChild) {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success());
}

fn names(run: &str) -> (String, String, String) {
    (
        format!("wait-{run}"),
        format!("a-{run}"),
        format!("b-{run}"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_after_a_crash_waits_out_the_lease_then_claims() {
    let Some(url) = url() else { return };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    let run = uuid::Uuid::new_v4().simple().to_string();
    let (tenant, a, b) = names(&run);
    let dir = tempfile::tempdir().unwrap();

    let mut first = start(
        &url,
        &tenant,
        &a,
        &dir.path().join("a"),
        &dir.path().join("a.log"),
        &[],
    );
    let epoch = claimed_by(&pool, &tenant, &a).await;
    first.kill_tree().expect("SIGKILL the first instance");
    let killed = Instant::now();

    let mut second = start(
        &url,
        &tenant,
        &b,
        &dir.path().join("b"),
        &dir.path().join("b.log"),
        &[],
    );
    assert_eq!(claimed_by(&pool, &tenant, &b).await, epoch + 1);
    // It claimed only once the dead instance's lease lapsed.
    assert!(
        killed.elapsed() >= LEASE_TTL.saturating_sub(LEASE_RENEW_INTERVAL),
        "claimed after {:?}, before the lease could expire",
        killed.elapsed()
    );
    let log = std::fs::read_to_string(dir.path().join("b.log")).unwrap();
    assert!(log.contains("waiting for the tenant lease"), "{log}");
    assert!(second.try_wait().unwrap().is_none(), "{log}");
    sigterm(&second);
    let exited = second.wait_with_timeout(wait()).unwrap().unwrap();
    assert_eq!(exited.code(), Some(0), "{exited:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn while_the_holder_renews_it_exits_1_after_the_bound() {
    let Some(url) = url() else { return };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    let run = uuid::Uuid::new_v4().simple().to_string();
    let (tenant, a, b) = names(&run);
    let dir = tempfile::tempdir().unwrap();
    let (_store, _heartbeat, epoch) = hold(&pool, &tenant, &a).await;

    let started = Instant::now();
    let mut waiter = start(
        &url,
        &tenant,
        &b,
        dir.path(),
        &dir.path().join("b.log"),
        &[(CLAIM_WAIT_ENV, "3")],
    );
    let exited = waiter.wait_with_timeout(wait()).unwrap().unwrap();
    let log = std::fs::read_to_string(dir.path().join("b.log")).unwrap();
    assert_eq!(exited.code(), Some(1), "{log}");
    assert!(
        started.elapsed() >= Duration::from_secs(3),
        "exited after {:?}, before the bound: {log}",
        started.elapsed()
    );
    assert!(log.contains("waiting for the tenant lease"), "{log}");
    assert_eq!(lease(&pool, &tenant).await, Some((a, epoch, false)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_while_waiting_exits_0_and_leaves_the_lease() {
    let Some(url) = url() else { return };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    let run = uuid::Uuid::new_v4().simple().to_string();
    let (tenant, a, b) = names(&run);
    let dir = tempfile::tempdir().unwrap();
    let (_store, _heartbeat, epoch) = hold(&pool, &tenant, &a).await;

    let log = dir.path().join("b.log");
    let mut waiter = start(&url, &tenant, &b, dir.path(), &log, &[]);
    waiting(&log).await;
    sigterm(&waiter);
    let exited = waiter
        .wait_with_timeout(intentd_test_support::test_timeout(Duration::from_secs(20)))
        .unwrap()
        .expect("a waiting orchestrator stops on SIGTERM");
    let text = std::fs::read_to_string(&log).unwrap();
    assert_eq!(exited.code(), Some(0), "{text}");
    assert_eq!(lease(&pool, &tenant).await, Some((a, epoch, false)));
}
