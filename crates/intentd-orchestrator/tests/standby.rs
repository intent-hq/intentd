//! The orchestrator binary as a standby (`INTENTD_ORCHESTRATOR_STANDBY=1`):
//! while another instance holds the tenant it waits instead of exiting,
//! takes the tenant over once the owner releases it, and a stop signal ends
//! the wait at once. Without the variable, a held tenant still fails the
//! start.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(all(feature = "postgres", unix))]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use intent_store::postgres::{self, PgPool};
use intentd_test_support::GuardedChild;

/// What a standby logs each time it finds the tenant held.
const WAITING: &str = "standby: another instance holds the tenant";

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

/// One orchestrator process serving `tenant` as `instance`, its output in
/// `<root>/<instance>.log`.
struct Orchestrator {
    child: GuardedChild,
    log: PathBuf,
    _data: tempfile::TempDir,
}

impl Orchestrator {
    fn start(url: &str, tenant: &str, instance: &str, standby: bool, root: &Path) -> Self {
        let data = tempfile::tempdir().unwrap();
        let log = root.join(format!("{instance}.log"));
        let out = std::fs::File::create(&log).unwrap();
        let err = out.try_clone().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"));
        command
            .env("INTENTD_POSTGRES_URL", url)
            .env("INTENTD_POSTGRES_TENANTS", tenant)
            .env("INTENTD_ORCHESTRATOR_INSTANCE", instance)
            .env("INTENTD_DATA_DIR", data.path())
            .env("RUST_LOG", "info")
            .env_remove("INTENTD_ORCHESTRATOR_STANDBY")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        if standby {
            command.env("INTENTD_ORCHESTRATOR_STANDBY", "1");
        }
        // Killed with its process group if the test fails before it exits.
        let child = GuardedChild::spawn(&mut command).expect("start the orchestrator");
        Self {
            child,
            log,
            _data: data,
        }
    }

    fn output(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Wait until the process logged `line`.
    async fn logged(&self, line: &str) {
        let found = tokio::time::timeout(wait(), async {
            while !self.output().contains(line) {
                // timing-guard: poll interval while waiting for a log line
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(
            found.is_ok(),
            "never logged {line:?}; output:\n{}",
            self.output()
        );
    }

    /// The exit code once the process exits on its own.
    #[track_caller]
    fn exit_code(&mut self) -> Option<i32> {
        let status = self
            .child
            .wait_with_timeout(wait())
            .expect("wait for the orchestrator");
        let status = status.unwrap_or_else(|| panic!("never exited; output:\n{}", self.output()));
        status.code()
    }

    /// SIGTERM, then the exit code.
    #[track_caller]
    fn terminate(&mut self) -> Option<i32> {
        let sent = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("send SIGTERM");
        assert!(sent.success());
        self.exit_code()
    }
}

/// Wait until `instance` holds `tenant`'s live lease; its epoch.
async fn held_by(pool: &PgPool, tenant: &str, instance: &str) -> i64 {
    let held = tokio::time::timeout(wait(), async {
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
    .await;
    held.unwrap_or_else(|_| panic!("{instance} never claimed {tenant}"))
}

#[tokio::test]
async fn a_standby_waits_for_a_held_tenant_and_takes_it_over_on_release() {
    let Some(url) = url() else { return };
    let pool = postgres::connect(&url, 4).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    let run = uuid::Uuid::new_v4().simple().to_string();
    let tenant = format!("standby-{run}");
    let logs = tempfile::tempdir().unwrap();
    let (a, b, c, d) = (
        format!("a-{run}"),
        format!("b-{run}"),
        format!("c-{run}"),
        format!("d-{run}"),
    );

    let mut owner = Orchestrator::start(&url, &tenant, &a, false, logs.path());
    let epoch = held_by(&pool, &tenant, &a).await;

    // Without standby, a held tenant still fails the start.
    let mut eager = Orchestrator::start(&url, &tenant, &c, false, logs.path());
    assert_eq!(eager.exit_code(), Some(1), "{}", eager.output());

    // A standby waits; it takes over as soon as the owner releases.
    let mut standby = Orchestrator::start(&url, &tenant, &b, true, logs.path());
    standby.logged(WAITING).await;
    assert_eq!(
        lease(&pool, &tenant).await.map(|row| row.0),
        Some(a.clone())
    );
    assert_eq!(owner.terminate(), Some(0), "{}", owner.output());
    let next = held_by(&pool, &tenant, &b).await;
    assert_eq!(next, epoch + 1, "the standby claims the next epoch");

    // A standby still waiting stops on a signal without touching the lease.
    let mut waiting = Orchestrator::start(&url, &tenant, &d, true, logs.path());
    waiting.logged(WAITING).await;
    assert_eq!(waiting.terminate(), Some(0), "{}", waiting.output());
    assert_eq!(lease(&pool, &tenant).await, Some((b.clone(), next, false)));

    // The new owner stops cleanly and releases in turn.
    assert_eq!(standby.terminate(), Some(0), "{}", standby.output());
    assert_eq!(lease(&pool, &tenant).await, Some((b, next, true)));
}
