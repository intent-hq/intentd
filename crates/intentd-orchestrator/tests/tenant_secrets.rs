//! Tenant secrets through the orchestrator's `Services`: they live in the
//! tenant's Postgres rows, encrypted (the GitLab credential pair included),
//! so the instance that takes a tenant over reads what the previous one
//! wrote; a fenced instance cannot write; no `secrets.json` is written; a
//! wrong key fails closed.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use intent_core::WorkspaceApi as _;
use intent_services::SecretStore as _;
use intent_store::postgres::{self, PgPool, PostgresOptions, SecretKeyring, TenantId};
use intent_store::{OrchestratorOnly, Store};
use intentd_orchestrator::secrets::TenantSecrets;
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

/// `settings.update` of one sensitive setting, as the daemon itself.
async fn save(services: &intent_services::Services, path: &str, value: &str) {
    intent_core::with_caller(
        intent_core::Caller::Daemon,
        services.settings_update(serde_json::json!([{ "path": path, "value": value }])),
    )
    .await
    .expect("settings.update");
}

fn keyring(key: u8) -> Arc<SecretKeyring> {
    Arc::new(SecretKeyring::new("k1", &[key; 32]).unwrap())
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(60))
}

fn run_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_owned()
}

/// A short lease, so a takeover does not wait long once renewals stop.
fn short() -> LeaseTiming {
    LeaseTiming {
        ttl: Duration::from_secs(3),
        interval: Duration::from_millis(100),
    }
}

/// Serve `tenant` as `instance`, retrying while another instance's lease
/// is still live.
async fn serve_when_free(
    pool: &PgPool,
    tenant: &str,
    data_dir: &std::path::Path,
    instance: &str,
    keys: Arc<SecretKeyring>,
) -> ServedTenant {
    tokio::time::timeout(wait(), async {
        loop {
            match serve_tenant(
                pool.clone(),
                tenant,
                data_dir,
                instance,
                LeaseTiming::default(),
                keys.clone(),
                OrchestratorOnly::for_tests(),
            )
            .await
            {
                Ok(served) => return served,
                Err(intent_core::Error::NotOwner(_)) => {
                    // timing-guard: poll interval while the previous instance's lease runs out
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(other) => panic!("serve {tenant}: {other:?}"),
            }
        }
    })
    .await
    .expect("the lease frees up")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secrets_follow_the_tenant_to_the_instance_that_takes_it_over() {
    let Some(pool) = pool().await else { return };
    let run = run_id();
    let tenant = format!("secrets-{run}");
    let keys = keyring(0x11);
    let first_dir = tempfile::tempdir().unwrap();
    let mut first = serve_tenant(
        pool.clone(),
        &tenant,
        first_dir.path(),
        &format!("first-{run}"),
        short(),
        keys.clone(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve the tenant");

    // Save the token as a client would (`settings.update`), through the
    // tenant's `Services`, and read it back through them.
    save(
        first.services(),
        "sourceControl.github.token",
        "gho_follows",
    )
    .await;
    let first_secrets = first.secrets().clone();
    assert_eq!(
        first
            .services()
            .feature_secrets()
            .load("sourceControl.github.token")
            .await
            .unwrap()
            .as_deref(),
        Some("gho_follows")
    );
    assert!(
        !first.dir().join("secrets.json").exists(),
        "the orchestrator writes no secrets file"
    );
    let stored: Vec<u8> = sqlx::query_scalar(
        "SELECT ciphertext FROM tenant_secrets WHERE tenant_id = $1 AND account = $2",
    )
    .bind(&tenant)
    .bind("sourceControl.github.token")
    .fetch_one(&pool)
    .await
    .expect("the secret is a row");
    assert!(!stored.windows(11).any(|w| w == b"gho_follows"));

    // The first instance stops (its fence fires, as on a lost lease); a
    // second instance, on another host's data dir, takes the tenant over.
    first.fence().fire("test: the first instance stops");
    tokio::time::timeout(wait(), first.stopped())
        .await
        .expect("the first instance stops");
    // A fenced instance writes nothing. (After the takeover below, the
    // two instances of this one test process share the store's
    // process-local claim; the store tests check the old owner's refusal
    // across processes.)
    let refused = tokio::task::spawn_blocking({
        let secrets = first_secrets.clone();
        move || secrets.store("sourceControl.github.token", "gho_stale")
    })
    .await
    .unwrap();
    assert!(
        matches!(refused, Err(intent_core::Error::NotOwner(_))),
        "{refused:?}"
    );
    let second_dir = tempfile::tempdir().unwrap();
    let second = serve_when_free(
        &pool,
        &tenant,
        second_dir.path(),
        &format!("second-{run}"),
        keys.clone(),
    )
    .await;
    assert_eq!(
        second
            .services()
            .feature_secrets()
            .load("sourceControl.github.token")
            .await
            .unwrap()
            .as_deref(),
        Some("gho_follows"),
        "the new instance reads what the old one wrote"
    );

    drop(first_secrets);
    second.fence().fire("test: done");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_served_with_the_wrong_key_fails_closed_on_its_secrets() {
    let Some(pool) = pool().await else { return };
    let run = run_id();
    let tenant = format!("wrong-key-{run}");
    // Write a secret under the right key before any instance claims the
    // tenant.
    let unclaimed = Store::open_postgres(
        pool.clone(),
        TenantId::new(&tenant).unwrap(),
        PostgresOptions::default(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .unwrap();
    let right = TenantSecrets::new(&unclaimed, keyring(0x11)).unwrap();
    tokio::task::spawn_blocking(move || right.store("linear.token", "lin_secret"))
        .await
        .unwrap()
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let served = serve_tenant(
        pool,
        &tenant,
        dir.path(),
        &format!("instance-{run}"),
        LeaseTiming::default(),
        keyring(0x22),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve the tenant");
    let err = served
        .services()
        .feature_secrets()
        .load("linear.token")
        .await
        .expect_err("a secret the key cannot open is an error, not unset");
    assert!(err.to_string().contains("does not decrypt"), "{err}");
    served.fence().fire("test: done");
}

#[tokio::test]
async fn serving_on_a_current_thread_runtime_is_refused() {
    let Some(pool) = pool().await else { return };
    let run = run_id();
    let dir = tempfile::tempdir().unwrap();
    let refused = serve_tenant(
        pool,
        &format!("current-thread-{run}"),
        dir.path(),
        &format!("instance-{run}"),
        LeaseTiming::default(),
        keyring(0x11),
        OrchestratorOnly::for_tests(),
    )
    .await;
    let Err(err) = refused else {
        panic!("a current-thread runtime cannot serve tenant secrets");
    };
    assert!(err.to_string().contains("multi-thread"), "{err}");
    assert!(
        !intentd_orchestrator::tenant_dir(dir.path(), &format!("current-thread-{run}")).exists(),
        "nothing was set up"
    );
}

/// The credentials #3705 routed through the tenant's store, in Postgres
/// mode: each lands in the tenant's own encrypted rows and resolves from
/// them (`secret_source`, what the GitHub/GitLab/Linear/voice token chains
/// read), never from or into the process-wide default file. The GitLab
/// token, whose credential pair `Services` keeps in its own handle, lands
/// in the rows too: no tenant writes a `secrets.json`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_credentials_live_in_their_rows_not_the_host_file() {
    const IN_ROWS: &[&str] = &[
        "sourceControl.github.token",
        "sourceControl.gitlab.token",
        "linear.token",
        "voice.elevenlabs.apiKey",
        "voice.openai.apiKey",
    ];
    let Some(pool) = pool().await else { return };
    let run = run_id();
    let root = tempfile::tempdir().unwrap();
    let host_file = root.path().join("host.secrets.json");
    let host = intent_core::FileSecretStore::with_path(&host_file);
    for account in IN_ROWS {
        host.store(account, "host-value").unwrap();
    }
    let host_before = std::fs::read(&host_file).unwrap();
    std::env::set_var("INTENTD_SECRETS_FILE", &host_file);

    let keys = keyring(0x33);
    let mut served = Vec::new();
    for tag in ["a", "b"] {
        let tenant = format!("creds-{tag}-{run}");
        let s = serve_tenant(
            pool.clone(),
            &tenant,
            root.path(),
            &format!("instance-{run}"),
            LeaseTiming::default(),
            keys.clone(),
            OrchestratorOnly::for_tests(),
        )
        .await
        .expect("serve the tenant");
        assert!(
            !s.services().secret_source().allows_host_credentials(),
            "a tenant never falls back to the host's env vars or gh login"
        );
        for account in IN_ROWS {
            assert_eq!(
                s.services().secret_source().load(account).unwrap(),
                None,
                "a fresh tenant must not see the host's {account}"
            );
        }
        for account in IN_ROWS {
            save(s.services(), account, &format!("{tag}-{account}")).await;
        }
        served.push((tag, tenant, s));
    }

    for (tag, tenant, s) in &served {
        for account in IN_ROWS {
            let want = format!("{tag}-{account}");
            assert_eq!(
                s.services()
                    .secret_source()
                    .load(account)
                    .unwrap()
                    .as_deref(),
                Some(want.as_str()),
                "tenant {tag} resolves another store's {account}"
            );
        }
        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM tenant_secrets WHERE tenant_id = $1 AND account = ANY($2)",
        )
        .bind(tenant)
        .bind(IN_ROWS)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            i64::try_from(IN_ROWS.len()).unwrap(),
            "tenant {tag}'s rows"
        );
        assert!(
            !s.dir().join("secrets.json").exists(),
            "tenant {tag} wrote no file for these accounts"
        );
    }
    assert_eq!(
        std::fs::read(&host_file).unwrap(),
        host_before,
        "the host file is untouched"
    );
    std::env::remove_var("INTENTD_SECRETS_FILE");
    for (_, _, s) in &served {
        s.fence().fire("test: done");
    }
}
