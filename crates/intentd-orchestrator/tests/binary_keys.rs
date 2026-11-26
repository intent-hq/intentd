//! The orchestrator binary refuses to start without a tenant secret key:
//! it exits before it claims any tenant's lease.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::process::Command;

use intent_store::postgres;

fn run(
    url: &str,
    tenant: &str,
    home: &std::path::Path,
    extra: &[(&str, &str)],
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_intentd-orchestrator"));
    command
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("RUST_LOG", "info")
        .env(intentd_orchestrator::POSTGRES_URL_ENV, url)
        .env(intentd_orchestrator::TENANTS_ENV, tenant);
    for (name, value) in extra {
        command.env(name, value);
    }
    command.output().expect("run the orchestrator binary")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_usable_key_the_binary_exits_before_claiming_a_tenant() {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return;
    };
    let pool = postgres::connect(&url, 2).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    let home = tempfile::tempdir().unwrap();
    let tenant = format!(
        "no-key-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let cases: [(&[(&str, &str)], &str); 2] = [
        (&[], "tenant secrets need a key"),
        (
            &[("INTENTD_SECRETS_KEY_ID", "k1")],
            "INTENTD_SECRETS_KEY is not",
        ),
    ];
    for (extra, expected) in cases {
        let out = run(&url, &tenant, home.path(), extra);
        let log = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(1), "{log}");
        assert!(log.contains(expected), "{log}");
    }
    let leases: i64 =
        sqlx::query_scalar("SELECT count(*) FROM lease WHERE tenant_id = $1 AND scope = 'tenant'")
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(leases, 0, "no tenant was claimed");
    assert!(
        !home.path().join("intent").exists()
            || std::fs::read_dir(home.path().join("intent"))
                .unwrap()
                .next()
                .is_none(),
        "nothing was written to the data dir"
    );
}
