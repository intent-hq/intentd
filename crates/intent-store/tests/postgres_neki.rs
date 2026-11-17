//! The Postgres migrations and a two-tenant round trip on `PlanetScale` Neki,
//! the production target.
//!
//! Neki rejects some statements vanilla Postgres accepts (for example
//! user-created text-search configurations, Neki errors 302/303), so CI's
//! Postgres service alone cannot prove a migration deploys. This test
//! applies every migration in a fresh schema of the Neki branch named by
//! `INTENT_STORE_NEKI_URL`, opens two tenants with identical keys, checks
//! they stay apart, and always drops the schema. Each run uses its own
//! schema, so concurrent CI runs share the branch safely.
//!
//! Without the URL the test skips, unless `INTENT_STORE_REQUIRE_NEKI=1`
//! (set by CI's `store-neki` job when Postgres code changed) makes that a
//! failure.
#![cfg(feature = "postgres")]

use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::Store;

async fn exec(pool: &PgPool, sql: String) -> Result<(), String> {
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn round_trip(pool: PgPool) -> Result<(), String> {
    postgres::migrate(&pool).await.map_err(|e| e.to_string())?;
    let open = |tenant: &'static str| {
        let pool = pool.clone();
        async move {
            Store::open_postgres(
                pool,
                TenantId::new(tenant).map_err(|e| e.to_string())?,
                PostgresOptions::default(),
            )
            .await
            .map_err(|e| e.to_string())
        }
    };
    let a = open("neki-a").await?;
    let b = open("neki-b").await?;
    a.set_setting("userRules", "\"a\"")
        .await
        .map_err(|e| e.to_string())?;
    b.set_setting("userRules", "\"b\"")
        .await
        .map_err(|e| e.to_string())?;
    let seen = (
        a.get_setting("userRules")
            .await
            .map_err(|e| e.to_string())?,
        b.get_setting("userRules")
            .await
            .map_err(|e| e.to_string())?,
    );
    if seen != (Some("\"a\"".into()), Some("\"b\"".into())) {
        return Err(format!("tenants not isolated on Neki: {seen:?}"));
    }
    if a.daemon_id().await.map_err(|e| e.to_string())?
        == b.daemon_id().await.map_err(|e| e.to_string())?
    {
        return Err("tenants share a daemon id on Neki".into());
    }
    Ok(())
}

/// Drop schemas left by runs that panicked, were cancelled or timed out
/// more than two hours ago.
async fn drop_stale_schemas(admin: &PgPool, now: u64) {
    let names: Vec<(String,)> = sqlx::query_as(
        "SELECT nspname::text FROM pg_namespace WHERE nspname LIKE 'intent\\_ci\\_%'",
    )
    .fetch_all(admin)
    .await
    .expect("list run schemas");
    for (name,) in names {
        let started = name
            .strip_prefix("intent_ci_")
            .and_then(|rest| rest.split_once('_'))
            .and_then(|(secs, _)| secs.parse::<u64>().ok());
        if started.is_some_and(|secs| now.saturating_sub(secs) > 2 * 60 * 60) {
            if let Err(e) = exec(admin, format!("DROP SCHEMA IF EXISTS {name} CASCADE")).await {
                eprintln!("could not drop stale schema {name}: {e}");
            }
        }
    }
}

#[tokio::test]
async fn migrations_and_tenant_round_trip_run_on_neki() {
    // A missing Actions secret (forks, Dependabot) arrives as an empty value.
    let Some(url) = std::env::var("INTENT_STORE_NEKI_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_NEKI").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_NEKI=1 but INTENT_STORE_NEKI_URL is unset: add the \
             INTENT_STORE_NEKI_URL Actions secret (a Neki branch role that may create schemas)"
        );
        eprintln!("skipping: INTENT_STORE_NEKI_URL is unset");
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs();
    let schema = format!("intent_ci_{now}_{}", uuid::Uuid::new_v4().simple());
    let admin = postgres::connect(&url, 1).await.expect("connect to Neki");
    drop_stale_schemas(&admin, now).await;
    exec(&admin, format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create the run's schema");

    let search_path = format!("SET search_path = {schema}");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |conn, _| {
            let sql = search_path.clone();
            Box::pin(async move {
                sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(postgres::connect_options(&url).expect("parse the Neki url"))
        .await
        .expect("connect the schema pool");
    let outcome = round_trip(pool.clone()).await;
    pool.close().await;

    let dropped = exec(&admin, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin.close().await;
    outcome.expect("migrations and tenant round trip on Neki");
    dropped.expect("drop the run's schema");
}
