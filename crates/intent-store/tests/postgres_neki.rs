//! The Postgres migrations and a two-tenant round trip on `PlanetScale` Neki,
//! the production target.
//!
//! Neki rejects some statements vanilla Postgres accepts (for example
//! user-created text-search configurations, Neki errors 302/303), so the
//! `store-postgres` CI job alone cannot prove a migration deploys. Run this
//! by hand against a Neki branch before landing a migration:
//! `INTENT_STORE_NEKI_URL=... cargo test -p intent-store --features postgres
//! --test postgres_neki`. It applies every migration in a fresh schema,
//! opens two tenants with identical keys, checks they stay apart, and always
//! drops the schema. Without the URL it skips.
//!
//! The sharded tests (audit finding 1) need a Neki branch, ideally sharded
//! by `tenant_id` across more than one shard, in
//! `INTENT_STORE_NEKI_SHARDED_URL`; without it they skip. Each migrates its
//! own fresh schema, so run one Neki suite at a time (concurrent migrations
//! can hit Neki's DDL lock timeout).
//! - writers of one tenant in separate processes exclude each other through
//!   the tenant writer lock (the `tenant_writer` row lock), so no
//!   read-then-write increment is lost;
//! - a writer-lock wait past `lock_timeout` fails with SQLSTATE 55P03
//!   through the router, so a store writer retries and ends in `StoreBusy`;
//! - in single-shard mode (`PoolSettings::single_shard`), a transaction
//!   that reaches a second tenant's shard fails instead of running across
//!   shards.
#![cfg(feature = "postgres")]

#[path = "support/writer_race.rs"]
mod writer_race;

use std::future::Future;

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

/// Drop schemas left by runs that panicked or were killed
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

/// A URL from `var`, or `None` (the test skips) when it is unset or empty.
fn neki_url(var: &str) -> Option<String> {
    let url = std::env::var(var).ok().filter(|url| !url.is_empty());
    if url.is_none() {
        eprintln!("skipping: {var} is unset");
    }
    url
}

/// Runs `body` on a pool of `connections` whose `search_path` is a fresh
/// schema on the Neki database at `url` (in single-shard mode when
/// `single_shard`), then drops the schema.
async fn in_fresh_schema<F, Fut>(
    url: &str,
    connections: u32,
    single_shard: bool,
    body: F,
) -> Result<(), String>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs();
    let schema = format!("intent_ci_{now}_{}", uuid::Uuid::new_v4().simple());
    let admin = postgres::connect(url, 1)
        .await
        .map_err(|e| format!("connect to Neki: {e}"))?;
    drop_stale_schemas(&admin, now).await;
    exec(&admin, format!("CREATE SCHEMA {schema}"))
        .await
        .map_err(|e| format!("create the run's schema: {e}"))?;

    let search_path = format!("SET search_path = {schema}");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(connections)
        .after_connect(move |conn, _| {
            let sql = search_path.clone();
            Box::pin(async move {
                sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with({
            let options = postgres::connect_options(url).map_err(|e| e.to_string())?;
            if single_shard {
                postgres::single_shard_options(options)
            } else {
                options
            }
        })
        .await;
    let outcome = match pool {
        Ok(pool) => {
            let outcome = body(pool.clone()).await;
            pool.close().await;
            outcome
        }
        Err(e) => Err(format!("connect the schema pool: {e}")),
    };
    let dropped = exec(&admin, format!("DROP SCHEMA {schema} CASCADE")).await;
    admin.close().await;
    outcome?;
    dropped.map_err(|e| format!("drop the run's schema: {e}"))
}

#[tokio::test]
async fn migrations_and_tenant_round_trip_run_on_neki() {
    let Some(url) = neki_url("INTENT_STORE_NEKI_URL") else {
        return;
    };
    in_fresh_schema(&url, 2, false, round_trip)
        .await
        .expect("migrations and tenant round trip on Neki");
}

/// Audit finding 1: eight processes increment one tenant's counter at once.
/// With a writer lock that does not exclude on sharded Neki, two increments
/// read the same value and one is lost. On a one-shard branch the old
/// advisory lock excluded too (every session reaches the one shard), so
/// this only tells the two locks apart on a branch with more than one shard.
#[tokio::test]
async fn writers_of_one_tenant_exclude_each_other_on_sharded_neki() {
    let Some(url) = neki_url("INTENT_STORE_NEKI_SHARDED_URL") else {
        return;
    };
    let connections = u32::try_from(writer_race::PROCESSES).expect("small") + 1;
    in_fresh_schema(&url, connections, true, |pool| async move {
        postgres::migrate(&pool)
            .await
            .map_err(|e| format!("migrate: {e}"))?;
        // A patient budget: the race measures lost updates, not latency
        // (40 serialized writer transactions over the network).
        let mut options = PostgresOptions::default();
        options.writer.wait_budget = std::time::Duration::from_secs(120);
        let tenant = format!("neki-race-{}", uuid::Uuid::new_v4().simple());
        let outcome = writer_race::race_increments(&pool, &tenant, options).await?;
        outcome.check()
    })
    .await
    .expect("the tenant writer lock excludes on sharded Neki");
}

/// A client cannot tell which shard a tenant lives on, so one transaction
/// in single-shard mode locks a first tenant's writer row and then tries
/// each of 16 others in turn. Any on another shard must be rejected; with
/// more than one shard, all 16 sharing the first's shard is vanishingly
/// unlikely, so at least one rejection is required. On a one-shard branch
/// nothing is cross-shard: every lock must succeed.
#[tokio::test]
async fn a_single_shard_transaction_cannot_reach_a_second_tenant_on_sharded_neki() {
    let Some(url) = neki_url("INTENT_STORE_NEKI_SHARDED_URL") else {
        return;
    };
    in_fresh_schema(&url, 2, true, |pool| async move {
        postgres::migrate(&pool).await.map_err(|e| e.to_string())?;
        let run = uuid::Uuid::new_v4().simple().to_string();
        let first = format!("neki-shard-0-{run}");
        let others: Vec<String> = (1..=16).map(|i| format!("neki-shard-{i}-{run}")).collect();
        for tenant in std::iter::once(&first).chain(&others) {
            let store = Store::open_postgres(
                pool.clone(),
                TenantId::new(tenant).map_err(|e| e.to_string())?,
                PostgresOptions::default(),
            )
            .await
            .map_err(|e| format!("open {tenant}: {e}"))?;
            store
                .set_setting("userRules", "\"x\"")
                .await
                .map_err(|e| format!("write {tenant}: {e}"))?;
        }
        let mut rejected = Vec::new();
        for other in &others {
            let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
            sqlx::query(postgres::WRITER_LOCK_SQL)
                .bind(&first)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| format!("lock the first tenant: {e}"))?;
            let second = sqlx::query(postgres::WRITER_LOCK_SQL)
                .bind(other)
                .fetch_one(&mut *tx)
                .await;
            let _ = tx.rollback().await;
            match second {
                Ok(_) => {}
                // A lock wait or a lost connection is not a shard rejection.
                Err(sqlx::Error::Database(e)) if e.code().as_deref() != Some("55P03") => {
                    rejected.push(format!("{other}: {:?} {e}", e.code()));
                }
                Err(e) => return Err(format!("lock {other}: {e}")),
            }
        }
        let shards: i64 = sqlx::query_scalar("SELECT count(*) FROM __neki.list_shards()")
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("count the shards: {e}"))?;
        eprintln!(
            "single-shard mode rejected {} of {} second tenants on {shards} shard(s)",
            rejected.len(),
            others.len()
        );
        for line in &rejected {
            eprintln!("  {line}");
        }
        if shards < 2 {
            // Every tenant is on the one shard: nothing can be cross-shard,
            // so single-shard mode must reject nothing.
            eprintln!(
                "only {shards} shard(s): cross-shard rejection cannot be observed; \
                 checked that single-shard transactions run"
            );
            return if rejected.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "single-shard mode rejected a same-shard tenant: {rejected:?}"
                ))
            };
        }
        if rejected.is_empty() {
            return Err(format!(
                "single-shard mode let one transaction lock all {} tenants",
                others.len() + 1
            ));
        }
        Ok(())
    })
    .await
    .expect("single-shard mode on sharded Neki");
}

/// Batch A's lock retry and `StoreBusy` rely on a writer-lock wait past
/// `lock_timeout` failing with SQLSTATE 55P03. Check the Neki router returns
/// that code for the row lock, and that a store writer behind a held lock
/// ends in `StoreBusy` (retried, then given up), not an internal error.
#[tokio::test]
async fn a_held_writer_lock_times_out_as_55p03_and_store_busy_on_neki() {
    let Some(url) = neki_url("INTENT_STORE_NEKI_SHARDED_URL") else {
        return;
    };
    in_fresh_schema(&url, 3, true, |pool| async move {
        use std::time::Duration;
        postgres::migrate(&pool)
            .await
            .map_err(|e| format!("migrate: {e}"))?;
        let tenant = format!("neki-timeout-{}", uuid::Uuid::new_v4().simple());
        let quick = PostgresOptions {
            writer: postgres::WriterLimits {
                wait_budget: Duration::from_millis(1500),
                lock_attempt: Duration::from_millis(300),
                ..postgres::WriterLimits::default()
            },
            ..PostgresOptions::default()
        };
        // Opening seeds the tenant's lock row.
        let store = Store::open_postgres(
            pool.clone(),
            TenantId::new(&tenant).map_err(|e| e.to_string())?,
            quick,
        )
        .await
        .map_err(|e| format!("open: {e}"))?;

        let patient = postgres::WriterLimits {
            lock_attempt: Duration::from_secs(60),
            ..postgres::WriterLimits::default()
        };
        let mut holder = pool.begin().await.map_err(|e| e.to_string())?;
        postgres::lock_tenant_writer(&mut holder, &tenant, Duration::from_secs(10), &patient)
            .await
            .map_err(|e| format!("hold the writer lock: {e}"))?;

        let mut waiter = pool.begin().await.map_err(|e| e.to_string())?;
        let second_attempt = postgres::lock_tenant_writer(
            &mut waiter,
            &tenant,
            Duration::from_millis(200),
            &patient,
        )
        .await;
        let _ = waiter.rollback().await;
        let code = match second_attempt {
            Ok(()) => {
                let _ = holder.rollback().await;
                return Err("a second transaction took the held writer lock".into());
            }
            Err(e) => {
                let code = e
                    .as_database_error()
                    .and_then(sqlx::error::DatabaseError::code)
                    .map(std::borrow::Cow::into_owned);
                eprintln!("writer-lock timeout on Neki: code {code:?}: {e}");
                code
            }
        };

        let busy = store.set_setting("userRules", "\"x\"").await;
        let _ = holder.rollback().await;
        if code.as_deref() != Some("55P03") {
            return Err(format!("lock timeout SQLSTATE is {code:?}, not 55P03"));
        }
        match busy {
            Err(intent_core::Error::StoreBusy(_)) => {}
            other => return Err(format!("a writer behind the held lock got {other:?}")),
        }
        store
            .set_setting("userRules", "\"x\"")
            .await
            .map_err(|e| format!("write once the lock is free: {e}"))
    })
    .await
    .expect("writer-lock timeouts on Neki");
}
