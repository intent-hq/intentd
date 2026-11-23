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
//! drops the schema. Without the URL it skips. Each run also drops
//! `intent_ci_*` schemas older than two hours that killed runs left;
//! `INTENT_STORE_NEKI_KEEP_STALE=1` keeps a run to its own schema.
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
//!
//! A fresh `intent_ci_*` schema is not in the branch's data topology, so
//! every table in it lands on the authoritative shard: the sharded tests
//! then run, but nothing in them is cross-shard. To exercise real sharding,
//! bind one fixed schema in the data topology (every table with `tenant_id`
//! to a shard group with an xxhash index on `tenant_id` spread over two or
//! more shards, the migration ledger to the authoritative group) and name
//! it in `INTENT_STORE_NEKI_SHARDED_SCHEMA`. When the schema file gains a
//! tenant table, `migrate` stops until the topology binds it; the tests then bind
//! it to the schema's tenant group (`migrate_bound`, needs `neki_operator`)
//! and migrate again, so the topology keeps up with the schema. Each sharded test then drops,
//! creates and migrates that schema, keeping the topology, and drops it at
//! the end, so a schema left by an earlier history of the schema file (the
//! numbered migrations it replaced, or an edit in place) is never reused; a file lock in cargo's target temp directory
//! (`CARGO_TARGET_TMPDIR`) runs them one at a time on this host (do not
//! point two hosts at one schema). Only
//! in that mode do the topology-dependent tests run (routing, `row_seq`
//! order, cross-shard writer locks); they skip otherwise.
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
    migrate_bound(&pool).await?;
    let open = |tenant: &'static str| {
        let pool = pool.clone();
        async move {
            Store::open_postgres(
                pool,
                TenantId::new(tenant).map_err(|e| e.to_string())?,
                PostgresOptions::default(),
                intent_store::OrchestratorOnly::for_tests(),
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

/// [`postgres::migrate`], plus the rollout step it leaves to an operator on
/// a sharded schema: when it stops because the Neki data topology does not
/// bind a new tenant table (the run on #3642 found `tenant_lease` unbound),
/// bind the tables it names ([`bind_to_topology`]) and migrate again.
/// `applied` covers both runs. In an unbound fresh schema, and on a branch
/// whose topology already binds every table, this is `migrate`.
async fn migrate_bound(pool: &PgPool) -> Result<postgres::MigrateReport, String> {
    /// Gaps to bind at most: each binds every table unbound at the time,
    /// so more than one only if a later migration adds another.
    const BINDS: usize = 4;
    let before = completed_migrations(pool).await?;
    let mut binds = 0;
    let mut report = loop {
        let stopped = match postgres::migrate(pool).await {
            Ok(report) => break report,
            Err(e) => e.to_string(),
        };
        let gap = postgres::neki_topology_gap(pool)
            .await
            .map_err(|e| format!("{stopped}; then reading the topology: {e}"))?;
        let Some(gap) = gap else {
            return Err(stopped);
        };
        if binds == BINDS {
            return Err(format!("still a gap after {BINDS} binds: {stopped}"));
        }
        eprintln!("migrate stopped on a topology gap; binding: {gap}");
        bind_to_topology(pool, &gap).await?;
        binds += 1;
    };
    if binds > 0 {
        report.applied = completed_migrations(pool)
            .await?
            .into_iter()
            .filter(|version| !before.contains(version))
            .collect();
    }
    Ok(report)
}

/// The migrations the schema's ledger records complete; none without one.
async fn completed_migrations(pool: &PgPool) -> Result<Vec<i64>, String> {
    let (has_ledger,): (bool,) =
        sqlx::query_as("SELECT to_regclass('intent_schema_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await
            .map_err(|e| format!("look for the ledger: {e}"))?;
    if !has_ledger {
        return Ok(Vec::new());
    }
    sqlx::query_scalar("SELECT version FROM intent_schema_migrations WHERE complete ORDER BY 1")
        .fetch_all(pool)
        .await
        .map_err(|e| format!("read the ledger: {e}"))
}

/// Binds `gap`'s unbound tables to its shard group in the Neki data
/// topology, as the operator does in the rollout (`neki_operator`): reads
/// the topology and its revision, adds the tables, sets it at that revision
/// (`expected_revision`, so a concurrent change fails instead of being
/// overwritten) and waits until every router has it. Tables bound to another
/// group are an error: moving rows between groups is not a test's call.
async fn bind_to_topology(pool: &PgPool, gap: &postgres::NekiTopologyGap) -> Result<(), String> {
    if !gap.misplaced.is_empty() {
        return Err(format!("{gap}; not moving tables bound to another group"));
    }
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| format!("connect to bind: {e}"))?
        .detach();
    let bound = bind_on(&mut conn, gap).await;
    let _ = sqlx::Connection::close(conn).await;
    bound
}

/// A string as a SQL literal of unknown type, so Postgres reads it as
/// whatever type the `__neki` function parameter has.
fn sql_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The single row `call` (a set-returning call) yields, as a JSON object.
async fn neki_row(conn: &mut sqlx::PgConnection, call: &str) -> Result<serde_json::Value, String> {
    let sql = format!("SELECT to_jsonb(r)::text FROM (SELECT * FROM {call}) AS r");
    let row: String = sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .fetch_one(&mut *conn)
        .await
        .and_then(|row| sqlx::Row::try_get(&row, 0))
        .map_err(|e| format!("{}: {e}", call.split('(').next().unwrap_or(call)))?;
    serde_json::from_str(&row).map_err(|e| format!("parse {row}: {e}"))
}

/// The revision in a `__neki` result row: its `revision` field, else its
/// only `*revision` number that is not the one it was based on, else its
/// only number.
fn revision_in(row: &serde_json::Value) -> Result<i64, String> {
    if let Some(revision) = row["revision"].as_i64() {
        return Ok(revision);
    }
    let fields: Vec<(&String, i64)> = row
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(name, value)| Some((name, value.as_i64()?)))
        .collect();
    let named: Vec<i64> = fields
        .iter()
        .filter(|(name, _)| name.ends_with("revision") && !name.contains("based"))
        .map(|(_, revision)| *revision)
        .collect();
    if let [revision] = named.as_slice() {
        return Ok(*revision);
    }
    let numbers: Vec<i64> = fields.iter().map(|(_, n)| *n).collect();
    match numbers.as_slice() {
        [revision] => Ok(*revision),
        _ => Err(format!("no revision in {row}")),
    }
}

async fn bind_on(
    conn: &mut sqlx::PgConnection,
    gap: &postgres::NekiTopologyGap,
) -> Result<(), String> {
    let run = |sql: String| sqlx::raw_sql(sqlx::AssertSqlSafe(sql));
    run("SET __neki.tx_mode = 'multi'".to_owned())
        .execute(&mut *conn)
        .await
        .map_err(|e| format!("multi-shard mode: {e}"))?;
    let revision = revision_in(&neki_row(conn, "__neki.get_data_topology_revision()").await?)?;
    let doc: String = sqlx::query_scalar("SELECT __neki.get_data_topology()::text")
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| format!("read the data topology: {e}"))?;
    let mut topology: serde_json::Value =
        serde_json::from_str(&doc).map_err(|e| format!("parse the topology: {e}"))?;
    for table in &gap.unbound {
        topology["databases"][&gap.database]["schemas"][&gap.schema]["tables"][table] =
            serde_json::json!({ "shard_group": gap.shard_group });
    }
    let topology = sql_literal(&topology.to_string());
    let findings: Vec<String> = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "SELECT to_jsonb(f)::text FROM __neki.validate_data_topology({topology}, \
         '{{\"overwrite\":true}}') AS f"
    )))
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| format!("validate the topology: {e}"))?
    .iter()
    .map(|row| sqlx::Row::try_get(row, 0))
    .collect::<Result<_, _>>()
    .map_err(|e| format!("read the findings: {e}"))?;
    eprintln!("topology revision {revision}: validation findings {findings:?}");
    let options = serde_json::json!({
        "comment": format!(
            "intent-store postgres_neki: bind {} to {}",
            gap.unbound.join(", "),
            gap.shard_group
        ),
        "expected_revision": revision,
    });
    let set = neki_row(
        conn,
        &format!(
            "__neki.set_data_topology({topology}, true, {})",
            sql_literal(&options.to_string())
        ),
    )
    .await?;
    let applied = revision_in(&set)?;
    run(format!("SELECT __neki.wait_for_data_topology({applied})"))
        .execute(&mut *conn)
        .await
        .map_err(|e| format!("wait for topology revision {applied}: {e}"))?;
    eprintln!(
        "bound {} to {} at topology revision {applied}",
        gap.unbound.join(", "),
        gap.shard_group
    );
    Ok(())
}

/// Drop schemas left by runs that panicked or were killed
/// more than two hours ago, unless `INTENT_STORE_NEKI_KEEP_STALE=1` (a run
/// that may only touch its own schema).
async fn drop_stale_schemas(admin: &PgPool, now: u64) {
    if std::env::var("INTENT_STORE_NEKI_KEEP_STALE").as_deref() == Ok("1") {
        return;
    }
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

/// The SQLSTATE Neki returns when a transaction in single-shard mode
/// (`__neki.tx_mode = 'single'`) reaches a second shard.
const NEKI_SINGLE_SHARD_VIOLATION: &str = "NK313";

/// The shard the router sends `tenant`'s `tenant_writer` row to, read from
/// the `Shard <uid>` node of `EXPLAIN (ANALYZE, NEKI_PLAN)` (plain
/// `NEKI_PLAN` names only the shard group).
async fn shard_of(pool: &PgPool, tenant: &str) -> Result<String, String> {
    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, NEKI_PLAN) SELECT 1 FROM tenant_writer WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("plan {tenant}: {e}"))?;
    let shards: Vec<&str> = plan
        .iter()
        .filter_map(|line| line.trim_start().strip_prefix("->  Shard "))
        .filter_map(|rest| rest.split_whitespace().next())
        .collect();
    match shards.as_slice() {
        [shard] => Ok((*shard).to_owned()),
        _ => Err(format!(
            "plan for {tenant} does not name one shard:\n{}",
            plan.join("\n")
        )),
    }
}

/// Opens `tenant` on `pool`, which seeds its `tenant_writer` row.
async fn open_tenant(pool: &PgPool, tenant: &str) -> Result<Store, String> {
    Store::open_postgres(
        pool.clone(),
        TenantId::new(tenant).map_err(|e| e.to_string())?,
        PostgresOptions::default(),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .map_err(|e| format!("open {tenant}: {e}"))
}

/// Up to `count` tenants named after `prefix`, each with its shard, until
/// two shards are seen; `Err` when all landed on one.
async fn tenants_on_two_shards(
    pool: &PgPool,
    prefix: &str,
    count: usize,
) -> Result<Vec<(String, String)>, String> {
    let mut placed = Vec::new();
    for i in 0..count {
        let tenant = format!("{prefix}-{i}");
        let shard = shard_of(pool, &tenant).await?;
        placed.push((tenant, shard));
        let first = &placed[0].1;
        if placed.iter().any(|(_, shard)| shard != first) {
            return Ok(placed);
        }
    }
    Err(format!(
        "all {count} tenants routed to one shard: {placed:?}"
    ))
}

/// The prefix a fixed sharded schema must carry: the tests drop it.
const FIXED_SCHEMA_PREFIX: &str = "intent_shard_";

/// The topology-bound schema named in `INTENT_STORE_NEKI_SHARDED_SCHEMA`,
/// or `None` when it is unset or empty. Every sharded test drops this
/// schema, so it must carry the dedicated `intent_shard_` prefix (never
/// `public` or a real schema) and be a plain lower-case identifier (it is
/// spliced into DDL); anything else panics.
#[track_caller]
fn fixed_sharded_schema() -> Option<String> {
    let name = std::env::var("INTENT_STORE_NEKI_SHARDED_SCHEMA")
        .ok()
        .filter(|name| !name.is_empty())?;
    let plain = name.len() > FIXED_SCHEMA_PREFIX.len()
        && name.starts_with(FIXED_SCHEMA_PREFIX)
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    assert!(
        plain,
        "INTENT_STORE_NEKI_SHARDED_SCHEMA must be a plain lower-case identifier \
         starting with {FIXED_SCHEMA_PREFIX}, not {name:?}"
    );
    Some(name)
}

/// Where a test runs: a fresh, unbound `intent_ci_*` schema, or the fixed
/// schema the data topology binds.
enum RunSchema {
    Fresh,
    Fixed(String),
}

/// The schema for a sharded test: the fixed one when configured.
fn sharded_run_schema() -> RunSchema {
    fixed_sharded_schema().map_or(RunSchema::Fresh, RunSchema::Fixed)
}

/// An exclusive lock on a temp file named after `schema`, held until
/// dropped, so tests sharing the fixed schema run one at a time on this
/// host (as threads of one test binary or as separate processes).
/// (`flock`; `std::fs::File::lock` is newer than the crate's MSRV.)
#[cfg(unix)]
fn lock_fixed_schema(schema: &str) -> nix::fcntl::Flock<std::fs::File> {
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("intent-store-neki-{schema}.lock"));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusive)
        .unwrap_or_else(|(_, e)| panic!("lock {}: {e}", path.display()))
}

/// Without `flock`, run the fixed-schema tests one at a time by hand
/// (`--test-threads 1`, one process).
#[cfg(not(unix))]
fn lock_fixed_schema(_schema: &str) {}

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
    in_schema(url, RunSchema::Fresh, connections, single_shard, body).await
}

/// [`in_fresh_schema`] for a sharded test: in the fixed, topology-bound
/// schema when `INTENT_STORE_NEKI_SHARDED_SCHEMA` names one.
async fn in_sharded_schema<F, Fut>(
    url: &str,
    connections: u32,
    single_shard: bool,
    body: F,
) -> Result<(), String>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    in_schema(url, sharded_run_schema(), connections, single_shard, body).await
}

/// Runs `body` on a pool of `connections` whose `search_path` is `run`'s
/// schema, created empty for the run and dropped after it. A fixed schema
/// is dropped first if a killed run left it, under [`lock_fixed_schema`];
/// stale `intent_ci_*` schemas are only cleaned up by fresh-schema runs.
async fn in_schema<F, Fut>(
    url: &str,
    run: RunSchema,
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
    // Wait for the fixed schema's lock before connecting, so a waiting
    // test holds no idle Neki connection; wait off the runtime thread.
    let _lock = match &run {
        RunSchema::Fresh => None,
        RunSchema::Fixed(schema) => {
            let schema = schema.clone();
            Some(
                tokio::task::spawn_blocking(move || lock_fixed_schema(&schema))
                    .await
                    .expect("lock the fixed schema"),
            )
        }
    };
    let admin = postgres::connect(url, 1)
        .await
        .map_err(|e| format!("connect to Neki: {e}"))?;
    let schema = match run {
        RunSchema::Fresh => {
            drop_stale_schemas(&admin, now).await;
            format!("intent_ci_{now}_{}", uuid::Uuid::new_v4().simple())
        }
        RunSchema::Fixed(schema) => {
            exec(&admin, format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                .await
                .map_err(|e| format!("drop a leftover {schema}: {e}"))?;
            schema
        }
    };
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
    in_sharded_schema(&url, connections, true, |pool| async move {
        migrate_bound(&pool)
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

/// One transaction in single-shard mode locks a first tenant's writer row
/// and then tries each of 16 others in turn: a second tenant on another
/// shard must be refused with Neki's single-shard error, one on the same
/// shard must lock. In an unbound fresh schema every tenant is on the
/// authoritative shard, so every lock must succeed; in the fixed,
/// topology-bound schema each tenant's shard comes from the router's plan
/// ([`shard_of`]), and
/// all 16 sharing the first's shard is vanishingly unlikely, so at least
/// one cross-shard pair is required.
#[tokio::test]
async fn a_single_shard_transaction_cannot_reach_a_second_tenant_on_sharded_neki() {
    let Some(url) = neki_url("INTENT_STORE_NEKI_SHARDED_URL") else {
        return;
    };
    let fixed = matches!(sharded_run_schema(), RunSchema::Fixed(_));
    in_sharded_schema(&url, 2, true, |pool| async move {
        migrate_bound(&pool).await?;
        // A fresh schema is not in the data topology, so all its tenants
        // share the authoritative shard; only the fixed schema is sharded.
        let placement = |tenant: String| {
            let pool = pool.clone();
            async move {
                if fixed {
                    shard_of(&pool, &tenant).await
                } else {
                    Ok("the authoritative shard".to_owned())
                }
            }
        };
        let run = uuid::Uuid::new_v4().simple().to_string();
        let first = format!("neki-shard-0-{run}");
        let others: Vec<String> = (1..=16).map(|i| format!("neki-shard-{i}-{run}")).collect();
        for tenant in std::iter::once(&first).chain(&others) {
            let store = Store::open_postgres(
                pool.clone(),
                TenantId::new(tenant).map_err(|e| e.to_string())?,
                PostgresOptions::default(),
                intent_store::OrchestratorOnly::for_tests(),
            )
            .await
            .map_err(|e| format!("open {tenant}: {e}"))?;
            store
                .set_setting("userRules", "\"x\"")
                .await
                .map_err(|e| format!("write {tenant}: {e}"))?;
        }
        let first_shard = placement(first.clone()).await?;
        let mut cross_shard = 0;
        let mut wrong = Vec::new();
        for other in &others {
            let other_shard = placement(other.clone()).await?;
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
            let outcome = match &second {
                Ok(_) => "locked".to_owned(),
                Err(sqlx::Error::Database(e)) => {
                    format!("{} {e}", e.code().as_deref().unwrap_or("-"))
                }
                Err(e) => return Err(format!("lock {other}: {e}")),
            };
            eprintln!("  {first_shard} + {other_shard}: {outcome}");
            let rejected_by_neki = matches!(
                &second,
                Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some(NEKI_SINGLE_SHARD_VIOLATION)
            );
            if other_shard == first_shard {
                if second.is_err() {
                    wrong.push(format!("same-shard {other} was refused: {outcome}"));
                }
            } else {
                cross_shard += 1;
                if !rejected_by_neki {
                    wrong.push(format!("cross-shard {other} was not refused: {outcome}"));
                }
            }
        }
        eprintln!(
            "single-shard mode: {cross_shard} of {} second tenants on another shard than {first_shard}",
            others.len()
        );
        if !wrong.is_empty() {
            return Err(wrong.join("; "));
        }
        if fixed && cross_shard == 0 {
            // With two or more tenant shards, all 16 sharing the first's
            // shard is vanishingly unlikely: the topology is not applied.
            return Err(format!(
                "all {} tenants routed to {first_shard} in the fixed schema",
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
    in_sharded_schema(&url, 3, true, |pool| async move {
        use std::time::Duration;
        migrate_bound(&pool)
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
            intent_store::OrchestratorOnly::for_tests(),
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
            Ok(_) => {
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

/// Step 0c on Neki: a daemon's open refuses an unmigrated schema and writes
/// nothing; the migrate job (its own `multi` connection) applies every
/// migration, then waits for every router to see the DDL
/// (`__neki.wait_for_ddl`); a second run applies nothing and still waits;
/// the open then succeeds through a single-shard pool, and refuses again
/// once the ledger has a migration this build does not know.
#[tokio::test]
async fn migrate_waits_for_ddl_and_open_checks_the_schema_on_neki() {
    let Some(url) = neki_url("INTENT_STORE_NEKI_SHARDED_URL") else {
        return;
    };
    in_sharded_schema(&url, 2, true, |pool| async move {
        let expected: Vec<i64> = postgres::MIGRATOR.iter().map(|m| m.version).collect();
        let open = |tenant: &'static str| {
            let pool = pool.clone();
            async move {
                Store::open_postgres(
                    pool,
                    TenantId::new(tenant).map_err(|e| e.to_string())?,
                    PostgresOptions::default(),
                    intent_store::OrchestratorOnly::for_tests(),
                )
                .await
                .map_err(|e| e.to_string())
            }
        };
        let schema_mismatch = |tenant: &'static str| {
            let pool = pool.clone();
            async move {
                match Store::open_postgres(
                    pool,
                    TenantId::new(tenant).map_err(|e| e.to_string())?,
                    PostgresOptions::default(),
                    intent_store::OrchestratorOnly::for_tests(),
                )
                .await
                {
                    Err(intent_core::Error::SchemaMismatch(m)) => Ok(*m),
                    Err(e) => Err(format!("open {tenant}: expected SchemaMismatch, got {e}")),
                    Ok(_) => Err(format!("open {tenant} passed the schema gate")),
                }
            }
        };

        let unmigrated = schema_mismatch("neki-gate-before").await?;
        if !unmigrated.applied.is_empty() || unmigrated.expected != expected {
            return Err(format!("unmigrated mismatch: {unmigrated:?}"));
        }
        let (ledger,): (bool,) =
            sqlx::query_as("SELECT to_regclass('intent_schema_migrations') IS NOT NULL")
                .fetch_one(&pool)
                .await
                .map_err(|e| e.to_string())?;
        if ledger {
            return Err("the refused open created the migration ledger".into());
        }

        let first = migrate_bound(&pool)
            .await
            .map_err(|e| format!("migrate: {e}"))?;
        eprintln!("first migrate: {first:?}");
        if first.applied != expected {
            return Err(format!("first migrate applied {:?}", first.applied));
        }
        let Some(waited) = first.neki_ddl else {
            return Err("migrate did not wait for the DDL on Neki".into());
        };
        let again = postgres::migrate(&pool)
            .await
            .map_err(|e| format!("second migrate: {e}"))?;
        eprintln!("second migrate: {again:?}");
        if !again.applied.is_empty() {
            return Err(format!("second migrate applied {:?}", again.applied));
        }
        match again.neki_ddl {
            Some(v) if v.schema_version >= waited.schema_version => {}
            other => return Err(format!("second migrate waited for {other:?}")),
        }

        postgres::check_schema(&pool)
            .await
            .map_err(|e| format!("gate after migrate: {e}"))?;
        let store = open("neki-gate-after").await?;
        store
            .set_setting("userRules", "\"x\"")
            .await
            .map_err(|e| format!("write after migrate: {e}"))?;

        exec(
            &pool,
            "INSERT INTO intent_schema_migrations \
             (version, description, checksum, applied_statements, applied_prefix, complete) \
             VALUES (9999, 'future', '\\x00', 0, '\\x00', true)"
                .to_owned(),
        )
        .await
        .map_err(|e| format!("record a future migration: {e}"))?;
        let newer = schema_mismatch("neki-gate-newer").await?;
        if newer.applied.last() != Some(&9999) {
            return Err(format!("newer-schema mismatch: {newer:?}"));
        }
        Ok(())
    })
    .await
    .expect("migrate job and schema gate on Neki");
}

/// The URL of a sharded branch and its fixed, topology-bound schema, or
/// `None` (the test skips) unless both are set.
fn fixed_sharded_target() -> Option<String> {
    let url = neki_url("INTENT_STORE_NEKI_SHARDED_URL")?;
    if fixed_sharded_schema().is_none() {
        eprintln!("skipping: INTENT_STORE_NEKI_SHARDED_SCHEMA is unset");
        return None;
    }
    Some(url)
}

/// The fixed schema's data topology: after every migration, each table
/// with a `tenant_id` column is bound to a shard group whose index hashes
/// `tenant_id` (xxhash) over two or more shards, and each table without
/// one to the authoritative group. A migration that adds a tenant table
/// stops `migrate` until the topology binds it ([`migrate_bound`] binds it
/// here, as the operator does in a rollout); a table without `tenant_id`
/// fails this until it is bound to the authoritative group by hand.
#[tokio::test]
async fn the_data_topology_binds_every_table_of_the_fixed_schema() {
    let Some(url) = fixed_sharded_target() else {
        return;
    };
    in_sharded_schema(&url, 2, false, |pool| async move {
        migrate_bound(&pool)
            .await
            .map_err(|e| format!("migrate: {e}"))?;
        let (database, schema): (String, String) =
            sqlx::query_as("SELECT current_database()::text, current_schema()::text")
                .fetch_one(&pool)
                .await
                .map_err(|e| e.to_string())?;
        let (json,): (String,) = sqlx::query_as("SELECT __neki.get_data_topology()")
            .fetch_one(&pool)
            .await
            .map_err(|e| format!("read the data topology: {e}"))?;
        let topology: serde_json::Value =
            serde_json::from_str(&json).map_err(|e| format!("parse the topology: {e}"))?;
        let authoritative = topology["authoritative_shard_group"]
            .as_str()
            .ok_or("no authoritative_shard_group")?;
        let tables: Vec<(String, bool)> = sqlx::query_as(
            "SELECT c.relname::text, EXISTS (SELECT 1 FROM pg_attribute a \
               WHERE a.attrelid = c.oid AND a.attname = 'tenant_id' AND NOT a.attisdropped) \
             FROM pg_class c WHERE c.relnamespace = current_schema()::regnamespace \
               AND c.relkind IN ('r', 'p') ORDER BY 1",
        )
        .fetch_all(&pool)
        .await
        .map_err(|e| e.to_string())?;
        let bound = &topology["databases"][&database]["schemas"][&schema]["tables"];
        let mut problems = Vec::new();
        let mut tenant_tables = 0;
        for (table, has_tenant) in &tables {
            let Some(group) = bound[table]["shard_group"].as_str() else {
                problems.push(format!("{table} is not bound"));
                continue;
            };
            if !has_tenant {
                if group != authoritative {
                    problems.push(format!("{table} has no tenant_id but is bound to {group}"));
                }
                continue;
            }
            tenant_tables += 1;
            let Some(spec) = topology["shard_groups"]
                .as_array()
                .and_then(|groups| groups.iter().find(|g| g["uid"] == group))
            else {
                problems.push(format!("{table}: no shard group {group}"));
                continue;
            };
            let index = bound[table]["shard_index"]
                .as_str()
                .or_else(|| spec["default_shard_index"].as_str())
                .unwrap_or_default();
            let shard_index = &topology["shard_indexes"][index];
            let shards: std::collections::BTreeSet<&str> = spec["key_ranges"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|range| range["shard_uid"].as_str())
                .collect();
            if shard_index["type"] != "xxhash"
                || shard_index["columns"] != serde_json::json!(["tenant_id"])
                || shards.len() < 2
            {
                problems.push(format!(
                    "{table} is bound to {group} (index {index:?}: {shard_index}, shards {shards:?}), \
                     not an xxhash(tenant_id) group over two or more shards"
                ));
            }
        }
        eprintln!(
            "{schema}: {} tables, {tenant_tables} sharded by tenant_id, {} on {authoritative}",
            tables.len(),
            tables.len() - tenant_tables
        );
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems.join("\n"))
        }
    })
    .await
    .expect("the data topology binds the fixed schema");
}

/// The router plans each tenant's single-tenant read for one shard, and
/// two tenants land on different shards of the tenant group: sharding by
/// `tenant_id` is in effect, not every row on the authoritative shard.
#[tokio::test]
async fn tenants_route_to_different_shards_in_the_fixed_schema() {
    let Some(url) = fixed_sharded_target() else {
        return;
    };
    in_sharded_schema(&url, 2, true, |pool| async move {
        migrate_bound(&pool)
            .await
            .map_err(|e| format!("migrate: {e}"))?;
        // The authoritative group has one open key range: its uid is its shard's.
        let authoritative: String = sqlx::query_scalar(
            "SELECT (__neki.get_data_topology()::jsonb) ->> 'authoritative_shard_group'",
        )
        .fetch_one(&pool)
        .await
        .map_err(|e| format!("read the authoritative group: {e}"))?;
        let run = uuid::Uuid::new_v4().simple().to_string();
        let placed = tenants_on_two_shards(&pool, &format!("neki-route-{run}"), 32).await?;
        let (a, a_shard) = &placed[0];
        let (b, b_shard) = placed
            .iter()
            .find(|(_, shard)| shard != a_shard)
            .ok_or("no second shard")?;
        for (tenant, shard) in [(a, a_shard), (b, b_shard)] {
            if *shard == authoritative {
                return Err(format!(
                    "{tenant} routed to the authoritative shard {shard}"
                ));
            }
            open_tenant(&pool, tenant)
                .await?
                .set_setting("userRules", "\"routed\"")
                .await
                .map_err(|e| format!("write {tenant}: {e}"))?;
            let plan: Vec<String> = sqlx::query_scalar(
                "EXPLAIN (ANALYZE, NEKI_PLAN) SELECT value FROM settings \
                 WHERE tenant_id = $1 AND \"key\" = 'userRules'",
            )
            .bind(tenant)
            .fetch_all(&pool)
            .await
            .map_err(|e| e.to_string())?;
            eprintln!("{tenant}:\n{}", plan.join("\n"));
            let on_shard = format!("->  Shard {shard} ");
            let routed = plan.iter().any(|l| l.contains("Route [EqualUnique]"))
                && plan.iter().any(|l| l.trim_start().starts_with(&on_shard))
                && plan.iter().any(|l| l.trim() == "RowsTotal: 1");
            if !routed {
                return Err(format!(
                    "{tenant}'s settings read is not a one-row route to {shard}"
                ));
            }
        }
        eprintln!("{a} -> {a_shard}, {b} -> {b_shard} (authoritative {authoritative})");
        Ok(())
    })
    .await
    .expect("tenants route to different shards");
}

/// `row_seq` comes from the tenant's `tenant_writer` counter, not a
/// sequence: on sharded Neki each router reserves identity
/// values in batches, so an identity numbered rows out of insertion order.
/// For tenants on two shards, rows inserted under the writer lock, in one
/// large statement and then in transactions alternating between two
/// connections (each may reach the shard through a different router), come
/// back in insertion order from `ORDER BY row_seq`, strictly increasing.
#[tokio::test]
async fn row_seq_follows_insertion_order_on_tenant_shards() {
    /// Rows in one statement: past a router's batch of reserved values.
    const BULK: i64 = 2500;
    /// Alternating transactions, and rows in each.
    const ROUNDS: i64 = 20;
    const PER_ROUND: i64 = 60;
    let Some(url) = fixed_sharded_target() else {
        return;
    };
    in_sharded_schema(&url, 4, true, |pool| async move {
        migrate_bound(&pool)
            .await
            .map_err(|e| format!("migrate: {e}"))?;
        let run = uuid::Uuid::new_v4().simple().to_string();
        let placed = tenants_on_two_shards(&pool, &format!("neki-seq-{run}"), 32).await?;
        let first_shard = &placed[0].1;
        let pair = [
            placed[0].clone(),
            placed
                .iter()
                .find(|(_, shard)| shard != first_shard)
                .cloned()
                .ok_or("no second shard")?,
        ];
        let limits = postgres::WriterLimits::default();
        let attempt = std::time::Duration::from_secs(10);
        for (tenant, shard) in &pair {
            // Provisions the tenant (its seed rows take row_seq too).
            open_tenant(&pool, tenant).await?;
            let mut expected: Vec<String> = Vec::new();

            let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
            postgres::lock_tenant_writer(&mut tx, tenant, attempt, &limits)
                .await
                .map_err(|e| format!("lock {tenant}: {e}"))?;
            sqlx::query(
                "INSERT INTO settings (tenant_id, \"key\", value) \
                 SELECT $1, 'k-0-' || lpad(n::text, 5, '0'), 'v' \
                 FROM generate_series(1, $2::bigint) AS n ORDER BY n",
            )
            .bind(tenant)
            .bind(BULK)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("bulk insert for {tenant} on {shard}: {e}"))?;
            tx.commit().await.map_err(|e| format!("commit: {e}"))?;
            expected.extend((1..=BULK).map(|n| format!("k-0-{n:05}")));

            let mut conns = [
                pool.acquire().await.map_err(|e| e.to_string())?,
                pool.acquire().await.map_err(|e| e.to_string())?,
            ];
            for round in 1..=ROUNDS {
                let conn = &mut conns[usize::try_from(round % 2).unwrap_or(0)];
                let mut tx = sqlx::Connection::begin(&mut **conn)
                    .await
                    .map_err(|e| e.to_string())?;
                postgres::lock_tenant_writer(&mut tx, tenant, attempt, &limits)
                    .await
                    .map_err(|e| format!("lock {tenant} (round {round}): {e}"))?;
                sqlx::query(
                    "INSERT INTO settings (tenant_id, \"key\", value) \
                     SELECT $1, 'k-' || lpad($3::text, 2, '0') || '-' || lpad(n::text, 5, '0'), 'v' \
                     FROM generate_series(1, $2::bigint) AS n ORDER BY n",
                )
                .bind(tenant)
                .bind(PER_ROUND)
                .bind(round)
                .execute(&mut *tx)
                .await
                .map_err(|e| format!("round {round} insert for {tenant} on {shard}: {e}"))?;
                tx.commit().await.map_err(|e| format!("commit: {e}"))?;
                expected.extend((1..=PER_ROUND).map(|n| format!("k-{round:02}-{n:05}")));
            }
            drop(conns);

            let rows: Vec<(String, i64)> = sqlx::query_as(
                "SELECT \"key\", row_seq FROM settings \
                 WHERE tenant_id = $1 AND \"key\" LIKE 'k-%' ORDER BY row_seq",
            )
            .bind(tenant)
            .fetch_all(&pool)
            .await
            .map_err(|e| e.to_string())?;
            let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
            let increasing = rows.windows(2).all(|w| w[0].1 < w[1].1);
            eprintln!(
                "{tenant} on {shard}: {} rows, row_seq {}..={}, increasing {increasing}",
                rows.len(),
                rows.first().map_or(0, |r| r.1),
                rows.last().map_or(0, |r| r.1),
            );
            if keys != expected || !increasing {
                let first_wrong = keys.iter().zip(&expected).position(|(a, b)| *a != b.as_str());
                return Err(format!(
                    "{tenant} on {shard}: ORDER BY row_seq is not insertion order \
                     ({} rows, expected {}; first difference at {first_wrong:?}; increasing {increasing})",
                    keys.len(),
                    expected.len()
                ));
            }
        }
        Ok(())
    })
    .await
    .expect("row_seq is insertion order on tenant shards");
}

/// The tenant writer lock is a row lock on the tenant's own shard, so a
/// held lock for one tenant does not delay a writer of a tenant on another
/// shard (nor, as a control, one on the same shard); a second writer of the
/// held tenant still times out.
#[tokio::test]
async fn writer_locks_of_tenants_on_other_shards_do_not_block() {
    let Some(url) = fixed_sharded_target() else {
        return;
    };
    in_sharded_schema(&url, 4, true, |pool| async move {
        use std::time::{Duration, Instant};
        migrate_bound(&pool)
            .await
            .map_err(|e| format!("migrate: {e}"))?;
        let run = uuid::Uuid::new_v4().simple().to_string();
        let mut placed = tenants_on_two_shards(&pool, &format!("neki-lock-{run}"), 32).await?;
        let (held, held_shard) = placed[0].clone();
        let other_shard = placed
            .iter()
            .find(|(_, shard)| *shard != held_shard)
            .cloned()
            .ok_or("no second shard")?;
        let mut i = placed.len();
        let same_shard = loop {
            if let Some(found) = placed[1..].iter().find(|(_, shard)| *shard == held_shard) {
                break found.clone();
            }
            if i > 64 {
                return Err(format!(
                    "none of {i} tenants shares {held_shard} with {held}"
                ));
            }
            let tenant = format!("neki-lock-{run}-{i}");
            let shard = shard_of(&pool, &tenant).await?;
            placed.push((tenant, shard));
            i += 1;
        };
        for tenant in [&held, &other_shard.0, &same_shard.0] {
            open_tenant(&pool, tenant).await?;
        }
        let limits = postgres::WriterLimits::default();
        let mut holder = pool.begin().await.map_err(|e| e.to_string())?;
        postgres::lock_tenant_writer(&mut holder, &held, Duration::from_secs(10), &limits)
            .await
            .map_err(|e| format!("hold {held}'s writer lock: {e}"))?;
        let mut report = Vec::new();
        let mut problems = Vec::new();
        for (tenant, shard, expect_free) in [
            (&other_shard.0, &other_shard.1, true),
            (&same_shard.0, &same_shard.1, true),
            (&held, &held_shard, false),
        ] {
            let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
            let started = Instant::now();
            let got =
                postgres::lock_tenant_writer(&mut tx, tenant, Duration::from_secs(5), &limits)
                    .await;
            let took = started.elapsed();
            let _ = tx.rollback().await;
            let code = got
                .as_ref()
                .err()
                .and_then(|e| e.as_database_error())
                .and_then(sqlx::error::DatabaseError::code)
                .map(std::borrow::Cow::into_owned);
            report.push(format!(
                "{tenant} on {shard}: {code:?} after {} ms",
                took.as_millis()
            ));
            match (expect_free, &got) {
                (true, Ok(_)) if took < Duration::from_secs(3) => {}
                (false, Err(_)) if code.as_deref() == Some("55P03") => {}
                _ => problems.push(format!("{tenant} on {shard}: {got:?} after {took:?}")),
            }
        }
        let _ = holder.rollback().await;
        eprintln!("holding {held} on {held_shard}:\n  {}", report.join("\n  "));
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems.join("; "))
        }
    })
    .await
    .expect("writer locks on other shards do not block");
}
