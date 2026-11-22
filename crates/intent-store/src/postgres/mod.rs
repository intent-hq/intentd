//! Multi-tenant Postgres backend (cargo feature `postgres`).
//!
//! A [`Store`](crate::Store) opened with [`Store::open_postgres`](crate::Store::open_postgres)
//! serves exactly one tenant: every statement binds that tenant's id, so
//! callers never pass one. Tenants share one database (PlanetScale Neki in
//! production, sharded by `tenant_id`); the schema lives in
//! `postgres/migrations/` and `postgres_tenant_schema_lint` keeps every table,
//! key and trigger tenant-scoped.
//!
//! Release builds never enable this feature. Methods not ported yet go
//! through `pending_postgres!` and panic ([`unported_panic`]).
//!
//! # Pool contract
//!
//! Every tenant's store shares one [`PgPool`] (the pool's owner, not a
//! store, closes it). [`connect_with`] sizes it with [`PoolSettings`]:
//!
//! - **Acquire timeout** bounds every checkout, including opening a new
//!   connection. A checkout that times out is [`Error::StoreBusy`]: nothing
//!   ran, so the caller may retry.
//! - **Connect timeout** bounds the first connection [`connect_with`] opens.
//!   (libpq's `connect_timeout` URL parameter is not read by sqlx.)
//! - **Writers park at most one connection per tenant.** A store's writers
//!   take its tenant writer slot (an in-process semaphore of one permit)
//!   *before* they check out a connection, and hold it until the
//!   transaction ends. So a tenant with a burst of writers, or a writer stuck
//!   behind another process's lock, holds one connection, not the pool, and
//!   other tenants keep theirs. Size the pool for the tenants that write at
//!   once plus read headroom; past that, checkouts wait up to the acquire
//!   timeout. The slot is per tenant, shared by every store for it in this
//!   process (`tenant_registry`).
//! - **Single-shard mode** ([`PoolSettings::single_shard`], for Neki sharded
//!   by `tenant_id`) is a startup option of every connection, so a
//!   transaction that reaches a second shard fails instead of running
//!   across shards.
//! - **Writer waits are bounded** by [`WriterLimits`]: see
//!   [`PgStore::begin_write`] for the lock timeout and retry policy, and the
//!   statement and idle-in-transaction timeouts a writer transaction runs
//!   under. They are `SET LOCAL`, never session settings, so they also hold
//!   behind a transaction-mode pooler such as Neki's.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use intent_core::{Error, Result, SchemaMismatch};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, Postgres};
use sqlx::Transaction;

pub use sqlx::postgres::PgPool;

pub(crate) use agents::PreparedPgMonitorMessage;
pub use lease::{
    LeaseHeartbeat, LeaseState, TenantClaim, TenantLease, LEASE_RENEW_INTERVAL, LEASE_TTL,
};

mod agent_messages;
mod agents;
mod attachments;
mod browser_sender;
mod browser_tabs;
mod clients;
mod comments;
mod completion_watches;
mod desktop;
mod discussions;
mod events;
mod homes;
mod hooks;
mod idempotency;
mod lease;
mod mcp_oauth;
mod nodes;
mod note_search;
mod notes;
pub(crate) mod pr_monitors;
mod presence_focus;
mod principals;
mod purge;
mod read_seam;
mod repositories;
mod script_monitors;
mod scripts;
mod settings;
mod tenant_registry;
mod usage_metrics;
mod workspaces;

/// Test seams for the bounded workspace and agent deletes.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub use purge::probe as delete_probe;

/// Test seam that pauses a multi-statement read between its statements.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub use read_seam::probe as read_probe;

/// Postgres schema migrations, separate from the `SQLite` ones.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./postgres/migrations");

/// The tenant a Postgres store serves: 1-128 characters of ASCII letters,
/// digits, `-`, `_`, `.` and `:`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(Arc<str>);

impl TenantId {
    /// # Errors
    /// `Error::InvalidParams` when `id` is empty, longer than 128 characters,
    /// or uses another character.
    pub fn new(id: &str) -> Result<Self> {
        let valid = (1..=128).contains(&id.len())
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
        if !valid {
            return Err(Error::InvalidParams(format!("invalid tenant id: {id:?}")));
        }
        Ok(Self(id.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How a Postgres store runs its transactions.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostgresOptions {
    /// Set `intent.tenant_id` in every transaction, for the row-level
    /// security guard the store tests install. Production stores leave it
    /// off: their isolation is the tenant filter in every statement.
    pub rls_guard: bool,
    /// How long writers wait for the tenant writer lock, and the limits a
    /// writer transaction runs under.
    pub writer: WriterLimits,
}

/// Bounds on a writer: its wait for the tenant writer lock and the
/// transaction it then holds. See [`PgStore::begin_write`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterLimits {
    /// The total time a writer waits for its tenant's writer slot, a pool
    /// connection and the lock before it fails with [`Error::StoreBusy`].
    pub wait_budget: Duration,
    /// `lock_timeout` for one attempt at the writer lock (between attempts
    /// the writer gives its connection back to the pool), and for any other
    /// lock a writer transaction waits on once it holds the writer lock.
    pub lock_attempt: Duration,
    /// `statement_timeout` for each statement of a writer transaction, so a
    /// runaway statement cannot hold the tenant lock indefinitely.
    pub statement_timeout: Duration,
    /// `idle_in_transaction_session_timeout` for a writer transaction: the
    /// server ends one whose client stopped talking while it holds the lock.
    pub idle_in_transaction_timeout: Duration,
}

impl Default for WriterLimits {
    fn default() -> Self {
        Self {
            wait_budget: Duration::from_secs(15),
            lock_attempt: Duration::from_secs(2),
            statement_timeout: Duration::from_secs(60),
            idle_in_transaction_timeout: Duration::from_secs(60),
        }
    }
}

/// The pause before lock attempt `attempt + 1`, after `attempt` attempts
/// timed out: 25 ms doubling to at most 400 ms.
fn retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(25 << attempt.saturating_sub(1).min(4))
}

/// The `lock_timeout` for the next lock attempt, after `attempts` attempts
/// that timed out and `waited` since the writer started waiting, and the
/// pause before it; `None` once the budget is spent.
fn next_lock_attempt(
    limits: &WriterLimits,
    waited: Duration,
    attempts: u32,
) -> Option<(Duration, Duration)> {
    let pause = if attempts == 0 {
        Duration::ZERO
    } else {
        retry_backoff(attempts)
    };
    let timeout = lock_timeout_left(limits, waited + pause)?;
    Some((pause, timeout))
}

/// `lock_attempt`, cut to what the budget has left after `waited`; `None`
/// below 1 ms (`lock_timeout = 0` means "wait for ever": never send it).
fn lock_timeout_left(limits: &WriterLimits, waited: Duration) -> Option<Duration> {
    let left = limits.wait_budget.checked_sub(waited)?;
    let timeout = limits.lock_attempt.min(left);
    (timeout >= Duration::from_millis(1)).then_some(timeout)
}

/// Pool sizing and timeouts for [`connect_with`]; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSettings {
    pub max_connections: u32,
    /// The longest a checkout waits for a free or new connection.
    pub acquire_timeout: Duration,
    /// The longest [`connect_with`] waits for its first connection.
    pub connect_timeout: Duration,
    /// Open every connection in `PlanetScale` Neki's single-shard mode
    /// (`__neki.tx_mode = 'single'`, a startup option; see
    /// [`single_shard_options`]), so Neki rejects a statement that would
    /// take a transaction to a second shard instead of running it across
    /// shards (Neki's default, `multi`). Turn it on for a Neki database
    /// sharded by `tenant_id`: a store's transactions touch one tenant, so
    /// one shard. [`migrate`] switches its own connection back to `multi`.
    pub single_shard: bool,
}

impl PoolSettings {
    #[must_use]
    pub fn new(max_connections: u32) -> Self {
        Self {
            max_connections,
            acquire_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            single_shard: false,
        }
    }
}

/// Connect options for a `postgres://` URL. libpq's `sslrootcert=system`
/// ("verify against the system trust store", which `PlanetScale` URLs carry)
/// is dropped, because sqlx would read it as a file path; the bundled webpki
/// roots verify the server instead. libpq also makes that option imply
/// `sslmode=verify-full` and refuses weaker modes, and so does this: sqlx's
/// own default (`prefer`) checks no certificate.
///
/// # Errors
/// `Error::InvalidParams` when the URL does not parse, or pairs
/// `sslrootcert=system` with a mode weaker than `verify-full`.
pub fn connect_options(url: &str) -> Result<PgConnectOptions> {
    with_bundled_root_cert(url)?
        .parse::<PgConnectOptions>()
        .map_err(|e| Error::InvalidParams(format!("invalid postgres url: {e}")))
}

/// `options` with Neki's single-shard mode as a startup option, so every
/// connection starts in it and no transaction can leave it: Neki refuses to
/// change `__neki.tx_mode` inside a transaction. Vanilla Postgres accepts
/// the setting and ignores it. For pools built without [`connect_with`].
#[must_use]
pub fn single_shard_options(options: PgConnectOptions) -> PgConnectOptions {
    options.options([("__neki.tx_mode", "single")])
}

fn with_bundled_root_cert(url: &str) -> Result<String> {
    let Some((base, query)) = url.split_once('?') else {
        return Ok(url.to_owned());
    };
    let params: Vec<&str> = query.split('&').collect();
    if !params.contains(&"sslrootcert=system") {
        return Ok(url.to_owned());
    }
    let mut kept: Vec<&str> = params
        .into_iter()
        .filter(|kv| *kv != "sslrootcert=system")
        .collect();
    let mode = kept.iter().find_map(|kv| {
        kv.strip_prefix("sslmode=")
            .or_else(|| kv.strip_prefix("ssl-mode="))
    });
    match mode {
        None => kept.push("sslmode=verify-full"),
        Some(mode) if mode.eq_ignore_ascii_case("verify-full") => {}
        Some(mode) => {
            return Err(Error::InvalidParams(format!(
                "sslrootcert=system needs sslmode=verify-full, not {mode}"
            )));
        }
    }
    Ok(format!("{base}?{}", kept.join("&")))
}

/// Connect a pool with [`PoolSettings::new`]. The caller runs [`migrate`]
/// once per database.
///
/// # Errors
/// As [`connect_with`].
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    connect_with(url, PoolSettings::new(max_connections)).await
}

/// Connect a pool under the [pool contract](self#pool-contract).
///
/// # Errors
/// `Error::InvalidParams` for a bad URL; `Error::Internal` when the first
/// connection fails or takes longer than `settings.connect_timeout`.
pub async fn connect_with(url: &str, settings: PoolSettings) -> Result<PgPool> {
    let mut options = connect_options(url)?;
    if settings.single_shard {
        options = single_shard_options(options);
    }
    let connecting = pool_options(settings).connect_with(options);
    match tokio::time::timeout(settings.connect_timeout, connecting).await {
        Ok(pool) => pool.map_err(|e| Error::Internal(format!("postgres connect failed: {e}"))),
        Err(_) => Err(Error::Internal(format!(
            "postgres connect failed: no connection after {} ms",
            settings.connect_timeout.as_millis()
        ))),
    }
}

fn pool_options(settings: PoolSettings) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .acquire_timeout(settings.acquire_timeout)
}

/// Apply the Postgres schema migrations that are not applied yet.
///
/// Statement by statement, each in its own transaction: `PlanetScale` Neki
/// cannot plan a statement against a relation created earlier in the same
/// transaction (Neki error 817), and both sqlx's migrator and a
/// multi-statement simple query wrap a whole file in one. After each
/// statement commits, its progress is recorded in `intent_schema_migrations`
/// (in a separate transaction: Neki also rejects DML after DDL in one), so a
/// file that fails part-way resumes at the failed statement once it is
/// fixed, as long as the statements before it are unchanged. A crash between
/// a statement and its progress record replays that one statement, so every
/// statement must be safe to run twice (`IF [NOT] EXISTS`, `CREATE OR
/// REPLACE`); a unit test enforces it.
///
/// A session advisory lock keeps concurrent callers from migrating at once.
/// The lock's connection is detached from the pool and closed at the end,
/// so a cancelled or failed run releases the lock with its session.
///
/// # The migrate job
///
/// Serving daemons never call this: [`Store::open_postgres`](crate::Store::open_postgres)
/// only checks the schema ([`check_schema`]) and refuses a mismatch. One
/// migrate job runs it before a rollout, on its own connection in Neki's
/// cross-shard `multi` mode (a single-shard pool's startup mode is undone).
///
/// On `PlanetScale` Neki, native DDL runs on every shard but is **not
/// atomic across shards**: a failed statement can leave some shards changed
/// and others not. Re-running `migrate` is the repair, which is why every
/// statement must be safe to run again. Other routers may also not see the
/// DDL yet, so after the migrations (applied now or by an earlier run) the
/// job asks this router for its applied versions (`__neki.ddl_versions()`)
/// and waits until every router has them (`__neki.wait_for_ddl`, at most
/// [`NEKI_DDL_WAIT`]). Vanilla Postgres has neither function, so there is
/// nothing to wait for.
///
/// # Errors
/// `Error::Internal` when a statement fails, an applied migration changed,
/// the schema has a migration this build does not know, the schema was
/// migrated by the earlier sqlx migrator, or the Neki routers did not see
/// the DDL within [`NEKI_DDL_WAIT`] (the migrations are applied; re-run).
pub async fn migrate(pool: &PgPool) -> Result<MigrateReport> {
    migrate_with(pool, false).await
}

/// What a [`migrate`] run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateReport {
    /// Versions this run completed, ascending; empty when the schema was
    /// already current.
    pub applied: Vec<i64>,
    /// The router versions the run waited for on Neki; `None` on vanilla
    /// Postgres.
    pub neki_ddl: Option<NekiDdlVersions>,
}

/// A Neki router's applied schema and cluster catalog versions
/// (`__neki.ddl_versions()`), the arguments of `__neki.wait_for_ddl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NekiDdlVersions {
    pub schema_version: i64,
    pub cluster_version: i64,
}

/// The longest [`migrate`] waits for every Neki router to see its DDL.
pub const NEKI_DDL_WAIT: Duration = Duration::from_secs(120);

/// [`migrate`], running each statement a second time before recording its
/// progress: what a crash between the two replays. For the store tests.
///
/// # Errors
/// As [`migrate`].
#[doc(hidden)]
pub async fn migrate_replaying_each_statement(pool: &PgPool) -> Result<MigrateReport> {
    migrate_with(pool, true).await
}

async fn migrate_with(pool: &PgPool, replay: bool) -> Result<MigrateReport> {
    let mut conn = pool.acquire().await.map_err(db)?.detach();
    // Schema changes span every shard: undo a single-shard pool's startup
    // mode on this detached connection, which is closed at the end.
    sqlx::query(MULTI_SHARD_SQL)
        .execute(&mut conn)
        .await
        .map_err(db)?;
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('intent-schema-migrations', 0))")
        .execute(&mut conn)
        .await
        .map_err(db)?;
    let outcome = match migrate_locked(&mut conn, replay).await {
        Ok(applied) => wait_for_neki_ddl(&mut conn)
            .await
            .map(|neki_ddl| MigrateReport { applied, neki_ddl }),
        Err(e) => Err(e),
    };
    let closed = sqlx::Connection::close(conn).await.map_err(db);
    outcome.and_then(|report| closed.map(|()| report))
}

/// On Neki, waits until every router has the DDL this connection's router
/// applied; `None` on vanilla Postgres, which has no `__neki` functions.
async fn wait_for_neki_ddl(conn: &mut sqlx::PgConnection) -> Result<Option<NekiDdlVersions>> {
    let versions: std::result::Result<(i64, i64), sqlx::Error> =
        sqlx::query_as("SELECT schema_version, cluster_version FROM __neki.ddl_versions()")
            .fetch_one(&mut *conn)
            .await;
    let (schema_version, cluster_version) = match versions {
        Ok(versions) => versions,
        // invalid_schema_name: no `__neki` schema, so not Neki. Anything
        // else, including undefined_function (42883: `__neki` exists but the
        // function is missing or changed), is an error: skipping the wait on
        // Neki would let lagging routers serve the old schema.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("3F000") => {
            return Ok(None);
        }
        Err(e) => return Err(Error::Internal(format!("neki ddl_versions failed: {e}"))),
    };
    sqlx::query("SELECT __neki.wait_for_ddl($1, $2, make_interval(secs => $3))")
        .bind(schema_version)
        .bind(cluster_version)
        .bind(NEKI_DDL_WAIT.as_secs_f64())
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "postgres migrations are applied, but not every Neki router saw the DDL \
                 (schema version {schema_version}, cluster version {cluster_version}) within \
                 {} s; re-run the migrate job: {e}",
                NEKI_DDL_WAIT.as_secs()
            ))
        })?;
    Ok(Some(NekiDdlVersions {
        schema_version,
        cluster_version,
    }))
}

/// The schema gate every serving daemon passes before it writes: the
/// schema's migration ledger must record exactly this build's migrations,
/// each complete and unchanged. It only reads; an unmigrated schema (no
/// ledger table) is a mismatch, not something to create.
///
/// A binary older than the schema (the ledger has a migration it does not
/// know) is refused too: migrations are not checked to be
/// backward-compatible, so a newer schema may have dropped or changed what
/// the older binary uses. Roll out the new binary after the migrate job.
///
/// [`Store::open_postgres`](crate::Store::open_postgres) runs this on every
/// open, so the gate cannot be skipped; it costs one short read-only
/// transaction per open. A consequence for rollouts: once the migrate job
/// has run, a still-running older daemon keeps serving the tenants it has
/// open but refuses to open any other tenant until it is replaced, and a
/// newer daemon refuses every tenant until the migrate job has run. Keep
/// that window short: migrate, then replace the daemons at once.
///
/// The serving role needs `SELECT` on `intent_schema_migrations` (and
/// `to_regclass` needs `USAGE` on its schema).
///
/// # Errors
/// `Error::SchemaMismatch` on any difference; `Error::StoreBusy` when no
/// pool connection comes free; `Error::Internal` when the ledger cannot be
/// read.
pub async fn check_schema(pool: &PgPool) -> Result<()> {
    // One snapshot: a migrate job committing between the two reads must not
    // make the ledger look half-present.
    let mut tx = pool
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .await
        .map_err(|e| match e {
            sqlx::Error::PoolTimedOut => Error::StoreBusy(format!(
                "no postgres connection came free within {} ms",
                pool.options().get_acquire_timeout().as_millis()
            )),
            e => db(e),
        })?;
    let (has_ledger, legacy_sqlx): (bool, bool) = sqlx::query_as(
        "SELECT to_regclass('intent_schema_migrations') IS NOT NULL, \
                to_regclass('_sqlx_migrations') IS NOT NULL",
    )
    .fetch_one(&mut *tx)
    .await
    .map_err(db)?;
    let ledger: Vec<(i64, Vec<u8>, bool)> = if has_ledger {
        sqlx::query_as(
            "SELECT version, checksum, complete FROM intent_schema_migrations ORDER BY version",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?
    } else {
        Vec::new()
    };
    tx.commit().await.map_err(db)?;
    let embedded: Vec<(i64, &[u8])> = MIGRATOR
        .iter()
        .map(|m| (m.version, m.checksum.as_ref()))
        .collect();
    match schema_mismatch(&ledger, &embedded) {
        None => Ok(()),
        Some(mut mismatch) => {
            // As `migrate` decides: a legacy schema only without ledger rows.
            mismatch.legacy_sqlx = legacy_sqlx && ledger.is_empty();
            Err(Error::SchemaMismatch(Box::new(mismatch)))
        }
    }
}

/// The ledger rows `(version, checksum, complete)` against the embedded
/// migrations `(version, checksum)`; `None` when they match exactly.
fn schema_mismatch(
    ledger: &[(i64, Vec<u8>, bool)],
    embedded: &[(i64, &[u8])],
) -> Option<SchemaMismatch> {
    let mut applied: Vec<i64> = ledger
        .iter()
        .filter(|(_, _, complete)| *complete)
        .map(|(v, ..)| *v)
        .collect();
    applied.sort_unstable();
    let mut expected: Vec<i64> = embedded.iter().map(|(v, _)| *v).collect();
    expected.sort_unstable();
    let changed: Vec<i64> = ledger
        .iter()
        .filter(|(version, checksum, complete)| {
            *complete
                && embedded
                    .iter()
                    .any(|(v, sum)| v == version && *sum != checksum.as_slice())
        })
        .map(|(v, ..)| *v)
        .collect();
    let mut part_way: Vec<i64> = ledger
        .iter()
        .filter(|(_, _, complete)| !*complete)
        .map(|(v, ..)| *v)
        .collect();
    part_way.sort_unstable();
    if applied == expected && changed.is_empty() && part_way.is_empty() {
        return None;
    }
    Some(SchemaMismatch {
        applied,
        expected,
        changed,
        part_way,
        legacy_sqlx: false,
    })
}

/// `(version, checksum, applied_statements, applied_prefix, complete)`.
type LedgerRow = (i64, Vec<u8>, i32, Vec<u8>, bool);

/// Returns the versions this run completed.
async fn migrate_locked(conn: &mut sqlx::PgConnection, replay: bool) -> Result<Vec<i64>> {
    // The applied-migration ledger belongs to the schema, not to a tenant.
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS intent_schema_migrations \
         (version bigint PRIMARY KEY, description text NOT NULL, checksum bytea NOT NULL, \
          applied_statements integer NOT NULL, applied_prefix bytea NOT NULL, \
          complete boolean NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(&mut *conn)
    .await
    .map_err(db)?;
    let applied: Vec<LedgerRow> = sqlx::query_as(
        "SELECT version, checksum, applied_statements, applied_prefix, complete \
         FROM intent_schema_migrations ORDER BY version",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(db)?;
    if applied.is_empty() {
        let (legacy,): (bool,) =
            sqlx::query_as("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
                .fetch_one(&mut *conn)
                .await
                .map_err(db)?;
        if legacy {
            return Err(Error::Internal(
                "postgres schema was migrated by the earlier sqlx migrator \
                 (_sqlx_migrations) and has no intent_schema_migrations record; \
                 recreate the schema"
                    .into(),
            ));
        }
    }
    if let Some((version, ..)) = applied
        .iter()
        .find(|(v, ..)| !MIGRATOR.iter().any(|m| m.version == *v))
    {
        return Err(Error::Internal(format!(
            "postgres migration {version} is applied but unknown to this build"
        )));
    }
    let mut completed = Vec::new();
    for migration in MIGRATOR.iter() {
        let version = migration.version;
        let stmts = statements(migration.sql.as_str());
        let start = match applied.iter().find(|row| row.0 == version) {
            Some((_, checksum, _, _, true)) => {
                if checksum.as_slice() != migration.checksum.as_ref() {
                    return Err(Error::Internal(format!(
                        "postgres migration {version} changed after it was applied"
                    )));
                }
                continue;
            }
            Some((_, _, done, prefix, false)) => {
                let done = usize::try_from(*done).unwrap_or(usize::MAX);
                if done > stmts.len() || prefix_hash(&stmts[..done]) != *prefix {
                    return Err(Error::Internal(format!(
                        "postgres migration {version} stopped after {done} statements, and \
                         those statements changed since; restore them or repair the schema"
                    )));
                }
                done
            }
            None => {
                sqlx::query(
                    "INSERT INTO intent_schema_migrations \
                     (version, description, checksum, applied_statements, applied_prefix, complete) \
                     VALUES ($1, $2, $3, 0, $4, false)",
                )
                .bind(version)
                .bind(migration.description.as_ref())
                .bind(migration.checksum.as_ref())
                .bind(prefix_hash(&[]))
                .execute(&mut *conn)
                .await
                .map_err(db)?;
                0
            }
        };
        for (i, statement) in stmts.iter().enumerate().skip(start) {
            let done = i32::try_from(i + 1).map_err(db)?;
            for _ in 0..=usize::from(replay) {
                sqlx::raw_sql(sqlx::AssertSqlSafe((*statement).to_owned()))
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "postgres migration {version} ({}) statement {} failed: {e}",
                            migration.description,
                            i + 1
                        ))
                    })?;
            }
            sqlx::query(
                "UPDATE intent_schema_migrations \
                 SET applied_statements = $2, applied_prefix = $3 WHERE version = $1",
            )
            .bind(version)
            .bind(done)
            .bind(prefix_hash(&stmts[..=i]))
            .execute(&mut *conn)
            .await
            .map_err(db)?;
        }
        sqlx::query(
            "UPDATE intent_schema_migrations \
             SET complete = true, checksum = $2, description = $3, applied_at = now() \
             WHERE version = $1",
        )
        .bind(version)
        .bind(migration.checksum.as_ref())
        .bind(migration.description.as_ref())
        .execute(&mut *conn)
        .await
        .map_err(db)?;
        completed.push(version);
    }
    Ok(completed)
}

/// Identifies the statements a part-way migration already applied.
fn prefix_hash(stmts: &[&str]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for s in stmts {
        hash.update((s.len() as u64).to_le_bytes());
        hash.update(s.as_bytes());
    }
    hash.finalize().to_vec()
}

/// The statements of a migration file, split at each `;` outside quotes,
/// `$tag$` bodies and comments. Chunks with only whitespace and comments
/// are dropped.
fn statements(sql: &str) -> Vec<&str> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let (mut start, mut i, mut meaningful) = (0, 0, false);
    while i < b.len() {
        match b[i] {
            b'-' if b.get(i + 1) == Some(&b'-') => {
                i = sql[i..].find('\n').map_or(b.len(), |n| i + n);
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 0usize;
                while i < b.len() {
                    if b[i..].starts_with(b"/*") {
                        depth += 1;
                        i += 2;
                    } else if b[i..].starts_with(b"*/") {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
                continue;
            }
            quote @ (b'\'' | b'"') => {
                meaningful = true;
                i += 1;
                while i < b.len() {
                    if b[i] == quote {
                        if b.get(i + 1) == Some(&quote) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'$' => {
                if let Some(tag) = dollar_tag(&sql[i..]) {
                    meaningful = true;
                    let body = i + tag.len();
                    i = sql[body..]
                        .find(tag)
                        .map_or(b.len(), |n| body + n + tag.len());
                    continue;
                }
            }
            b';' => {
                if meaningful {
                    out.push(sql[start..=i].trim());
                }
                start = i + 1;
                meaningful = false;
            }
            c if !c.is_ascii_whitespace() => meaningful = true,
            _ => {}
        }
        i += 1;
    }
    if meaningful {
        out.push(sql[start..].trim());
    }
    out
}

/// The opening `$$` or `$tag$` at the start of `s`, if any (`$1` is not one).
fn dollar_tag(s: &str) -> Option<&str> {
    let end = s[1..].find('$')?;
    let tag = &s[1..=end];
    let valid = tag.is_empty()
        || (tag.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    valid.then(|| &s[..end + 2])
}

/// A transaction holding the tenant's writer lock; only
/// [`PgStore::begin_write`] makes one. Helpers that write inside a caller's
/// transaction take `&mut WriteTx`, so they cannot run without the lock.
/// It also holds the store's writer slot until it ends.
pub(crate) struct WriteTx {
    tx: Transaction<'static, Postgres>,
    _slot: tokio::sync::OwnedSemaphorePermit,
    /// The tenant's `tenant_writer` row as the lock returned it.
    writer: WriterRow,
}

impl WriteTx {
    /// The tenant's fence as this transaction locked it.
    pub(crate) fn writer(&self) -> &WriterRow {
        &self.writer
    }

    pub(crate) async fn commit(self) -> std::result::Result<(), sqlx::Error> {
        self.tx.commit().await
    }

    pub(crate) async fn rollback(self) -> std::result::Result<(), sqlx::Error> {
        self.tx.rollback().await
    }
}

impl std::ops::Deref for WriteTx {
    type Target = sqlx::PgConnection;
    fn deref(&self) -> &sqlx::PgConnection {
        &self.tx
    }
}

impl std::ops::DerefMut for WriteTx {
    fn deref_mut(&mut self) -> &mut sqlx::PgConnection {
        &mut self.tx
    }
}

/// The tenant's writer slot, shared by every store for the tenant in this
/// process (`tenant_registry`): one permit, taken before a writer checks out a
/// connection (see the [pool contract](self#pool-contract)).
#[derive(Clone)]
pub(crate) struct WriterSlot {
    permit: Arc<tokio::sync::Semaphore>,
    /// Writers waiting for the slot or the lock, for tests to observe.
    waiting: Arc<AtomicUsize>,
}

impl Default for WriterSlot {
    fn default() -> Self {
        Self {
            permit: Arc::new(tokio::sync::Semaphore::new(1)),
            waiting: Arc::default(),
        }
    }
}

impl WriterSlot {
    /// Whether both handles are the same slot.
    #[cfg(feature = "test-support")]
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.permit, &other.permit) && Arc::ptr_eq(&self.waiting, &other.waiting)
    }
}

/// Counts one writer in [`WriterSlot::waiting`] while it lives.
struct Waiting(Arc<AtomicUsize>);

impl Waiting {
    fn new(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Sets `lock_timeout` ($1), `statement_timeout` ($2) and
/// `idle_in_transaction_session_timeout` ($3) for the transaction, before
/// the writer asks for the lock.
const WRITER_LIMITS_SQL: &str = "SELECT set_config('lock_timeout', $1, true), \
     set_config('statement_timeout', $2, true), \
     set_config('idle_in_transaction_session_timeout', $3, true)";

/// The tenant writer lock ($1 tenant): a row lock on the tenant's
/// `tenant_writer` row. It names `tenant_id`, so Neki routes it to the
/// tenant's shard (an advisory lock reads no table and carries no shard key).
/// No row: the tenant has not written since the table was added.
///
/// It returns the row's fence (owner instance and epoch, see [`WriterRow`]).
/// The fence is compared in Rust, never in the `WHERE`: a stale epoch there
/// would read as "no row" and send the writer down the seed path. At READ
/// COMMITTED a `FOR UPDATE` that waited returns the newest committed
/// version of the row, so a writer that queued behind a takeover sees the
/// new owner.
#[doc(hidden)]
pub const WRITER_LOCK_SQL: &str =
    "SELECT owner_instance, epoch FROM tenant_writer WHERE tenant_id = $1 FOR UPDATE";

/// A tenant's fence as its locked `tenant_writer` row holds it: the
/// instance that owns the tenant (`None`: never claimed) and the tenant's
/// epoch, which every claim bumps.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct WriterRow {
    pub owner_instance: Option<String>,
    pub epoch: i64,
}

/// Seeds the tenant's `tenant_writer` row ($1). On a conflict with a
/// concurrent seed it waits for that transaction, under `lock_timeout`.
const WRITER_SEED_SQL: &str =
    "INSERT INTO tenant_writer (tenant_id) VALUES ($1) ON CONFLICT (tenant_id) DO NOTHING";

/// Resets `lock_timeout` ($1) once the writer holds the lock.
const WRITER_RESET_SQL: &str = "SELECT set_config('lock_timeout', $1, true)";

/// Neki's default, cross-shard mode, for [`migrate`]'s connection (a
/// session setting: Neki refuses it inside a transaction); see
/// [`PoolSettings::single_shard`].
const MULTI_SHARD_SQL: &str = "SET __neki.tx_mode = 'multi'";

/// Takes `tenant`'s writer lock in the transaction on `conn`: sets the
/// writer limits (`lock_timeout` = `attempt` for this attempt), seeds the
/// tenant's lock row if it has none, locks it, then resets `lock_timeout`
/// to `limits.lock_attempt` for the rest of the transaction. A wait past
/// `attempt` fails with SQLSTATE 55P03. Returns the locked row's fence; the
/// caller checks it ([`PgStore::begin_write`] does).
///
/// # Errors
/// The failed statement's error.
#[doc(hidden)]
pub async fn lock_tenant_writer(
    conn: &mut sqlx::PgConnection,
    tenant: &str,
    attempt: Duration,
    limits: &WriterLimits,
) -> std::result::Result<WriterRow, sqlx::Error> {
    sqlx::query(WRITER_LIMITS_SQL)
        .bind(millis(attempt))
        .bind(millis(limits.statement_timeout))
        .bind(millis(limits.idle_in_transaction_timeout))
        .execute(&mut *conn)
        .await?;
    let locked = sqlx::query_as::<_, WriterRow>(WRITER_LOCK_SQL)
        .bind(tenant)
        .fetch_optional(&mut *conn)
        .await?;
    let row = if let Some(row) = locked {
        row
    } else {
        sqlx::query(WRITER_SEED_SQL)
            .bind(tenant)
            .execute(&mut *conn)
            .await?;
        sqlx::query_as::<_, WriterRow>(WRITER_LOCK_SQL)
            .bind(tenant)
            .fetch_one(&mut *conn)
            .await?
    };
    sqlx::query(WRITER_RESET_SQL)
        .bind(millis(limits.lock_attempt))
        .execute(&mut *conn)
        .await?;
    Ok(row)
}

fn is_lock_timeout(e: &sqlx::Error) -> bool {
    // 55P03 lock_not_available: `lock_timeout` expired.
    let sqlx::Error::Database(d) = e else {
        return false;
    };
    d.code().as_deref() == Some("55P03")
}

fn writer_busy(tenant: &str, waited: Duration) -> Error {
    Error::StoreBusy(format!(
        "tenant {tenant}'s writer lock stayed held for {} ms; nothing was written",
        waited.as_millis()
    ))
}

/// Whether [`PgStore::begin_write_as`] checks the tenant's fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fence {
    /// Every write: the locked row must name this process's claim.
    Check,
    /// A claim, which changes the fence under the same lock.
    Skip,
}

#[derive(Clone)]
pub(crate) struct PgStore {
    pool: PgPool,
    tenant: TenantId,
    options: PostgresOptions,
    /// Per-note content-write locks; see `note_write_lock`. Process-local,
    /// shared by every open of this tenant (`tenant_registry`).
    note_write_lanes: Arc<crate::note_write_lock::NoteWriteLanes>,
    task_event_baselines: Arc<crate::TaskEventBaselines>,
    task_events_enabled: Arc<std::sync::atomic::AtomicBool>,
    /// Process-local, like `SQLite`'s; see `browser_tab_repo`.
    browser_tab_displayed: crate::browser_tab_repo::DisplayedOverlay,
    /// Process-local teardown gates; see `desktop_repo`.
    desktop_writes: crate::desktop_repo::DesktopWrites,
    /// See the [pool contract](self#pool-contract).
    writer_slot: WriterSlot,
    /// This process's claim on the tenant, checked by every write; see
    /// [`Self::begin_write`] and `lease`.
    claim: lease::ClaimSlot,
    /// Process-local: the queue version this process last saw per agent;
    /// see `agents::QueueVersions`.
    queue_versions: agents::QueueVersions,
    /// Keeps the gates above registered for the next open of this tenant;
    /// see `tenant_registry`. Held, not read, outside tests.
    #[cfg_attr(
        not(feature = "test-support"),
        expect(dead_code, reason = "held so the registry entry stays live")
    )]
    tenant_state: Arc<tenant_registry::TenantState>,
}

impl PgStore {
    pub(crate) fn tenant(&self) -> &str {
        self.tenant.as_str()
    }

    /// Every operation runs in one transaction bound to this store's tenant.
    /// A checkout past the pool's acquire timeout is [`Error::StoreBusy`].
    ///
    /// Plain `begin()` runs at READ COMMITTED: each statement sees the
    /// commits made before it started. Use it for writes (through
    /// [`Self::begin_write`]) and for reads of one statement; a read of more
    /// than one statement takes [`Self::begin_read_snapshot`].
    pub(crate) async fn begin(&self) -> Result<Transaction<'static, Postgres>> {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|e| self.checkout_error(e))?;
        self.bind_tenant(tx).await
    }

    /// A read-only transaction whose statements all see one snapshot
    /// (`REPEATABLE READ`, taken at its first statement), bound to this
    /// store's tenant. `SQLite` reads that run more than one statement on one
    /// read connection see one WAL snapshot; this is the Postgres match, so
    /// a check and the data it guards, or keys and the rows they page, come
    /// from the same commit. A read-only snapshot never fails to serialize.
    pub(crate) async fn begin_read_snapshot(&self) -> Result<Transaction<'static, Postgres>> {
        let tx = self
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .await
            .map_err(|e| self.checkout_error(e))?;
        self.bind_tenant(tx).await
    }

    fn checkout_error(&self, e: sqlx::Error) -> Error {
        match e {
            sqlx::Error::PoolTimedOut => Error::StoreBusy(format!(
                "no postgres connection came free within {} ms",
                self.pool.options().get_acquire_timeout().as_millis()
            )),
            e => db(e),
        }
    }

    async fn bind_tenant(
        &self,
        mut tx: Transaction<'static, Postgres>,
    ) -> Result<Transaction<'static, Postgres>> {
        if self.options.rls_guard {
            sqlx::query("SELECT set_config('intent.tenant_id', $1, true)")
                .bind(self.tenant())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        Ok(tx)
    }

    /// A write transaction that holds this tenant's writer lock until it
    /// ends. `SQLite` runs one writer at a time (`BEGIN IMMEDIATE` on a
    /// single-connection pool) and store code relies on that for its
    /// read-then-write sequences; this keeps that guarantee per tenant while
    /// other tenants write in parallel. Use it for any method that reads and
    /// then writes, or writes more than one statement.
    ///
    /// Waiting is bounded by [`WriterLimits`] (`options.writer`):
    ///
    /// 1. The writer takes its tenant's writer slot first, holding no
    ///    connection while it waits (see the
    ///    [pool contract](self#pool-contract)).
    /// 2. It then checks out a connection (a checkout waits at most what the
    ///    budget has left, even when the pool's acquire timeout is longer)
    ///    and asks for the tenant's row lock (`tenant_writer`, see
    ///    [`lock_tenant_writer`]) with `lock_timeout = lock_attempt`,
    ///    less if the budget has less left. Another process or store handle
    ///    may hold it.
    /// 3. **Retry policy:** an attempt that times out rolls back, returning
    ///    the connection, pauses 25 ms doubling to at most 400 ms, and tries
    ///    again, as long as `wait_budget` has time left. Nothing has run in
    ///    the transaction yet, so a retry is always safe.
    /// 4. Once the budget is spent (on the slot, a checkout or the lock, or
    ///    by the time the lock is granted), it rolls back and fails with
    ///    [`Error::StoreBusy`]. This store call wrote nothing, so
    ///    it may be retried later.
    ///
    /// The transaction then runs with `SET LOCAL statement_timeout` and
    /// `idle_in_transaction_session_timeout`. Once the lock is granted,
    /// `lock_timeout` is reset to the fixed `lock_attempt` (not the attempt's
    /// budget-cut value) for any other lock the transaction waits on; those
    /// failures stay `Internal`.
    ///
    /// **Fence.** With the lock held, the writer compares the locked row's
    /// owner and epoch with this process's claim on the tenant (`lease`).
    /// A store whose process claimed the tenant writes only while the row
    /// still names that claim; a store that never claimed writes only while
    /// no process ever claimed the tenant. Otherwise the transaction rolls
    /// back and the call fails with [`Error::NotOwner`], having written
    /// nothing. The row is read after the lock is granted, at READ
    /// COMMITTED, so a writer that waited behind a takeover sees it.
    pub(crate) async fn begin_write(&self) -> Result<WriteTx> {
        self.begin_write_as(Fence::Check).await
    }

    /// [`Self::begin_write`], with or without the fence check: only a claim
    /// skips it, because a claim is how the fence changes.
    pub(crate) async fn begin_write_as(&self, fence: Fence) -> Result<WriteTx> {
        let limits = self.options.writer;
        let started = Instant::now();
        let _waiting = Waiting::new(&self.writer_slot.waiting);
        let slot = tokio::time::timeout(
            limits.wait_budget,
            self.writer_slot.permit.clone().acquire_owned(),
        )
        .await
        .map_err(|_| writer_busy(self.tenant(), started.elapsed()))?
        .map_err(db)?;
        let mut attempts = 0;
        loop {
            let Some((pause, lock_timeout)) =
                next_lock_attempt(&limits, started.elapsed(), attempts)
            else {
                return Err(writer_busy(self.tenant(), started.elapsed()));
            };
            tokio::time::sleep(pause).await;
            let left = limits.wait_budget.saturating_sub(started.elapsed());
            let mut tx = tokio::time::timeout(left, self.begin())
                .await
                .map_err(|_| writer_busy(self.tenant(), started.elapsed()))??;
            // The checkout may have used part of this attempt's share.
            let Some(lock_timeout) =
                lock_timeout_left(&limits, started.elapsed()).map(|t| t.min(lock_timeout))
            else {
                let _ = tx.rollback().await;
                return Err(writer_busy(self.tenant(), started.elapsed()));
            };
            let locked = lock_tenant_writer(&mut tx, self.tenant(), lock_timeout, &limits).await;
            match locked {
                // The deadline is checked last too: a writer never starts
                // writing after its budget, however the wait was split.
                Ok(_) if started.elapsed() > limits.wait_budget => {
                    let _ = tx.rollback().await;
                    return Err(writer_busy(self.tenant(), started.elapsed()));
                }
                Ok(writer) => {
                    if fence == Fence::Check {
                        if let Err(e) = self.claim.check(self.tenant(), &writer) {
                            let _ = tx.rollback().await;
                            return Err(e);
                        }
                    }
                    return Ok(WriteTx {
                        tx,
                        _slot: slot,
                        writer,
                    });
                }
                Err(e) if is_lock_timeout(&e) => {
                    let _ = tx.rollback().await;
                    attempts += 1;
                }
                Err(e) => return Err(db(e)),
            }
        }
    }

    /// Nothing to release: tenants share the pool, and closing it would end
    /// every other tenant's store. The pool's owner closes it at shutdown.
    #[expect(clippy::unused_async, reason = "same signature as Store::close")]
    pub async fn close(&self) {}
}

impl crate::Store {
    /// Open the store for one tenant on a migrated database, creating the
    /// tenant's seed rows on first use (`intent_provision_tenant`).
    ///
    /// It never migrates. It first passes the schema gate
    /// ([`check_schema`](crate::postgres::check_schema)): unless the
    /// schema has exactly this build's migrations, it fails with
    /// `Error::SchemaMismatch` having written nothing. The migrate job
    /// ([`migrate`](crate::postgres::migrate)) runs before the daemons.
    ///
    /// Every open of one tenant in this process shares its process-local
    /// gates (`tenant_registry`), including the writer slot (see the
    /// [pool contract](crate::postgres#pool-contract)): any number of opens
    /// for one tenant park at most one connection in the writer lock.
    ///
    /// Postgres exists only in orchestrator mode: `_orchestrator` is the
    /// proof, minted by the intentd binary once it resolved that mode (see
    /// [`OrchestratorOnly`](crate::OrchestratorOnly)).
    ///
    /// # Errors
    /// `Error::SchemaMismatch` when the schema does not match this build;
    /// `Error::Internal` when provisioning fails; `Error::StoreBusy` when the
    /// tenant's writer lock stays held past the writer wait budget.
    pub async fn open_postgres(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        _orchestrator: crate::OrchestratorOnly,
    ) -> Result<Self> {
        // Every open of this tenant in this process shares one set of gates.
        let key = tenant_registry::tenant_key(&pool, &tenant);
        Self::open_postgres_keyed(pool, tenant, options, key).await
    }

    /// Test-only: [`Self::open_postgres`] as if from another process named
    /// `process`: the store gets its own process-local gates (writer slot,
    /// note lanes and the rest), shared only with opens for the same
    /// `process`, so only the database orders its writes against other
    /// opens of the tenant, as between two daemons.
    ///
    /// # Errors
    /// As [`Self::open_postgres`].
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn open_postgres_as_process(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        process: &str,
        _orchestrator: crate::OrchestratorOnly,
    ) -> Result<Self> {
        let key = format!(
            "{}@process:{process}",
            tenant_registry::tenant_key(&pool, &tenant)
        );
        Self::open_postgres_keyed(pool, tenant, options, key).await
    }

    async fn open_postgres_keyed(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        key: String,
    ) -> Result<Self> {
        let store = PgStore::for_key(pool, tenant, options, key);
        // The schema gate comes first: a daemon whose schema does not match
        // its build refuses before it writes anything (no seed rows, and no
        // later startup step such as a lease).
        check_schema(&store.pool).await?;
        store.provision().await?;
        Ok(Self {
            backend: crate::Backend::Postgres(store),
        })
    }
}

impl PgStore {
    /// A store sharing the process-local gates registered under `key`.
    fn for_key(pool: PgPool, tenant: TenantId, options: PostgresOptions, key: String) -> Self {
        let shared = tenant_registry::tenant_state(key);
        PgStore {
            pool,
            tenant,
            options,
            note_write_lanes: Arc::clone(&shared.note_write_lanes),
            task_event_baselines: Arc::clone(&shared.task_event_baselines),
            task_events_enabled: Arc::clone(&shared.task_events_enabled),
            browser_tab_displayed: shared.browser_tab_displayed.clone(),
            desktop_writes: shared.desktop_writes.clone(),
            writer_slot: shared.writer_slot.clone(),
            claim: shared.claim.clone(),
            queue_versions: shared.queue_versions.clone(),
            tenant_state: shared,
        }
    }

    /// Creates the tenant's seed rows on first use. They are writes, so this
    /// runs under the writer lock and its fence like every other write (the
    /// lint sees the call as one): an unclaimed open of a tenant another
    /// process owns fails `NotOwner` here.
    async fn provision(&self) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query("SELECT intent_provision_tenant($1)")
            .bind(self.tenant())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
}

impl crate::Store {
    /// Writers of this Postgres store's tenant, through any of its stores in
    /// this process, waiting for the writer slot or the tenant writer lock;
    /// 0 on `SQLite`. For tests that need a writer to be queued before they
    /// go on.
    #[doc(hidden)]
    #[must_use]
    pub fn postgres_writers_waiting(&self) -> usize {
        match &self.backend {
            crate::Backend::Postgres(store) => store.writer_slot.waiting.load(Ordering::SeqCst),
            crate::Backend::Sqlite(_) => 0,
        }
    }
}

/// A duration as a Postgres time setting, in milliseconds (at least 1:
/// 0 would turn the timeout off).
fn millis(d: Duration) -> String {
    d.as_millis().max(1).to_string()
}

pub(crate) fn db(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("postgres: {e}"))
}

/// What a not-yet-ported `Store` method does on Postgres.
#[track_caller]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "no Store method is pending today; kept for the next unported one"
    )
)]
pub(crate) fn unported_panic(method: &'static str) -> ! {
    panic!("{method} is not yet supported on the Postgres store")
}

/// The error a SQLite-only `Store` method returns on Postgres.
pub(crate) fn sqlite_only(method: &'static str) -> Error {
    Error::Internal(format!(
        "{method} is SQLite-only and has no Postgres equivalent"
    ))
}

/// For the SQLite-only methods that do not return `Result`.
#[track_caller]
pub(crate) fn sqlite_only_panic(method: &'static str) -> ! {
    panic!("{method} is SQLite-only and has no Postgres equivalent")
}

impl PgStore {
    /// The embedded migrations against the schema's completed ledger rows;
    /// a part-way migration is not applied yet.
    pub async fn migration_status(&self) -> Result<crate::MigrationStatus> {
        let expected: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
        let mut tx = self.begin().await?;
        let applied: Vec<i64> = sqlx::query_scalar(
            "SELECT version FROM intent_schema_migrations WHERE complete ORDER BY version",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("query migrations failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(crate::MigrationStatus { expected, applied })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        next_lock_attempt, retry_backoff, schema_mismatch, statements, with_bundled_root_cert,
        WriterLimits, MIGRATOR,
    };
    use std::time::Duration;

    const MS: fn(u64) -> Duration = Duration::from_millis;

    #[test]
    fn the_schema_gate_passes_only_an_exact_complete_unchanged_ledger() {
        let embedded: [(i64, &[u8]); 2] = [(1, b"a"), (2, b"b")];
        let row = |v: i64, sum: &[u8], complete: bool| (v, sum.to_vec(), complete);
        assert_eq!(
            schema_mismatch(&[row(1, b"a", true), row(2, b"b", true)], &embedded),
            None
        );
        let missing = schema_mismatch(&[row(1, b"a", true)], &embedded).unwrap();
        assert_eq!((missing.applied, missing.expected), (vec![1], vec![1, 2]));
        let empty = schema_mismatch(&[], &embedded).unwrap();
        assert!(empty.applied.is_empty());
        let part_way =
            schema_mismatch(&[row(1, b"a", true), row(2, b"b", false)], &embedded).unwrap();
        assert_eq!((part_way.applied, part_way.part_way), (vec![1], vec![2]));
        let changed =
            schema_mismatch(&[row(1, b"x", true), row(2, b"b", true)], &embedded).unwrap();
        assert_eq!((changed.applied, changed.changed), (vec![1, 2], vec![1]));
        let newer = schema_mismatch(
            &[row(1, b"a", true), row(2, b"b", true), row(3, b"c", true)],
            &embedded,
        )
        .unwrap();
        assert_eq!(newer.applied, vec![1, 2, 3]);
        // An unknown part-way version is still a newer schema.
        let newer_part_way = schema_mismatch(
            &[row(1, b"a", true), row(2, b"b", true), row(3, b"c", false)],
            &embedded,
        )
        .unwrap();
        assert_eq!(newer_part_way.unknown(), vec![3]);
    }

    #[test]
    fn lock_retries_back_off_from_25_ms_doubling_to_400_ms() {
        let pauses: Vec<u128> = (1..=7).map(|n| retry_backoff(n).as_millis()).collect();
        assert_eq!(pauses, [25, 50, 100, 200, 400, 400, 400]);
    }

    #[test]
    fn lock_attempts_stop_when_the_wait_budget_is_spent() {
        let limits = WriterLimits {
            wait_budget: MS(1000),
            lock_attempt: MS(300),
            ..WriterLimits::default()
        };
        // The first attempt starts at once with the full attempt timeout.
        assert_eq!(next_lock_attempt(&limits, MS(0), 0), Some((MS(0), MS(300))));
        // A retry pauses first, then gets what the budget has left.
        assert_eq!(
            next_lock_attempt(&limits, MS(310), 1),
            Some((MS(25), MS(300)))
        );
        assert_eq!(
            next_lock_attempt(&limits, MS(900), 2),
            Some((MS(50), MS(50)))
        );
        // Never `lock_timeout = 0`, which would wait for ever.
        assert_eq!(next_lock_attempt(&limits, MS(975), 1), None);
        assert_eq!(next_lock_attempt(&limits, MS(1200), 3), None);
        let none = WriterLimits {
            wait_budget: MS(0),
            ..limits
        };
        assert_eq!(next_lock_attempt(&none, MS(0), 0), None);
    }

    #[test]
    fn writer_settings_are_whole_milliseconds_and_never_zero() {
        assert_eq!(super::millis(Duration::from_secs(2)), "2000");
        assert_eq!(super::millis(Duration::from_micros(10)), "1");
    }

    #[test]
    fn migration_files_split_into_statements_outside_function_bodies() {
        let sql = "-- header\nCREATE TABLE a (x int);\n\nCREATE FUNCTION f() RETURNS int\nLANGUAGE plpgsql AS $$\nBEGIN\n    RETURN 1;\nEND\n$$;\n-- trailing comment\n";
        let parts = statements(sql);
        assert_eq!(parts.len(), 2, "{parts:?}");
        assert!(parts[0].ends_with("CREATE TABLE a (x int);"));
        assert!(parts[1].starts_with("CREATE FUNCTION f()") && parts[1].contains("RETURN 1;"));
    }

    #[test]
    fn the_splitter_ignores_semicolons_in_comments_quotes_and_tagged_bodies() {
        let sql = "CREATE TABLE a (\n  -- one; two;\n  x int\n); -- done; really\n\
                   SELECT 'a;b''c;' AS \"x;y\";\n\
                   /* block; /* nested; */ still; */ SELECT 1;\n\
                   CREATE FUNCTION g() RETURNS text LANGUAGE plpgsql AS $body$\n\
                   BEGIN RETURN $$q;$$; END\n$body$;\n\
                   SELECT $1::int;";
        let parts = statements(sql);
        assert_eq!(parts.len(), 5, "{parts:#?}");
        assert!(parts[0].starts_with("CREATE TABLE a") && parts[0].ends_with(");"));
        assert!(parts[1].starts_with("-- done; really") && parts[1].ends_with("\"x;y\";"));
        assert!(parts[2].ends_with("SELECT 1;"));
        assert!(parts[3].contains("RETURN $$q;$$; END") && parts[3].ends_with("$body$;"));
        assert_eq!(parts[4], "SELECT $1::int;");
    }

    #[test]
    fn every_migration_statement_ends_with_its_semicolon() {
        for m in MIGRATOR.iter() {
            for s in statements(m.sql.as_str()) {
                assert!(s.ends_with(';'), "migration {}: {s}", m.version);
            }
        }
    }

    /// Whether `statement` is a no-op when run again right after it applied.
    fn rerunnable(statement: &str) -> bool {
        const PREFIXES: &[&str] = &[
            "CREATE TABLE IF NOT EXISTS ",
            "CREATE INDEX IF NOT EXISTS ",
            "CREATE UNIQUE INDEX IF NOT EXISTS ",
            "CREATE OR REPLACE FUNCTION ",
            "CREATE OR REPLACE TRIGGER ",
            "CREATE EXTENSION IF NOT EXISTS ",
        ];
        let code: String = statement
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase();
        let code = code.split_whitespace().collect::<Vec<_>>().join(" ");
        if PREFIXES.iter().any(|p| code.starts_with(p)) {
            return true;
        }
        if code.starts_with("DROP ") {
            return code.split(' ').nth(2) == Some("IF") || code.contains(" IF EXISTS ");
        }
        let Some(rest) = code.strip_prefix("ALTER TABLE ") else {
            return false;
        };
        let Some((_, action)) = rest.split_once(' ') else {
            return false;
        };
        if action.starts_with("ADD COLUMN IF NOT EXISTS ")
            || action.starts_with("DROP COLUMN IF EXISTS ")
        {
            return true;
        }
        // Re-adding a constraint: drop the same name first, in one statement.
        action
            .strip_prefix("DROP CONSTRAINT IF EXISTS ")
            .and_then(|a| a.split_once(", ADD CONSTRAINT "))
            .is_some_and(|(dropped, added)| added.starts_with(&format!("{dropped} ")))
    }

    /// `migrate` records progress after each statement commits, so a crash
    /// in between replays that one statement: it must be safe to run twice.
    #[test]
    fn every_migration_statement_is_safe_to_run_twice() {
        let bad: Vec<String> = MIGRATOR
            .iter()
            .flat_map(|m| {
                statements(m.sql.as_str())
                    .into_iter()
                    .filter(|s| !rerunnable(s))
                    .map(move |s| {
                        format!(
                            "migration {}: {}",
                            m.version,
                            s.lines().next().unwrap_or("")
                        )
                    })
            })
            .collect();
        assert!(
            bad.is_empty(),
            "use IF [NOT] EXISTS, CREATE OR REPLACE, or DROP CONSTRAINT IF EXISTS c, ADD CONSTRAINT c:\n{}",
            bad.join("\n")
        );
    }

    #[test]
    fn the_rerunnable_rule_rejects_one_shot_statements() {
        for one_shot in [
            "CREATE TABLE t (x int);",
            "CREATE INDEX i ON t (x);",
            "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;",
            "CREATE TRIGGER g AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();",
            "ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY (x) REFERENCES u (x);",
            "ALTER TABLE t DROP CONSTRAINT IF EXISTS c, ADD CONSTRAINT d FOREIGN KEY (x) REFERENCES u (x);",
            "ALTER TABLE t ADD COLUMN y int;",
            "DROP INDEX i;",
            "TRUNCATE t;",
        ] {
            assert!(!rerunnable(one_shot), "{one_shot}");
        }
        for safe in [
            "-- why\nCREATE TABLE IF NOT EXISTS t (x int);",
            "ALTER TABLE t DROP CONSTRAINT IF EXISTS c, ADD CONSTRAINT c FOREIGN KEY (x) REFERENCES u (x);",
            "DROP INDEX IF EXISTS i;",
        ] {
            assert!(rerunnable(safe), "{safe}");
        }
    }

    #[test]
    fn system_root_cert_is_dropped_and_verify_full_enforced() {
        let ok = |url: &str| with_bundled_root_cert(url).expect(url);
        assert_eq!(
            ok("postgresql://u:p@h:5432/db?sslmode=verify-full&sslrootcert=system"),
            "postgresql://u:p@h:5432/db?sslmode=verify-full"
        );
        assert_eq!(
            ok("postgresql://h/db?sslrootcert=system"),
            "postgresql://h/db?sslmode=verify-full"
        );
        assert_eq!(
            ok("postgresql://h/db?sslrootcert=/etc/ca.pem&sslmode=require"),
            "postgresql://h/db?sslrootcert=/etc/ca.pem&sslmode=require"
        );
        assert_eq!(ok("postgresql://h/db"), "postgresql://h/db");
        for weak in ["require", "prefer", "disable", "verify-ca"] {
            let url = format!("postgresql://h/db?sslrootcert=system&sslmode={weak}");
            assert!(with_bundled_root_cert(&url).is_err(), "{weak} accepted");
        }
    }
}
