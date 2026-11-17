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
//! through `pending_postgres!` and return [`unported`] errors.

use std::sync::Arc;

use intent_core::{Error, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, Postgres};
use sqlx::Transaction;

pub use sqlx::postgres::PgPool;

mod agent_messages;
mod agents;
mod comments;
mod discussions;
mod hooks;
mod nodes;
mod note_search;
mod notes;
mod scripts;
mod settings;
mod workspaces;

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

/// Connect a pool. The caller runs [`migrate`] once per database.
///
/// # Errors
/// `Error::Internal` when the connection fails.
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(connect_options(url)?)
        .await
        .map_err(|e| Error::Internal(format!("postgres connect failed: {e}")))
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
/// # Errors
/// `Error::Internal` when a statement fails, an applied migration changed,
/// the schema has a migration this build does not know, or the schema was
/// migrated by the earlier sqlx migrator.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    migrate_with(pool, false).await
}

/// [`migrate`], running each statement a second time before recording its
/// progress: what a crash between the two replays. For the store tests.
///
/// # Errors
/// As [`migrate`].
#[doc(hidden)]
pub async fn migrate_replaying_each_statement(pool: &PgPool) -> Result<()> {
    migrate_with(pool, true).await
}

async fn migrate_with(pool: &PgPool, replay: bool) -> Result<()> {
    let mut conn = pool.acquire().await.map_err(db)?.detach();
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('intent-schema-migrations', 0))")
        .execute(&mut conn)
        .await
        .map_err(db)?;
    let outcome = migrate_locked(&mut conn, replay).await;
    let closed = sqlx::Connection::close(conn).await.map_err(db);
    outcome.and(closed)
}

/// `(version, checksum, applied_statements, applied_prefix, complete)`.
type LedgerRow = (i64, Vec<u8>, i32, Vec<u8>, bool);

async fn migrate_locked(conn: &mut sqlx::PgConnection, replay: bool) -> Result<()> {
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
    }
    Ok(())
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
pub(crate) struct WriteTx(Transaction<'static, Postgres>);

impl WriteTx {
    pub(crate) async fn commit(self) -> std::result::Result<(), sqlx::Error> {
        self.0.commit().await
    }
}

impl std::ops::Deref for WriteTx {
    type Target = sqlx::PgConnection;
    fn deref(&self) -> &sqlx::PgConnection {
        &self.0
    }
}

impl std::ops::DerefMut for WriteTx {
    fn deref_mut(&mut self) -> &mut sqlx::PgConnection {
        &mut self.0
    }
}

#[derive(Clone)]
pub(crate) struct PgStore {
    pool: PgPool,
    tenant: TenantId,
    options: PostgresOptions,
    /// Per-note content-write locks; see `note_write_lock`. Process-local,
    /// like `SQLite`'s: they order writers within this store.
    note_write_lanes: Arc<crate::note_write_lock::NoteWriteLanes>,
    task_event_baselines: Arc<crate::TaskEventBaselines>,
    task_events_enabled: Arc<std::sync::atomic::AtomicBool>,
}

impl PgStore {
    pub(crate) fn tenant(&self) -> &str {
        self.tenant.as_str()
    }

    /// Every operation runs in one transaction bound to this store's tenant.
    pub(crate) async fn begin(&self) -> Result<Transaction<'static, Postgres>> {
        let mut tx = self.pool.begin().await.map_err(db)?;
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
    pub(crate) async fn begin_write(&self) -> Result<WriteTx> {
        let mut tx = self.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('intent-writer:' || $1, 0))")
            .bind(self.tenant())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        Ok(WriteTx(tx))
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
    /// # Errors
    /// `Error::Internal` when provisioning fails.
    pub async fn open_postgres(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
    ) -> Result<Self> {
        let store = PgStore {
            pool,
            tenant,
            options,
            note_write_lanes: Arc::default(),
            task_event_baselines: Arc::default(),
            task_events_enabled: Arc::default(),
        };
        let mut tx = store.begin().await?;
        sqlx::query("SELECT intent_provision_tenant($1)")
            .bind(store.tenant())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Self {
            backend: crate::Backend::Postgres(store),
        })
    }
}

pub(crate) fn db(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("postgres: {e}"))
}

/// The error a not-yet-ported `Store` method returns on Postgres.
pub(crate) fn unported(method: &'static str) -> Error {
    Error::Internal(format!(
        "{method} is not yet supported on the Postgres store"
    ))
}

/// For the few not-yet-ported methods that do not return `Result`.
#[track_caller]
pub(crate) fn unported_panic(method: &'static str) -> ! {
    panic!("{method} is not yet supported on the Postgres store")
}

#[cfg(test)]
mod tests {
    use super::{statements, with_bundled_root_cert, MIGRATOR};

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
