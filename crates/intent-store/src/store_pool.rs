//! Restricted pool access: callers cannot replace initialization or export a raw pool.

use std::time::Duration;

use futures_util::{future::BoxFuture, stream::BoxStream, TryStreamExt};
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{SqliteQueryResult, SqliteRow, SqliteStatement, SqliteTypeInfo};
use sqlx::{Describe, Either, Execute, Executor, Sqlite, SqlitePool, Transaction};

/// Query/transaction access to a Store's pool without mutable connection options.
/// Clones, checked-out connections and detached connections retain owned startup.
/// There is deliberately no raw-pool conversion, dereference or options setter.
#[derive(Clone, Debug)]
pub struct StorePool {
    pool: SqlitePool,
    owned: bool,
}

/// Read-only pool settings; cannot be used to create or retarget connections.
#[derive(Clone, Copy, Debug)]
pub struct StorePoolOptions {
    max_connections: u32,
    acquire_timeout: Duration,
}

impl StorePoolOptions {
    /// Maximum concurrent pooled connections.
    #[must_use]
    pub fn get_max_connections(&self) -> u32 {
        self.max_connections
    }

    /// Deadline for obtaining and initializing a pooled connection.
    #[must_use]
    pub fn get_acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }
}

impl StorePool {
    pub(crate) fn new(pool: SqlitePool, owned: bool) -> Self {
        Self { pool, owned }
    }

    /// Acquire a connection, retaining ownership through cancelled initialization.
    ///
    /// # Errors
    /// Returns the underlying database, pool-closed or acquisition-timeout error.
    #[must_use]
    pub fn acquire(&self) -> BoxFuture<'static, Result<PoolConnection<Sqlite>, sqlx::Error>> {
        if !self.owned {
            return Box::pin(self.pool.acquire());
        }
        let pool = self.pool.clone();
        // Caller cancellation drops only the JoinHandle. The task keeps the pool
        // owned until initialization completes. Internal SQLx timeout is separate:
        // owned pools install their SQLite destructor before blocking PRAGMAs.
        // Keep the wrapper future small in deeply nested service operations.
        Box::pin(async move {
            tokio::spawn(async move { pool.acquire().await })
                .await
                .map_err(|error| {
                    sqlx::Error::Protocol(format!("pool acquisition task failed: {error}"))
                })?
        })
    }

    /// Acquire and begin a transaction through the same guarded path as queries.
    ///
    /// # Errors
    /// Returns an acquisition or transaction-begin error.
    pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        Transaction::begin(self.acquire().await?, None).await
    }

    /// Acquire and start a transaction with an explicit BEGIN statement.
    ///
    /// # Errors
    /// Returns an acquisition or transaction-begin error.
    pub async fn begin_with(
        &self,
        statement: impl Into<std::borrow::Cow<'static, str>>,
    ) -> Result<Transaction<'static, Sqlite>, sqlx::Error> {
        Transaction::begin(self.acquire().await?, Some(statement.into())).await
    }

    /// Take an existing idle connection without opening a new one.
    #[must_use]
    pub fn try_acquire(&self) -> Option<PoolConnection<Sqlite>> {
        self.pool.try_acquire()
    }

    /// Close the pool and wait for checked-out connections to return.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Wait for the pool to begin closing.
    #[must_use]
    pub fn close_event(&self) -> sqlx::pool::CloseEvent {
        self.pool.close_event()
    }

    /// Whether the pool is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.pool.is_closed()
    }

    /// Number of pooled connections, including checked-out connections.
    #[must_use]
    pub fn size(&self) -> u32 {
        self.pool.size()
    }

    /// Number of idle connections.
    #[must_use]
    pub fn num_idle(&self) -> usize {
        self.pool.num_idle()
    }

    /// Read settings without exporting a reconnectable options object.
    #[must_use]
    pub fn options(&self) -> StorePoolOptions {
        StorePoolOptions {
            max_connections: self.pool.options().get_max_connections(),
            acquire_timeout: self.pool.options().get_acquire_timeout(),
        }
    }
}

// Constructing an unrelated unowned pool does not grant a startup cleanup lease.
impl From<SqlitePool> for StorePool {
    fn from(pool: SqlitePool) -> Self {
        Self::new(pool, false)
    }
}

impl<'c> sqlx::Acquire<'c> for &'c StorePool {
    type Database = Sqlite;
    type Connection = PoolConnection<Sqlite>;

    fn acquire(self) -> BoxFuture<'c, Result<Self::Connection, sqlx::Error>> {
        StorePool::acquire(self)
    }

    fn begin(self) -> BoxFuture<'c, Result<Transaction<'c, Sqlite>, sqlx::Error>> {
        Box::pin(async move { Transaction::begin(StorePool::acquire(self).await?, None).await })
    }
}

impl<'c> Executor<'c> for &'c StorePool {
    type Database = Sqlite;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<SqliteQueryResult, SqliteRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        Box::pin(async_stream::try_stream! {
            let mut connection = self.acquire().await?;
            let mut rows = (&mut *connection).fetch_many(query);
            while let Some(row) = rows.try_next().await? { yield row; }
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<SqliteRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Sqlite>,
    {
        Box::pin(async move { (&mut *self.acquire().await?).fetch_optional(query).await })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [SqliteTypeInfo],
    ) -> BoxFuture<'e, Result<SqliteStatement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            (&mut *self.acquire().await?)
                .prepare_with(sql, parameters)
                .await
        })
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Sqlite>, sqlx::Error>>
    where
        'c: 'e,
    {
        Box::pin(async move { (&mut *self.acquire().await?).describe(sql).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;
    use std::sync::Arc;

    async fn internal_timeout_retains_worker(read: bool) {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("timeout.db");
        let initial = crate::Store::open(&db).await.unwrap();
        initial.close().await;
        drop(initial);
        let lock = directory.path().join("timeout.db.daemon.lock");
        let owner = Arc::new(crate::daemon_ownership::DaemonOwnership::acquire(&lock).unwrap());
        let weak_owner = Arc::downgrade(&owner);
        let timeout = Duration::from_millis(100);
        let raw = if read {
            crate::connect_read_owned_timeout(&db, Some(owner.clone()), timeout)
                .await
                .unwrap()
        } else {
            crate::connect_write_owned(&db, timeout, Some(owner.clone()))
                .await
                .unwrap()
        };
        let pool = StorePool::new(raw, true);
        pool.acquire().await.unwrap().close().await.unwrap();
        assert_eq!(pool.size(), 0);
        drop(owner);
        let mut blocker = sqlx::SqliteConnection::connect_with(
            &sqlx::sqlite::SqliteConnectOptions::new().filename(&db),
        )
        .await
        .unwrap();
        sqlx::query("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE")
            .execute(&mut blocker)
            .await
            .unwrap();
        let result = pool.acquire().await;
        assert!(matches!(result, Err(sqlx::Error::PoolTimedOut)));
        drop(pool);
        // The acquisition and facade are gone, but SQLite is still busy inside
        // its guarded initialization PRAGMA. Check the actual OS lease as well
        // as eventual destructor release; do not keep a strong test owner alive.
        assert!(weak_owner.upgrade().is_some());
        assert!(crate::daemon_ownership::DaemonOwnership::acquire(&lock).is_err());
        sqlx::query("COMMIT").execute(&mut blocker).await.unwrap();
        blocker.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while weak_owner.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initialization worker must eventually release the lease");
        assert!(crate::daemon_ownership::DaemonOwnership::acquire(&lock).is_ok());
    }

    #[tokio::test]
    async fn write_internal_timeout_retains_initializing_worker() {
        internal_timeout_retains_worker(false).await;
    }

    #[tokio::test]
    async fn read_internal_timeout_retains_initializing_worker() {
        internal_timeout_retains_worker(true).await;
    }
}
