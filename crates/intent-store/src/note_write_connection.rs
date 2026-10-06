//! Cancellation ownership for raw note-writer transactions.
use std::ops::{Deref, DerefMut};

use intent_core::{Error, Result};
use sqlx::{pool::PoolConnection, Connection, Sqlite, SqliteConnection};

use crate::Store;

/// A cancelled writer closes its connection instead of returning an open raw
/// transaction to the pool. Normal completion preserves explicit rollback and
/// poisoned-connection handling.
pub(crate) struct NoteWriteConnection(Option<PoolConnection<Sqlite>>);

impl NoteWriteConnection {
    pub(crate) async fn begin(store: &Store) -> Result<Self> {
        Self::begin_pool(store.write_pool()).await
    }

    pub(crate) async fn begin_pool(pool: &crate::StorePool) -> Result<Self> {
        let conn = pool
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("acquire connection failed: {e}")))?;
        let mut guard = Self(Some(conn));
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *guard)
            .await
            .map_err(|e| Error::Internal(format!("begin IMMEDIATE failed: {e}")))?;
        Ok(guard)
    }

    pub(crate) async fn finish<T>(mut self, result: Result<T>, context: &str) -> Result<T> {
        let error = match result {
            Ok(value) => match sqlx::query("COMMIT").execute(&mut *self).await {
                Ok(_) => {
                    drop(self.0.take());
                    return Ok(value);
                }
                Err(error) => Error::Internal(format!("{context}: {error}")),
            },
            Err(error) => error,
        };
        if let Err(rollback_err) = sqlx::query("ROLLBACK").execute(&mut *self).await {
            if let Some(conn) = self.0.take() {
                match conn.detach().close().await {
                    Ok(()) => tracing::warn!(
                        rollback_error = %rollback_err,
                        "ROLLBACK failed; detached and closed the potentially poisoned write-pool connection"
                    ),
                    Err(close_err) => tracing::warn!(
                        rollback_error = %rollback_err,
                        close_error = %close_err,
                        "ROLLBACK failed; detached the potentially poisoned write-pool connection but close also failed"
                    ),
                }
            }
        } else {
            drop(self.0.take());
        }
        Err(error)
    }
}

impl Deref for NoteWriteConnection {
    type Target = SqliteConnection;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("active note writer")
    }
}

impl DerefMut for NoteWriteConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("active note writer")
    }
}

impl Drop for NoteWriteConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.0.as_mut() {
            conn.close_on_drop();
        }
    }
}
