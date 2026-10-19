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

    /// Acquire the writer and open `BEGIN IMMEDIATE`. Only the acquire is
    /// retried: a write-pool acquire timeout happens before any statement, so
    /// waiting it out cannot double-apply a mutation (intent-hq/intent#7070).
    ///
    /// The acquire phase (every attempt plus backoff) has a hard budget of
    /// twice the pool acquire timeout (20s in production). No attempt can
    /// start BEGIN after the deadline: each attempt is bounded by it, and a
    /// connection handed back late is checked again before BEGIN. This bounds
    /// only the wait for the writer. It does not cover the transaction's own
    /// SQL, any work the caller does before or after, or earlier outer retry
    /// attempts. When the budget runs out, nothing has been applied, and the
    /// error is not classified as transient, so an outer retry loop does not
    /// start another acquire phase.
    pub(crate) async fn begin_pool(pool: &crate::StorePool) -> Result<Self> {
        let budget = pool.options().get_acquire_timeout().saturating_mul(2);
        let deadline = tokio::time::Instant::now() + budget;
        let conn = crate::with_busy_retry(
            || async {
                match tokio::time::timeout_at(deadline, pool.acquire()).await {
                    Ok(acquired) => acquired
                        .map_err(|e| Error::Internal(format!("acquire connection failed: {e}"))),
                    // Report the cut-short final attempt as an ordinary pool
                    // timeout; the loop sees the budget spent and stops.
                    Err(_) => Err(Error::Internal(format!(
                        "acquire connection failed: {}",
                        sqlx::Error::PoolTimedOut
                    ))),
                }
            },
            budget,
        )
        .await
        .map_err(|error| match error {
            Error::Internal(msg) if crate::is_pool_timeout_message(&msg) => {
                writer_unavailable(budget)
            }
            other => other,
        })?;
        // `Timeout` polls the inner acquire before its timer, so a connection
        // can be accepted after the deadline when the task is scheduled late.
        let conn = admit_before_deadline(conn, tokio::time::Instant::now(), deadline, budget)?;
        let mut guard = Self(Some(conn));
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *guard)
            .await
            .map_err(|e| Error::Internal(format!("begin IMMEDIATE failed: {e}")))?;
        Ok(guard)
    }

    /// Only pool acquisition is timed. Once BEGIN is sent, its physical `SQLite`
    /// work and the subsequent transaction stay owned until explicit settlement.
    pub(crate) async fn begin_before(
        store: &Store,
        deadline: tokio::time::Instant,
    ) -> Result<Self> {
        let conn = tokio::time::timeout_at(deadline, store.write_pool().acquire())
            .await
            .map_err(|_| Error::NoteDelete(intent_core::note_delete::NoteDeleteError::Unavailable))?
            .map_err(|e| Error::Internal(format!("acquire note delete writer: {e}")))?;
        let mut guard = Self(Some(conn));
        if let Err(error) = sqlx::query("BEGIN IMMEDIATE").execute(&mut *guard).await {
            return guard
                .finish(
                    Err(Error::Internal(format!(
                        "begin note delete writer: {error}"
                    ))),
                    "begin note delete writer",
                )
                .await;
        }
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

/// Non-transient "nothing was applied" failure for an exhausted acquire budget.
fn writer_unavailable(budget: std::time::Duration) -> Error {
    Error::Internal(format!(
        "acquire connection failed: note writer unavailable for {}ms \
         (write pool saturated); nothing was applied",
        budget.as_millis()
    ))
}

/// Refuse a writer obtained at or after the acquire deadline. Dropping it
/// returns the idle connection to the pool before any statement is sent.
fn admit_before_deadline<T>(
    conn: T,
    now: tokio::time::Instant,
    deadline: tokio::time::Instant,
    budget: std::time::Duration,
) -> Result<T> {
    if now >= deadline {
        drop(conn);
        return Err(writer_unavailable(budget));
    }
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::admit_before_deadline;

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn late_writer_is_refused_and_released_before_begin() {
        let budget = Duration::from_secs(20);
        let deadline = tokio::time::Instant::now();
        for now in [deadline, deadline + Duration::from_millis(1)] {
            let dropped = Arc::new(AtomicBool::new(false));
            let err = admit_before_deadline(DropFlag(dropped.clone()), now, deadline, budget)
                .err()
                .expect("a writer at or past the deadline must be refused");
            assert!(err.to_string().contains("nothing was applied"), "{err}");
            assert!(
                dropped.load(Ordering::SeqCst),
                "the late writer is released"
            );
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let admitted = admit_before_deadline(
            DropFlag(dropped.clone()),
            deadline - Duration::from_millis(1),
            deadline,
            budget,
        );
        assert!(admitted.is_ok(), "an in-budget writer is admitted");
        assert!(!dropped.load(Ordering::SeqCst));
    }
}
