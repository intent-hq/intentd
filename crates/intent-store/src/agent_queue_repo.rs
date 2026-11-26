//! Agent queue repository: durable write-through snapshots of the per-agent
//! in-memory send queue (migration 0046). Each mutation replaces the agent's
//! whole persisted queue in one transaction — simple and race-safe under the
//! single-connection write pool. Inserts are chunked multi-row statements
//! (intent-hq/monorepo#3540 — the write-through used to insert one row per
//! statement, so every send against an N-entry queue cost O(N) statements),
//! keeping a replace at O(1) statements for any plausible queue size. Rows
//! cascade with their `agent_session` row (workspace/agent delete needs no
//! explicit cleanup).
//!
//! Several processes may share one Postgres tenant: [`Store::sync_agent_queue`]
//! and [`Store::refresh_agent_queue`] keep each process's in-memory queue a
//! cache of the stored one (see `agent_queue_merge`).

use intent_core::{AgentId, Error, Result};
use sqlx::{Row, Sqlite, Transaction};

use crate::Store;

/// Columns inserted by the bulk queue writes, in bind order.
const QUEUE_COLUMNS: &str = "id, agent_id, position, payload, created_at, turn_id";

/// Rows per bulk INSERT chunk. 4096 rows × 6 binds = 24576, well under the
/// bundled `SQLite`'s `SQLITE_MAX_VARIABLE_NUMBER` (32766 since 3.32); one
/// chunk covers any plausible queue, so the statement count stays flat.
const ROWS_PER_STATEMENT: usize = 4096;

/// One encoded queue row ready to bind: `(id, position, payload_json,
/// created_at, turn_id)` — the owning `agent_id` is bound by the caller.
type EncodedRow = (String, i64, String, String, String);

/// JSON-encode queue rows for binding, failing fast on an unencodable payload.
fn encode_rows(rows: &[AgentQueueRow]) -> Result<Vec<EncodedRow>> {
    rows.iter()
        .map(|r| {
            serde_json::to_string(&r.payload)
                .map(|payload| {
                    (
                        r.id.clone(),
                        r.position,
                        payload,
                        r.created_at.clone(),
                        r.turn_id.clone(),
                    )
                })
                .map_err(|e| Error::Internal(format!("encode agent queue payload failed: {e}")))
        })
        .collect()
}

/// Insert the encoded rows under `agent_id` in chunked multi-row statements
/// within the caller's transaction.
async fn insert_rows(
    tx: &mut Transaction<'_, Sqlite>,
    agent_id: &AgentId,
    rows: &[EncodedRow],
) -> Result<()> {
    let binds_per_row = QUEUE_COLUMNS.split(',').count();
    let row_placeholders = format!("({})", vec!["?"; binds_per_row].join(","));
    for chunk in rows.chunks(ROWS_PER_STATEMENT) {
        let placeholders = vec![row_placeholders.as_str(); chunk.len()].join(",");
        let sql = format!("INSERT INTO agent_queue ({QUEUE_COLUMNS}) VALUES {placeholders}");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        for (id, position, payload, created_at, turn_id) in chunk {
            query = query
                .bind(id)
                .bind(&agent_id.0)
                .bind(position)
                .bind(payload)
                .bind(created_at)
                .bind(turn_id);
        }
        query
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Internal(format!("bulk insert agent queue failed: {e}")))?;
    }
    Ok(())
}

/// One persisted queue entry. `payload` is the full internal `QueuedMessage`
/// JSON (owned by `intent-services`); the store treats it as opaque.
#[derive(Debug, Clone)]
pub struct AgentQueueRow {
    pub id: String,
    pub agent_id: AgentId,
    /// 0-based index in the agent's queue at snapshot time (0 = next to send).
    pub position: i64,
    pub payload: serde_json::Value,
    pub created_at: String,
    /// Turn correlation id (monorepo#1022): stable across terminal-failure
    /// requeues. Legacy rows (NULL column) load as the row `id`.
    pub turn_id: String,
}

/// The outcome of [`Store::sync_agent_queue`].
#[derive(Debug, Clone, Default)]
pub struct AgentQueueSync {
    /// The queue written when another process had changed the stored queue
    /// since this process last read or wrote it: this process's change
    /// merged onto the stored queue ([`crate::merge_agent_queue`]). The
    /// caller's memory must be refreshed from it. `None` when `rows` were
    /// written as given.
    pub merged: Option<Vec<AgentQueueRow>>,
    /// The claimed ids another process removed from the stored queue first
    /// ([`crate::lost_agent_queue_claims`]): the caller must not deliver them.
    pub lost: Vec<String>,
}

impl Store {
    /// Replace the persisted queue for one agent with the given snapshot
    /// (delete-then-bulk-insert in a single transaction, O(1) statements).
    /// An empty `rows` slice clears the agent's persisted queue. Every row
    /// must belong to `agent_id` — a mismatch fails fast instead of silently
    /// persisting rows under the wrong agent.
    ///
    /// On Postgres the replace is a compare-and-swap on the agent's queue
    /// version (batch G 2e): it fails `Error::Conflict` and writes nothing
    /// when another process persisted the queue since this process last
    /// loaded ([`Store::load_all_agent_queues`]) or wrote it, so an older
    /// snapshot never replaces a newer queue. `SQLite` has one writer.
    ///
    /// # Errors
    ///
    /// Returns `Error::Conflict` (Postgres) for an out-of-date snapshot, and
    /// `Error::Internal` if the database operation fails.
    pub async fn replace_agent_queue(
        &self,
        agent_id: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        dispatch!(self, backend => backend.replace_agent_queue(agent_id, rows).await)
    }

    /// Write this process's queue for one agent so that no other process's
    /// change is overwritten. Where [`Store::replace_agent_queue`] refuses an
    /// out-of-date snapshot, this merges it: one write transaction (under the
    /// tenant writer lock on Postgres) reads the stored queue version; when
    /// it is the version this process last read or wrote, `rows` replace the
    /// queue as is; otherwise this process's change since then is merged
    /// onto the stored rows ([`crate::merge_agent_queue`]) and the result,
    /// written, comes back in [`AgentQueueSync::merged`].
    ///
    /// `claims` are ids the caller removed from `rows` to deliver them (a
    /// drain). Any of them another process removed from the stored queue
    /// first comes back in [`AgentQueueSync::lost`]; exactly one of two
    /// processes draining one entry keeps it.
    ///
    /// `SQLite` has one writer, so this is [`Store::replace_agent_queue`]:
    /// never merged, nothing lost.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails; nothing is
    /// recorded then, so the next sync applies the same change.
    pub async fn sync_agent_queue(
        &self,
        agent_id: &AgentId,
        rows: &[AgentQueueRow],
        claims: &[String],
    ) -> Result<AgentQueueSync> {
        match &self.backend {
            crate::Backend::Sqlite(backend) => {
                let _ = claims;
                backend.replace_agent_queue(agent_id, rows).await?;
                Ok(AgentQueueSync::default())
            }
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(backend) => {
                backend.sync_agent_queue(agent_id, rows, claims).await
            }
        }
    }

    /// [`Store::sync_agent_queue`] for the holder of `claim` (a delivery
    /// claim): the write guards the claim's lease in its own transaction,
    /// so once the lease is lost it fails with `Error::LeaseLost` and writes
    /// nothing. `SQLite` checks the lease, then replaces the queue (one
    /// process: see `SqliteStore::guard_lease`).
    ///
    /// # Errors
    ///
    /// `Error::LeaseLost` when the claim is no longer held; otherwise as
    /// [`Store::sync_agent_queue`].
    pub async fn sync_agent_queue_guarded(
        &self,
        agent_id: &AgentId,
        rows: &[AgentQueueRow],
        claims: &[String],
        claim: &crate::LeaseKeeper,
    ) -> Result<AgentQueueSync> {
        let held = claim.lease().await;
        let lease = crate::completion_watch_repo::held_claim(&held, claim)?;
        match &self.backend {
            crate::Backend::Sqlite(backend) => {
                let _ = claims;
                backend.guard_lease(lease)?;
                backend.replace_agent_queue(agent_id, rows).await?;
                Ok(AgentQueueSync::default())
            }
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(backend) => {
                backend
                    .sync_agent_queue_guarded(agent_id, rows, claims, Some(lease))
                    .await
            }
        }
    }

    /// Read-only refresh of this process's view of one agent's queue: when
    /// another process wrote the queue since this process last read or wrote
    /// it, returns `local` (this process's queue now) with the stored change
    /// merged in ([`crate::merge_agent_queue`]), and remembers the stored
    /// queue as read. `None` when nothing changed, and always on `SQLite`
    /// (one writer: memory is current).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unused_async,
            reason = "only the Postgres backend awaits; the signature is shared"
        )
    )]
    pub async fn refresh_agent_queue(
        &self,
        agent_id: &AgentId,
        local: &[AgentQueueRow],
    ) -> Result<Option<Vec<AgentQueueRow>>> {
        match &self.backend {
            crate::Backend::Sqlite(_) => {
                let _ = (agent_id, local);
                Ok(None)
            }
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(backend) => backend.refresh_agent_queue(agent_id, local).await,
        }
    }

    /// Whether other processes may write this store's agent queues
    /// (Postgres), so a queue read or drain must consult the store first.
    #[must_use]
    pub fn agent_queues_shared(&self) -> bool {
        match &self.backend {
            crate::Backend::Sqlite(_) => false,
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(_) => true,
        }
    }

    /// Atomically move the persisted queue between two agents: ONE write
    /// transaction deletes the rows of BOTH agents and inserts `rows` under
    /// `to` (an empty `rows` slice just clears both). Backs the
    /// poisoned-session queue migration (monorepo#847): migrated entries
    /// keep their ids and `agent_queue.id` is a global primary key, so a
    /// non-atomic clear-then-replace pair risks either a PK-conflict
    /// rollback or a crash window where the messages are durable on
    /// NEITHER queue — this op commits the hand-off as a single unit.
    /// Every row must belong to `to`; a mismatch fails fast. On Postgres
    /// both queues are compared and swapped as in
    /// [`Store::replace_agent_queue`].
    ///
    /// # Errors
    ///
    /// Returns `Error::Conflict` (Postgres) for an out-of-date queue, and
    /// `Error::Internal` if the database operation fails.
    pub async fn move_agent_queue(
        &self,
        from: &AgentId,
        to: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        dispatch!(self, backend => backend.move_agent_queue(from, to, rows).await)
    }

    /// Load every persisted queue entry, ordered by agent then queue position,
    /// for startup rehydration. Joined against `agent_session` so entries
    /// whose session row no longer exists are skipped (defensive; the FK
    /// cascade should already have removed them). A row whose stored payload
    /// is not valid JSON comes back as `Value::Null` rather than failing the
    /// whole load — rehydration is best-effort and the caller skips entries
    /// it cannot decode. Legacy rows with a NULL `turn_id` (pre-monorepo#1022)
    /// load with `turn_id` defaulted to the row `id`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn load_all_agent_queues(&self) -> Result<Vec<AgentQueueRow>> {
        dispatch!(self, backend => backend.load_all_agent_queues().await)
    }
}

impl crate::SqliteStore {
    pub async fn replace_agent_queue(
        &self,
        agent_id: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        if let Some(row) = rows.iter().find(|r| r.agent_id != *agent_id) {
            return Err(Error::Internal(format!(
                "replace agent queue row {} belongs to agent {}, not {}",
                row.id, row.agent_id.0, agent_id.0
            )));
        }
        let pool = self.write_pool();
        let agent_id = agent_id.clone();
        let owned = encode_rows(rows)?;

        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("replace agent queue begin failed: {e}")))?;
            sqlx::query("DELETE FROM agent_queue WHERE agent_id = ?")
                .bind(&agent_id.0)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("replace agent queue clear failed: {e}")))?;
            insert_rows(&mut tx, &agent_id, &owned).await?;
            tx.commit()
                .await
                .map_err(|e| Error::Internal(format!("replace agent queue commit failed: {e}")))?;
            Ok(())
        })
        .await
    }

    pub async fn move_agent_queue(
        &self,
        from: &AgentId,
        to: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        if let Some(row) = rows.iter().find(|r| r.agent_id != *to) {
            return Err(Error::Internal(format!(
                "move agent queue row {} belongs to agent {}, not {}",
                row.id, row.agent_id.0, to.0
            )));
        }
        let pool = self.write_pool();
        let from = from.clone();
        let to = to.clone();
        let owned = encode_rows(rows)?;

        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("move agent queue begin failed: {e}")))?;
            sqlx::query("DELETE FROM agent_queue WHERE agent_id IN (?, ?)")
                .bind(&from.0)
                .bind(&to.0)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("move agent queue clear failed: {e}")))?;
            insert_rows(&mut tx, &to, &owned).await?;
            tx.commit()
                .await
                .map_err(|e| Error::Internal(format!("move agent queue commit failed: {e}")))?;
            Ok(())
        })
        .await
    }

    pub async fn load_all_agent_queues(&self) -> Result<Vec<AgentQueueRow>> {
        let rows = sqlx::query(
            "SELECT q.id, q.agent_id, q.position, q.payload, q.created_at, \
                    COALESCE(q.turn_id, q.id) AS turn_id \
             FROM agent_queue q JOIN agent_session s ON s.id = q.agent_id \
             WHERE NOT EXISTS(SELECT 1 FROM agent_queue_admission d WHERE d.id=q.id AND d.phase='completed') \
             UNION ALL SELECT d.id,d.agent_id,-1,d.payload,d.created_at,d.turn_id
             FROM agent_queue_admission d JOIN agent_session a ON a.id=d.agent_id
             WHERE d.phase != 'completed' AND NOT EXISTS(SELECT 1 FROM agent_queue q WHERE q.id=d.id)
             ORDER BY agent_id, position",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("load agent queues failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|row| {
                let payload_raw: String = row.get("payload");
                let payload = serde_json::from_str(&payload_raw).unwrap_or(serde_json::Value::Null);
                AgentQueueRow {
                    id: row.get("id"),
                    agent_id: AgentId(row.get("agent_id")),
                    position: row.get("position"),
                    payload,
                    created_at: row.get("created_at"),
                    turn_id: row.get("turn_id"),
                }
            })
            .collect())
    }

    /// Delete every persisted queue entry for one agent.
    #[cfg(test)]
    pub(crate) async fn delete_agent_queue(&self, agent_id: &AgentId) -> Result<()> {
        sqlx::query("DELETE FROM agent_queue WHERE agent_id = ?")
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete agent queue failed: {e}")))?;
        Ok(())
    }
}
