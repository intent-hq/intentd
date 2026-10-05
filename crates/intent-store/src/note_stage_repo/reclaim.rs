//! Bounded logical reclamation; no VACUUM or physical-size promise.
//!
//! The queue and live-pin triggers in 0153 are part of this implementation.
//! BEGIN IMMEDIATE serializes each tick with begin/seal/commit and cancellation.
//! A tick drains one indexed child batch per operation/root. Parents are deleted
//! only after every child phase is exhausted; queue progress commits atomically.
use crate::Store;
use intent_core::{Error, Result};
use sqlx::{Row, SqliteConnection};

const CHILD_BATCH: i64 = 64;
const STAGE_STEPS: usize = 8;
// All child FK access paths have operation_key as their leading primary-key
// column. Delete leaves before parents, so each cascade is empty, not merely
// bounded by the number of selected parent rows.
const CHILDREN: [(&str, &str); 15] = [
    (
        "note_stage_record",
        "operation_key,stream,chunk_sequence,ordinal",
    ),
    ("note_stage_chunk", "operation_key,stream,sequence"),
    ("note_stage_text_piece", "operation_key,text_id,start"),
    ("note_stage_text", "operation_key,text_id"),
    ("note_stage_view_piece", "operation_key,generation,start"),
    ("note_stage_view", "operation_key,generation"),
    ("note_stage_validation", "operation_key,kind,id"),
    ("note_stage_stream", "operation_key,stream"),
    ("note_operation_item", "operation_key,kind,sequence"),
    ("note_operation_source", "operation_key,phase,start"),
    ("note_operation_text", "operation_key,text_id"),
    ("note_operation_detail", "operation_key,reference,sequence"),
    ("note_operation_reference", "operation_key,reference"),
    ("note_operation_scalar", "operation_key,reference"),
    // The pin has at most one row, but drain it explicitly before stage removal.
    ("note_stage_root_pin", "operation_key"),
];

/// Counts logical rows removed in one bounded transaction, not reclaimed bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoteOperationReclaimStats {
    pub child_rows: u64,
    pub root_pieces: u64,
    pub operations: u64,
    pub roots: u64,
}
fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("note operation reclamation: {error}"))
}

impl Store {
    /// Drain at most 64 operation children and 64 unpinned root pieces.
    /// Retained receipt/replay identity is not removed before retain_until.
    /// # Errors
    /// Invalid timestamps, corrupt queue state or database errors roll back the
    /// entire batch. Dropping the future also drops the writer transaction.
    pub async fn reclaim_note_operations_batch(
        &self,
        now_epoch_ms: u64,
    ) -> Result<NoteOperationReclaimStats> {
        let now = i64::try_from(now_epoch_ms).map_err(db)?;
        crate::with_write_txn_retry(|| self.reclaim_note_operations_once(now)).await
    }

    async fn reclaim_note_operations_once(&self, now: i64) -> Result<NoteOperationReclaimStats> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let stats = reclaim_batch(&mut tx, now).await?;
        tx.commit().await.map_err(db)?;
        Ok(stats)
    }
}

// Caller owns one IMMEDIATE transaction and rolls it back on every error.
async fn reclaim_batch(conn: &mut SqliteConnection, now: i64) -> Result<NoteOperationReclaimStats> {
    if now < 0 {
        return Err(db("negative time"));
    }
    let mut stats = NoteOperationReclaimStats::default();
    reclaim_operation(conn, now, &mut stats).await?;
    reclaim_root(conn, now, &mut stats).await?;
    Ok(stats)
}

async fn reclaim_operation(
    conn: &mut SqliteConnection,
    now: i64,
    stats: &mut NoteOperationReclaimStats,
) -> Result<()> {
    let candidate: Option<(String, i64, i64)> = sqlx::query_as(
        "SELECT operation_key,mode,step FROM note_operation_reclaim WHERE due_ms<=? ORDER BY due_ms,operation_key LIMIT 1")
        .bind(now).fetch_optional(&mut *conn).await.map_err(db)?;
    let Some((key, mode, step)) = candidate else {
        return Ok(());
    };
    let step = usize::try_from(step).map_err(db)?;
    if !matches!(mode, 0 | 1) || step > CHILDREN.len() {
        return Err(db("invalid cleanup cursor"));
    }
    let row = sqlx::query("SELECT o.retain_until,o.outcome,s.phase FROM note_operation o LEFT JOIN note_stage s USING(operation_key) WHERE o.operation_key=?")
        .bind(&key).fetch_one(&mut *conn).await.map_err(db)?;
    let retain_ms = row
        .try_get::<i64, _>("retain_until")
        .map_err(db)?
        .checked_mul(1000)
        .ok_or_else(|| db("retention overflow"))?;
    if mode == 1 && now < retain_ms {
        schedule_full(conn, &key, retain_ms).await?;
        return Ok(());
    }
    if mode == 0 {
        match row
            .try_get::<Option<String>, _>("phase")
            .map_err(db)?
            .as_deref()
        {
            Some("staging" | "sealed") => {
                let state: serde_json::Value =
                    serde_json::from_str(row.try_get("outcome").map_err(db)?).map_err(db)?;
                let deadline = state["expiresAt"]
                    .as_str()
                    .and_then(intent_core::parse_iso)
                    .ok_or_else(|| db("invalid original deadline"))?;
                let deadline_ms =
                    i64::try_from(deadline.unix_timestamp_nanos() / 1_000_000).map_err(db)?;
                if now < deadline_ms {
                    sqlx::query("UPDATE note_operation_reclaim SET due_ms=? WHERE operation_key=?")
                        .bind(deadline_ms)
                        .bind(&key)
                        .execute(&mut *conn)
                        .await
                        .map_err(db)?;
                    return Ok(());
                }
                // Persist the admission fence BEFORE draining; cancelled cleanup
                // can never leave a readable/appendable partially drained view.
                sqlx::query("UPDATE note_stage SET phase='expired' WHERE operation_key=?")
                    .bind(&key)
                    .execute(&mut *conn)
                    .await
                    .map_err(db)?;
                sqlx::query("UPDATE note_operation SET outcome=json_set(outcome,'$.phase','expired') WHERE operation_key=?")
                    .bind(&key).execute(&mut *conn).await.map_err(db)?;
                if step != 0 {
                    return Err(db("active stage has cleanup progress"));
                }
            }
            Some("cancelled" | "expired") => {}
            Some("committed") | None => {
                schedule_full(conn, &key, retain_ms).await?;
                return Ok(());
            }
            _ => return Err(db("unknown stage phase")),
        }
        if step >= STAGE_STEPS {
            schedule_full(conn, &key, retain_ms).await?;
            return Ok(());
        }
    }
    if let Some((table, order)) = CHILDREN.get(step) {
        let removed = drain(conn, table, "operation_key", order, &key).await?;
        stats.child_rows += removed;
        if removed == 0 {
            sqlx::query("UPDATE note_operation_reclaim SET step=step+1 WHERE operation_key=?")
                .bind(&key)
                .execute(&mut *conn)
                .await
                .map_err(db)?;
        }
    } else {
        // All potentially unbounded child sets are empty at this writer snapshot.
        // Remaining stage/queue rows are each unique by operation_key.
        stats.operations += sqlx::query("DELETE FROM note_operation WHERE operation_key=?")
            .bind(&key)
            .execute(&mut *conn)
            .await
            .map_err(db)?
            .rows_affected();
    }
    Ok(())
}

async fn schedule_full(conn: &mut SqliteConnection, key: &str, due: i64) -> Result<()> {
    sqlx::query("UPDATE note_operation_reclaim SET mode=1,step=0,due_ms=? WHERE operation_key=?")
        .bind(due)
        .bind(key)
        .execute(conn)
        .await
        .map_err(db)?;
    Ok(())
}

async fn drain(
    conn: &mut SqliteConnection,
    table: &str,
    scope: &str,
    order: &str,
    key: &str,
) -> Result<u64> {
    // Identifiers come exclusively from constants above, never from requests.
    let sql = format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE {scope}=? ORDER BY {order} LIMIT ?)");
    Ok(sqlx::query(&sql)
        .bind(key)
        .bind(CHILD_BATCH)
        .execute(conn)
        .await
        .map_err(db)?
        .rows_affected())
}

async fn reclaim_root(
    conn: &mut SqliteConnection,
    now: i64,
    stats: &mut NoteOperationReclaimStats,
) -> Result<()> {
    let key: Option<String> = sqlx::query_scalar("SELECT root_key FROM note_stage_root_reclaim WHERE due_ms<=? ORDER BY due_ms,root_key LIMIT 1")
        .bind(now).fetch_optional(&mut *conn).await.map_err(db)?;
    let Some(key) = key else {
        return Ok(());
    };
    let pin: Option<i64> = sqlx::query_scalar("SELECT until_ms FROM note_stage_root_pin WHERE root_key=? AND until_ms>? ORDER BY until_ms DESC LIMIT 1")
        .bind(&key).bind(now).fetch_optional(&mut *conn).await.map_err(db)?;
    if let Some(until) = pin {
        sqlx::query("UPDATE note_stage_root_reclaim SET due_ms=? WHERE root_key=?")
            .bind(until)
            .bind(&key)
            .execute(&mut *conn)
            .await
            .map_err(db)?;
        return Ok(());
    }
    stats.root_pieces = drain(
        conn,
        "note_stage_base_piece",
        "root_key",
        "root_key,start",
        &key,
    )
    .await?;
    if stats.root_pieces != 0 {
        return Ok(());
    }
    // Expired pins are tiny metadata too, but may have unbounded fanout at a root.
    // They remain attached to their stage; root metadata survives until all stage
    // identities expire. Never cascade a root with even an expired stage owner.
    let owner: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM note_stage WHERE root_key=? LIMIT 1")
            .bind(&key)
            .fetch_optional(&mut *conn)
            .await
            .map_err(db)?;
    if owner.is_none() {
        stats.roots = sqlx::query("DELETE FROM note_stage_root WHERE root_key=?")
            .bind(&key)
            .execute(&mut *conn)
            .await
            .map_err(db)?
            .rows_affected();
    } else {
        sqlx::query("DELETE FROM note_stage_root_reclaim WHERE root_key=?")
            .bind(&key)
            .execute(&mut *conn)
            .await
            .map_err(db)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "reclaim_tests.rs"]
mod tests;
