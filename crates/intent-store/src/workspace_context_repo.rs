//! Per-workspace chat context items (§5.1 `workspace.getContext` /
//! `updateContext`). The store treats each item's payload as an opaque
//! JSON blob authored by the FE (`ContextItem` union in
//! `packages/cloudlands-fe/src/features/context/types.ts`); the row pulls
//! `id` out for keying and `ordinal` for stable insertion-order iteration.

use intent_core::{ContextItem, Error, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

impl Store {
    /// List context items for a workspace, ordered by insertion (`ordinal`).
    /// Returns an empty vec when nothing is stored yet.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_workspace_context_items(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ContextItem>> {
        let rows = sqlx::query(
            "SELECT id, payload FROM workspace_context_item \
             WHERE workspace_id = ? ORDER BY ordinal",
        )
        .bind(&workspace_id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list workspace context items failed: {e}")))?;
        rows.iter().map(map_context_row).collect()
    }

    /// Replace the entire context item list for a workspace atomically —
    /// the FE's `hydrateContextItems` / add / remove / update collapse to
    /// a single "here is the new list" write, so the daemon persists the
    /// caller-supplied ordering (assigning `ordinal` positionally).
    /// Returns the persisted list read back from the store so callers can
    /// forward it verbatim to `workspace:context-changed` subscribers.
    ///
    /// Uses whole-transaction retry to eliminate `SQLITE_BUSY` (code 5) failures
    /// during lock upgrade under concurrent load (STAB-7).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn replace_workspace_context_items(
        &self,
        workspace_id: &WorkspaceId,
        items: &[ContextItem],
    ) -> Result<Vec<ContextItem>> {
        let pool = self.write_pool();
        let workspace_id = workspace_id.clone();
        let items = items.to_vec();

        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("replace context tx failed: {e}")))?;
            sqlx::query("DELETE FROM workspace_context_item WHERE workspace_id = ?")
                .bind(&workspace_id.0)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("clear workspace context failed: {e}")))?;
            for (idx, item) in items.iter().enumerate() {
                let payload = serde_json::to_string(item)
                    .map_err(|e| Error::Internal(format!("encode context item failed: {e}")))?;
                sqlx::query(
                    "INSERT INTO workspace_context_item \
                     (workspace_id, id, ordinal, payload) VALUES (?, ?, ?, ?)",
                )
                .bind(&workspace_id.0)
                .bind(&item.id)
                .bind(i64::try_from(idx).unwrap_or(i64::MAX))
                .bind(payload)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("insert context item failed: {e}")))?;
            }
            tx.commit()
                .await
                .map_err(|e| Error::Internal(format!("replace context commit failed: {e}")))?;
            Ok(())
        })
        .await?;

        self.list_workspace_context_items(&workspace_id).await
    }
}

fn map_context_row(r: &SqliteRow) -> Result<ContextItem> {
    let id: String = r.get("id");
    let payload: String = r.get("payload");
    let mut item: ContextItem = serde_json::from_str(&payload)
        .map_err(|e| Error::Internal(format!("decode context item failed: {e}")))?;
    // Defensive: keep the row `id` authoritative even if the payload
    // roundtripped without it (should never happen — we validate at write).
    item.id = id;
    Ok(item)
}
