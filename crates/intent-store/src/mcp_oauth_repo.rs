//! MCP OAuth token repository (PROTOCOL §5.22 companion). The
//! `mcp_oauth_tokens` table is a `server_id` → JSON `token_bag` store used by
//! the `mcp.oauth.*` RPC family; every bag is treated as secret material and
//! the repository never inspects the JSON body — typing, presence-only wire
//! shape, and redaction are the `services::mcp_oauth` concern.

use intent_core::{Error, Result};
use sqlx::Row;

use crate::Store;

impl Store {
    /// Fetch the raw JSON token bag for `server_id`, or `None` when unset.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_mcp_oauth_token(&self, server_id: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT token_bag FROM mcp_oauth_tokens WHERE server_id = ?")
            .bind(server_id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get mcp oauth token failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>("token_bag")))
    }

    /// Upsert the raw JSON `token_bag` for `server_id`, replacing any prior bag
    /// and stamping `updated_at` to `now_iso` (caller-supplied).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn set_mcp_oauth_token(
        &self,
        server_id: &str,
        token_bag: &str,
        updated_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO mcp_oauth_tokens (server_id, token_bag, updated_at) \
             VALUES (?,?,?) \
             ON CONFLICT(server_id) DO UPDATE SET \
             token_bag = excluded.token_bag, updated_at = excluded.updated_at",
        )
        .bind(server_id)
        .bind(token_bag)
        .bind(updated_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set mcp oauth token failed: {e}")))?;
        Ok(())
    }

    /// Delete the persisted bag for `server_id`. Returns `true` when a row was
    /// removed; absence is an idempotent no-op success.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_mcp_oauth_token(&self, server_id: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM mcp_oauth_tokens WHERE server_id = ?")
            .bind(server_id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete mcp oauth token failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// List every `server_id` with a stored bag, sorted ascending for a stable
    /// wire order. Values are never returned; use `get_mcp_oauth_token` for
    /// internal (server-side) reads.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_mcp_oauth_server_ids(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT server_id FROM mcp_oauth_tokens ORDER BY server_id")
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list mcp oauth tokens failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("server_id"))
            .collect())
    }
}
