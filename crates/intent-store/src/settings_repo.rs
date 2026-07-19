//! Settings repository (§9.2 / §9.8). The `settings` table is a flat
//! `key` → JSON `value` store for **non-secret** BE-owned settings; sensitive
//! values (§9.8) never reach this table (they live in the OS keychain). The
//! repository deals only in raw JSON-encoded `value` strings — typing,
//! validation, defaults, and redaction are the `services::settings` concern.

use intent_core::{Error, Result};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

impl Store {
    /// Fetch the raw JSON-encoded value for `key`, or `None` when unset.
    pub async fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT value FROM settings WHERE key = ?")
            .bind(key)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get setting failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>("value")))
    }

    /// Upsert the raw JSON-encoded `value` for `key`, replacing any prior value.
    pub async fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES (?,?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set setting failed: {e}")))?;
        Ok(())
    }

    /// Delete the persisted value for `key` (used by `settings.reset`). Returns
    /// `true` when a row was removed; absence is an idempotent no-op success.
    pub async fn delete_setting(&self, key: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM settings WHERE key = ?")
            .bind(key)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete setting failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// List every persisted `(key, value)` pair (raw JSON `value` strings).
    pub async fn list_settings(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query("SELECT key, value FROM settings ORDER BY key")
            .fetch_all(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("list settings failed: {e}")))?;
        Ok(rows.iter().map(map_setting_row).collect())
    }
}

fn map_setting_row(r: &SqliteRow) -> (String, String) {
    (r.get("key"), r.get("value"))
}
