//! Intent apps library and per-app storage (PROTOCOL §5.60, Labs).
//!
//! `intent_app` holds one JSON record per installed app; the service layer
//! (`intent_services::app_ops`) owns its shape. `intent_app_storage` is the
//! durable key-value store behind `app.storage.*`. Keys sort by their UTF-8
//! bytes (`SQLite` BINARY collation, Postgres `COLLATE "C"`).

use intent_core::{Error, Result};
use sqlx::Row;

use crate::Store;

/// The outcome of [`Store::app_storage_set`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppStorageWrite {
    /// The row was written; `used_bytes` is the app's total after the write.
    Written { used_bytes: u64 },
    /// The write would exceed the limit; nothing changed. `used_bytes` is the
    /// app's current total.
    OverQuota { used_bytes: u64 },
}

impl Store {
    /// The record JSON for `app_id`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_app_record(&self, app_id: &str) -> Result<Option<String>> {
        dispatch!(self, backend => backend.get_app_record(app_id).await)
    }

    /// Every record JSON, ordered by app id bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_app_records(&self) -> Result<Vec<String>> {
        dispatch!(self, backend => backend.list_app_records().await)
    }

    /// Insert or replace the record for `app_id`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn put_app_record(
        &self,
        app_id: &str,
        record_json: &str,
        updated_at: &str,
    ) -> Result<()> {
        dispatch!(self, backend => backend.put_app_record(app_id, record_json, updated_at).await)
    }

    /// Delete the record and every storage row of `app_id` in one
    /// transaction. `true` when a record was removed.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_app(&self, app_id: &str) -> Result<bool> {
        dispatch!(self, backend => backend.delete_app(app_id).await)
    }

    /// The stored JSON value for `(app_id, key)`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn app_storage_get(&self, app_id: &str, key: &str) -> Result<Option<String>> {
        dispatch!(self, backend => backend.app_storage_get(app_id, key).await)
    }

    /// Upsert `(app_id, key)` unless the app's total `size_bytes` after the
    /// write would exceed `limit_bytes`; the check and the write share one
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn app_storage_set(
        &self,
        app_id: &str,
        key: &str,
        value_json: &str,
        size_bytes: u64,
        limit_bytes: u64,
    ) -> Result<AppStorageWrite> {
        dispatch!(self, backend => backend.app_storage_set(app_id, key, value_json, size_bytes, limit_bytes).await)
    }

    /// Delete `(app_id, key)`; `true` when a row was removed.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn app_storage_delete(&self, app_id: &str, key: &str) -> Result<bool> {
        dispatch!(self, backend => backend.app_storage_delete(app_id, key).await)
    }

    /// Up to `limit` keys of `app_id` that start with `prefix` and sort
    /// after `after` (exclusive), ordered by UTF-8 bytes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn app_storage_list(
        &self,
        app_id: &str,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>> {
        dispatch!(self, backend => backend.app_storage_list(app_id, prefix, after, limit).await)
    }
}

fn db(context: &'static str) -> impl Fn(sqlx::Error) -> Error {
    move |e| Error::Internal(format!("{context}: {e}"))
}

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// The exclusive upper bound of keys that start with `prefix` under a
/// bytewise order, or `None` when every key from `prefix` up qualifies
/// (empty prefix, or a prefix of only U+10FFFF). Used instead of `LIKE`,
/// which has wildcards and case folding.
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = u32::from(last) + 1;
        if (0xD800..0xE000).contains(&next) {
            next = 0xE000;
        }
        if let Some(c) = char::from_u32(next) {
            chars.push(c);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

impl crate::SqliteStore {
    pub async fn get_app_record(&self, app_id: &str) -> Result<Option<String>> {
        let row = sqlx::query("SELECT record_json FROM intent_app WHERE app_id = ?")
            .bind(app_id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(db("get app record failed"))?;
        Ok(row.map(|r| r.get::<String, _>("record_json")))
    }

    pub async fn list_app_records(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT record_json FROM intent_app ORDER BY app_id")
            .fetch_all(self.read_pool())
            .await
            .map_err(db("list app records failed"))?;
        Ok(rows
            .into_iter()
            .map(|r| r.get::<String, _>("record_json"))
            .collect())
    }

    pub async fn put_app_record(
        &self,
        app_id: &str,
        record_json: &str,
        updated_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO intent_app (app_id, record_json, updated_at) VALUES (?,?,?) \
             ON CONFLICT(app_id) DO UPDATE SET \
             record_json = excluded.record_json, updated_at = excluded.updated_at",
        )
        .bind(app_id)
        .bind(record_json)
        .bind(updated_at)
        .execute(self.write_pool())
        .await
        .map_err(db("put app record failed"))?;
        Ok(())
    }

    pub async fn delete_app(&self, app_id: &str) -> Result<bool> {
        let mut tx = self
            .write_pool()
            .begin()
            .await
            .map_err(db("delete app failed"))?;
        sqlx::query("DELETE FROM intent_app_storage WHERE app_id = ?")
            .bind(app_id)
            .execute(&mut *tx)
            .await
            .map_err(db("delete app storage failed"))?;
        let removed = sqlx::query("DELETE FROM intent_app WHERE app_id = ?")
            .bind(app_id)
            .execute(&mut *tx)
            .await
            .map_err(db("delete app failed"))?
            .rows_affected();
        tx.commit().await.map_err(db("delete app failed"))?;
        Ok(removed > 0)
    }

    pub async fn app_storage_get(&self, app_id: &str, key: &str) -> Result<Option<String>> {
        let row =
            sqlx::query("SELECT value_json FROM intent_app_storage WHERE app_id = ? AND key = ?")
                .bind(app_id)
                .bind(key)
                .fetch_optional(self.read_pool())
                .await
                .map_err(db("app storage get failed"))?;
        Ok(row.map(|r| r.get::<String, _>("value_json")))
    }

    pub async fn app_storage_set(
        &self,
        app_id: &str,
        key: &str,
        value_json: &str,
        size_bytes: u64,
        limit_bytes: u64,
    ) -> Result<AppStorageWrite> {
        let mut tx = self
            .write_pool()
            .begin()
            .await
            .map_err(db("app storage set failed"))?;
        let used: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM intent_app_storage WHERE app_id = ?",
        )
        .bind(app_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db("app storage usage failed"))?;
        let old: Option<i64> = sqlx::query_scalar(
            "SELECT size_bytes FROM intent_app_storage WHERE app_id = ? AND key = ?",
        )
        .bind(app_id)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db("app storage usage failed"))?;
        let used = to_u64(used);
        let after = used
            .saturating_sub(old.map_or(0, to_u64))
            .saturating_add(size_bytes);
        if after > limit_bytes {
            tx.rollback().await.map_err(db("app storage set failed"))?;
            return Ok(AppStorageWrite::OverQuota { used_bytes: used });
        }
        sqlx::query(
            "INSERT INTO intent_app_storage (app_id, key, value_json, size_bytes) VALUES (?,?,?,?) \
             ON CONFLICT(app_id, key) DO UPDATE SET \
             value_json = excluded.value_json, size_bytes = excluded.size_bytes",
        )
        .bind(app_id)
        .bind(key)
        .bind(value_json)
        .bind(to_i64(size_bytes))
        .execute(&mut *tx)
        .await
        .map_err(db("app storage set failed"))?;
        tx.commit().await.map_err(db("app storage set failed"))?;
        Ok(AppStorageWrite::Written { used_bytes: after })
    }

    pub async fn app_storage_delete(&self, app_id: &str, key: &str) -> Result<bool> {
        let removed = sqlx::query("DELETE FROM intent_app_storage WHERE app_id = ? AND key = ?")
            .bind(app_id)
            .bind(key)
            .execute(self.write_pool())
            .await
            .map_err(db("app storage delete failed"))?
            .rows_affected();
        Ok(removed > 0)
    }

    pub async fn app_storage_list(
        &self,
        app_id: &str,
        prefix: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>> {
        let upper = prefix_upper_bound(prefix);
        let rows = sqlx::query(
            "SELECT key FROM intent_app_storage WHERE app_id = ? AND key >= ? \
             AND (? IS NULL OR key < ?) AND (? IS NULL OR key > ?) \
             ORDER BY key LIMIT ?",
        )
        .bind(app_id)
        .bind(prefix)
        .bind(upper.as_deref())
        .bind(upper.as_deref())
        .bind(after)
        .bind(after)
        .bind(i64::from(limit))
        .fetch_all(self.read_pool())
        .await
        .map_err(db("app storage list failed"))?;
        Ok(rows
            .into_iter()
            .map(|r| r.get::<String, _>("key"))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_upper_bound_increments_the_last_char() {
        assert_eq!(prefix_upper_bound(""), None);
        assert_eq!(prefix_upper_bound("ab").as_deref(), Some("ac"));
        assert_eq!(prefix_upper_bound("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_upper_bound("\u{D7FF}").as_deref(), Some("\u{E000}"));
        assert_eq!(prefix_upper_bound("\u{10FFFF}"), None);
    }

    async fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.db")).await.unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn records_round_trip_and_delete_takes_storage() {
        let (_dir, store) = store().await;
        store
            .put_app_record("b", "{\"id\":\"b\"}", "t0")
            .await
            .unwrap();
        store
            .put_app_record("a", "{\"id\":\"a\"}", "t0")
            .await
            .unwrap();
        store
            .put_app_record("a", "{\"id\":\"a2\"}", "t1")
            .await
            .unwrap();
        assert_eq!(
            store.list_app_records().await.unwrap(),
            ["{\"id\":\"a2\"}", "{\"id\":\"b\"}"]
        );
        store.app_storage_set("a", "k", "1", 2, 100).await.unwrap();
        store.app_storage_set("b", "k", "2", 2, 100).await.unwrap();
        assert!(store.delete_app("a").await.unwrap());
        assert!(!store.delete_app("a").await.unwrap());
        assert_eq!(store.get_app_record("a").await.unwrap(), None);
        assert_eq!(store.app_storage_get("a", "k").await.unwrap(), None);
        assert_eq!(
            store.app_storage_get("b", "k").await.unwrap().as_deref(),
            Some("2")
        );
    }

    #[tokio::test]
    async fn storage_quota_counts_the_replaced_row_and_changes_nothing_on_refusal() {
        let (_dir, store) = store().await;
        assert_eq!(
            store
                .app_storage_set("a", "k1", "\"x\"", 6, 10)
                .await
                .unwrap(),
            AppStorageWrite::Written { used_bytes: 6 }
        );
        assert_eq!(
            store
                .app_storage_set("a", "k2", "\"y\"", 6, 10)
                .await
                .unwrap(),
            AppStorageWrite::OverQuota { used_bytes: 6 }
        );
        assert_eq!(store.app_storage_get("a", "k2").await.unwrap(), None);
        // Replacing k1 frees its old size first.
        assert_eq!(
            store
                .app_storage_set("a", "k1", "\"zz\"", 10, 10)
                .await
                .unwrap(),
            AppStorageWrite::Written { used_bytes: 10 }
        );
        assert!(store.app_storage_delete("a", "k1").await.unwrap());
        assert!(!store.app_storage_delete("a", "k1").await.unwrap());
    }

    #[tokio::test]
    async fn storage_list_is_bytewise_prefix_and_cursor_paged() {
        let (_dir, store) = store().await;
        for key in ["b", "a%", "a_", "aB", "ab", "a", "ab\u{e9}", "c"] {
            store
                .app_storage_set("app", key, "0", 1, 1000)
                .await
                .unwrap();
        }
        store
            .app_storage_set("other", "a1", "0", 1, 1000)
            .await
            .unwrap();
        let all = store.app_storage_list("app", "", None, 100).await.unwrap();
        assert_eq!(all, ["a", "a%", "aB", "a_", "ab", "ab\u{e9}", "b", "c"]);
        let a = store.app_storage_list("app", "a", None, 3).await.unwrap();
        assert_eq!(a, ["a", "a%", "aB"]);
        let next = store
            .app_storage_list("app", "a", Some("aB"), 3)
            .await
            .unwrap();
        assert_eq!(next, ["a_", "ab", "ab\u{e9}"]);
        // `%` and `_` are literal, not wildcards.
        assert_eq!(
            store.app_storage_list("app", "a_", None, 10).await.unwrap(),
            ["a_"]
        );
    }
}
