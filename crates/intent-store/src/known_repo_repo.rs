//! Known-repository registry (§9.2). Backs the `repo.list` method: a persistent
//! set of repos that survives workspace deletion. Mirrors the TS electron-store
//! `repo-registry` (`getAllRepos`/`addRepo`/`syncRepos`).

use intent_core::{now_iso, Error, KnownRepo, Result};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

const KNOWN_REPO_COLUMNS: &str = "path, name, owner, added_at, last_used_at";

impl Store {
    /// List every known repo, most-recently-used first (`last_used_at` DESC),
    /// mirroring TS `getAllRepos()`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_known_repos(&self) -> Result<Vec<KnownRepo>> {
        let sql = format!("SELECT {KNOWN_REPO_COLUMNS} FROM known_repo ORDER BY last_used_at DESC");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list known repos failed: {e}")))?;
        rows.iter().map(map_known_repo_row).collect()
    }

    /// Insert or update a known repo, keyed on `path` (TS `addRepo`). A new row
    /// is inserted with `added_at = last_used_at = now`. On conflict the
    /// `last_used_at` is bumped and `name`/`owner` are overwritten when a
    /// non-empty `name`/`Some(owner)` is supplied (else the existing value is
    /// kept). Idempotent on `path`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_known_repo(
        &self,
        path: &str,
        name: &str,
        owner: Option<&str>,
    ) -> Result<()> {
        let now = now_iso();
        sqlx::query(
            "INSERT INTO known_repo (path, name, owner, added_at, last_used_at) \
             VALUES (?,?,?,?,?) \
             ON CONFLICT(path) DO UPDATE SET \
                 name = CASE WHEN excluded.name != '' THEN excluded.name ELSE known_repo.name END, \
                 owner = COALESCE(excluded.owner, known_repo.owner), \
                 last_used_at = excluded.last_used_at",
        )
        .bind(path)
        .bind(name)
        .bind(owner)
        .bind(&now)
        .bind(&now)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert known repo failed: {e}")))?;
        Ok(())
    }

    /// Delete a known repo by `path` (TS `removeRepo`). Returns whether a row
    /// was actually removed; removing an unregistered path is not an error.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn remove_known_repo(&self, path: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM known_repo WHERE path = ?")
            .bind(path)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("remove known repo failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}

fn map_known_repo_row(r: &SqliteRow) -> Result<KnownRepo> {
    Ok(KnownRepo {
        path: r.get("path"),
        name: r.get("name"),
        owner: r.get("owner"),
        added_at: r.get("added_at"),
        last_used_at: r.get("last_used_at"),
    })
}
