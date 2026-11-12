//! Local pairing credentials (§5.58, migration 0191).
//!
//! One row per successful local code redemption. Only the token's hex SHA-256
//! is stored. This is not a wire authorization boundary: the transport
//! enforces loopback-only admission on `/ws` and the owner-only methods.
use crate::Store;
use intent_core::{now_iso, Error, Result};
use sqlx::{sqlite::SqliteRow, Row};

const COLUMNS: &str = "id, app_name, created_at, last_used_at, revoked_at";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("local pairing persistence: {error}"))
}

/// One stored local pairing credential (never its token or hash).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPairingCredentialRow {
    pub id: String,
    pub app_name: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub revoked_at: Option<String>,
}

/// What [`Store::revoke_local_pairing_credential`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalPairingRevocation {
    /// The credential was active and is now revoked.
    Revoked,
    /// The credential was already revoked; nothing changed.
    AlreadyRevoked,
    /// No credential has this id.
    NotFound,
}

fn row(row: &SqliteRow) -> LocalPairingCredentialRow {
    LocalPairingCredentialRow {
        id: row.get("id"),
        app_name: row.get("app_name"),
        created_at: row.get("created_at"),
        last_used_at: row.get("last_used_at"),
        revoked_at: row.get("revoked_at"),
    }
}

impl Store {
    /// Record a newly minted local pairing credential keyed by `token_hash`
    /// (hex SHA-256 of the token, never the token itself).
    ///
    /// # Errors
    ///
    /// `InvalidParams` for an empty id, hash or app name; `Internal` on
    /// database failure (including a duplicate id or hash).
    pub async fn insert_local_pairing_credential(
        &self,
        id: &str,
        token_hash: &str,
        app_name: &str,
    ) -> Result<LocalPairingCredentialRow> {
        if id.is_empty() || token_hash.is_empty() || app_name.is_empty() {
            return Err(Error::InvalidParams(
                "invalid local pairing credential".into(),
            ));
        }
        let now = now_iso();
        sqlx::query(
            "INSERT INTO local_pairing_credential (id, token_hash, app_name, created_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(id)
        .bind(token_hash)
        .bind(app_name)
        .bind(&now)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(LocalPairingCredentialRow {
            id: id.to_owned(),
            app_name: app_name.to_owned(),
            created_at: now,
            last_used_at: None,
            revoked_at: None,
        })
    }

    /// Unrevoked credentials ordered by `created_at`, then `id`.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn list_local_pairing_credentials(&self) -> Result<Vec<LocalPairingCredentialRow>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM local_pairing_credential WHERE revoked_at IS NULL \
             ORDER BY created_at, id"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        Ok(rows.iter().map(row).collect())
    }

    /// One credential by id, revoked or not.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn get_local_pairing_credential(
        &self,
        id: &str,
    ) -> Result<Option<LocalPairingCredentialRow>> {
        let sql = format!("SELECT {COLUMNS} FROM local_pairing_credential WHERE id = ?");
        let found = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id)
            .fetch_optional(self.write_pool())
            .await
            .map_err(db)?;
        Ok(found.as_ref().map(row))
    }

    /// Resolve an **unrevoked** credential by token hash and bump
    /// `last_used_at` in one statement, so a concurrent revoke cannot slip
    /// between lookup and touch. `None` for an unknown or revoked hash.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn resolve_local_pairing_credential(
        &self,
        token_hash: &str,
    ) -> Result<Option<String>> {
        let found = sqlx::query(
            "UPDATE local_pairing_credential SET last_used_at = ? \
             WHERE token_hash = ? AND revoked_at IS NULL RETURNING id",
        )
        .bind(now_iso())
        .bind(token_hash)
        .fetch_optional(self.write_pool())
        .await
        .map_err(db)?;
        Ok(found.map(|r| r.get::<String, _>("id")))
    }

    /// Revoke one credential by id.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn revoke_local_pairing_credential(
        &self,
        id: &str,
    ) -> Result<LocalPairingRevocation> {
        let flipped = sqlx::query(
            "UPDATE local_pairing_credential SET revoked_at = ? \
             WHERE id = ? AND revoked_at IS NULL",
        )
        .bind(now_iso())
        .bind(id)
        .execute(self.write_pool())
        .await
        .map_err(db)?
        .rows_affected();
        if flipped > 0 {
            return Ok(LocalPairingRevocation::Revoked);
        }
        Ok(match self.get_local_pairing_credential(id).await? {
            Some(_) => LocalPairingRevocation::AlreadyRevoked,
            None => LocalPairingRevocation::NotFound,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn open() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("local-pairing.db"))
            .await
            .unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn insert_resolve_list_and_revoke() {
        let (_dir, store) = open().await;
        let a = store
            .insert_local_pairing_credential("a", "hash-a", "Intent Alpha")
            .await
            .unwrap();
        store
            .insert_local_pairing_credential("b", "hash-b", "Intent")
            .await
            .unwrap();
        assert_eq!(a.last_used_at, None);
        assert_eq!(
            store
                .resolve_local_pairing_credential("hash-a")
                .await
                .unwrap()
                .as_deref(),
            Some("a")
        );
        assert!(store
            .get_local_pairing_credential("a")
            .await
            .unwrap()
            .unwrap()
            .last_used_at
            .is_some());
        assert_eq!(
            store
                .resolve_local_pairing_credential("nope")
                .await
                .unwrap(),
            None
        );
        let listed: Vec<_> = store
            .list_local_pairing_credentials()
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&"a".to_owned()) && listed.contains(&"b".to_owned()));

        assert_eq!(
            store.revoke_local_pairing_credential("a").await.unwrap(),
            LocalPairingRevocation::Revoked
        );
        assert_eq!(
            store.revoke_local_pairing_credential("a").await.unwrap(),
            LocalPairingRevocation::AlreadyRevoked
        );
        assert_eq!(
            store.revoke_local_pairing_credential("zzz").await.unwrap(),
            LocalPairingRevocation::NotFound
        );
        assert_eq!(
            store
                .resolve_local_pairing_credential("hash-a")
                .await
                .unwrap(),
            None
        );
        let listed = store.list_local_pairing_credentials().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, "b");
    }

    #[tokio::test]
    async fn rejects_empty_fields_and_duplicate_hashes() {
        let (_dir, store) = open().await;
        assert!(store
            .insert_local_pairing_credential("", "h", "n")
            .await
            .is_err());
        assert!(store
            .insert_local_pairing_credential("i", "", "n")
            .await
            .is_err());
        assert!(store
            .insert_local_pairing_credential("i", "h", "")
            .await
            .is_err());
        store
            .insert_local_pairing_credential("i", "h", "n")
            .await
            .unwrap();
        assert!(store
            .insert_local_pairing_credential("j", "h", "n")
            .await
            .is_err());
    }
}
