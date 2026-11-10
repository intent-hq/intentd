//! Node invites (migration 0189, monorepo `docs/protocol/methods/nodes.md`,
//! "Node invites and claim"): single-use, expiring invites a backend claims
//! for one executor credential. The secret is stored only as a hash, and a
//! claim reads the invite and spends it in the transaction that inserts the
//! credential, so two racing claims (or a claim racing a revoke) never both
//! win.
use crate::Store;
use intent_core::{machines::ExecutorCredential, now_iso, parse_iso, Error, Result};
use sqlx::{sqlite::SqliteRow, Row};

const INVITE_COLUMNS: &str =
    "id, label, created_at, expires_at, used_at, credential_id, revoked_at";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("node invite persistence: {error}"))
}

/// A node invite as the machine keeps it (never its secret).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInvite {
    pub id: String,
    pub label: Option<String>,
    pub created_at: String,
    pub expires_at: String,
    pub used_at: Option<String>,
    pub credential_id: Option<String>,
    pub revoked_at: Option<String>,
}

impl NodeInvite {
    /// Whether `expires_at` is at or before `now`; an unreadable timestamp
    /// counts as expired.
    #[must_use]
    pub fn expired_at(&self, now: &str) -> bool {
        match (parse_iso(&self.expires_at), parse_iso(now)) {
            (Some(expires), Some(now)) => expires <= now,
            _ => true,
        }
    }
}

/// Why [`Store::claim_node_invite`] refused, in the order the machine checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeInviteRefusal {
    /// No invite with that id and secret hash.
    Invalid,
    Revoked,
    Used,
    Expired {
        expires_at: String,
    },
    /// The invite is open, but the caller refused the claim for `code`
    /// (checked after the invite itself); nothing was spent.
    Late(&'static str),
}

/// What [`Store::revoke_node_invite`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeInviteRevocation {
    Revoked,
    /// Already revoked or used: nothing changed.
    Closed,
    NotFound,
}

fn invite_row(row: &SqliteRow) -> NodeInvite {
    NodeInvite {
        id: row.get("id"),
        label: row.get("label"),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
        used_at: row.get("used_at"),
        credential_id: row.get("credential_id"),
        revoked_at: row.get("revoked_at"),
    }
}

impl Store {
    /// Record a new open invite keyed by `secret_hash` (hex SHA-256 of the
    /// secret, never the secret).
    ///
    /// # Errors
    ///
    /// `InvalidParams` for an empty id or hash or a closed invite;
    /// `Internal` on database failure.
    pub async fn insert_node_invite(&self, invite: &NodeInvite, secret_hash: &str) -> Result<()> {
        if invite.id.is_empty()
            || secret_hash.is_empty()
            || invite.used_at.is_some()
            || invite.revoked_at.is_some()
        {
            return Err(Error::InvalidParams("invalid node invite".into()));
        }
        sqlx::query(
            "INSERT INTO node_invite (id, secret_hash, label, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&invite.id)
        .bind(secret_hash)
        .bind(&invite.label)
        .bind(&invite.created_at)
        .bind(&invite.expires_at)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(())
    }

    /// The open invites (unused, unrevoked, unexpired), oldest first.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn list_open_node_invites(&self) -> Result<Vec<NodeInvite>> {
        let sql = format!(
            "SELECT {INVITE_COLUMNS} FROM node_invite \
             WHERE used_at IS NULL AND revoked_at IS NULL ORDER BY created_at, id"
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(self.read_pool())
            .await
            .map_err(db)?;
        let now = now_iso();
        Ok(rows
            .iter()
            .map(invite_row)
            .filter(|invite| !invite.expired_at(&now))
            .collect())
    }

    /// The invite each claimed credential came from: `(credential id,
    /// invite id)` pairs.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn claimed_node_invites(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            "SELECT credential_id, id FROM node_invite WHERE credential_id IS NOT NULL",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(db)?;
        Ok(rows
            .iter()
            .map(|row| (row.get("credential_id"), row.get("id")))
            .collect())
    }

    /// Revoke an open invite. A used or already revoked invite is left as
    /// it is.
    ///
    /// # Errors
    ///
    /// `Internal` on database failure.
    pub async fn revoke_node_invite(&self, id: &str) -> Result<NodeInviteRevocation> {
        let updated = sqlx::query(
            "UPDATE node_invite SET revoked_at = ? \
             WHERE id = ? AND used_at IS NULL AND revoked_at IS NULL",
        )
        .bind(now_iso())
        .bind(id)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        if updated.rows_affected() > 0 {
            return Ok(NodeInviteRevocation::Revoked);
        }
        let known: Option<String> = sqlx::query_scalar("SELECT id FROM node_invite WHERE id = ?")
            .bind(id)
            .fetch_optional(self.write_pool())
            .await
            .map_err(db)?;
        Ok(match known {
            Some(_) => NodeInviteRevocation::Closed,
            None => NodeInviteRevocation::NotFound,
        })
    }

    /// Check invite `id` against `secret_hash`, then refuse with `late` if
    /// set, and otherwise, only while the invite is still open, spend it for `credential` and insert that credential (keyed by
    /// `token_hash`), all in one immediate transaction. `Ok(Err(_))` is a
    /// refusal that changed nothing.
    ///
    /// # Errors
    ///
    /// `InvalidParams` for an invalid credential; `Internal` on database
    /// failure.
    pub async fn claim_node_invite(
        &self,
        id: &str,
        secret_hash: &str,
        backend_id: &str,
        credential: &ExecutorCredential,
        token_hash: &str,
        late: Option<&'static str>,
    ) -> Result<std::result::Result<(), NodeInviteRefusal>> {
        if credential.id.as_str().is_empty()
            || token_hash.is_empty()
            || credential.backend_id.is_empty()
            || credential.backend_name.is_empty()
            || credential.revoked_at.is_some()
        {
            return Err(Error::InvalidParams("invalid executor credential".into()));
        }
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let sql = format!("SELECT {INVITE_COLUMNS}, secret_hash FROM node_invite WHERE id = ?");
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Ok(Err(NodeInviteRefusal::Invalid));
        };
        let stored: String = row.get("secret_hash");
        if !constant_time_eq(stored.as_bytes(), secret_hash.as_bytes()) {
            return Ok(Err(NodeInviteRefusal::Invalid));
        }
        let invite = invite_row(&row);
        let now = now_iso();
        if invite.revoked_at.is_some() {
            return Ok(Err(NodeInviteRefusal::Revoked));
        }
        if invite.used_at.is_some() {
            return Ok(Err(NodeInviteRefusal::Used));
        }
        if invite.expired_at(&now) {
            return Ok(Err(NodeInviteRefusal::Expired {
                expires_at: invite.expires_at,
            }));
        }
        if let Some(code) = late {
            return Ok(Err(NodeInviteRefusal::Late(code)));
        }
        sqlx::query(
            "INSERT INTO executor_credential (id, token_hash, backend_id, backend_name, \
             backend_fingerprint, created_at, last_used_at, revoked_at) \
             VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)",
        )
        .bind(credential.id.as_str())
        .bind(token_hash)
        .bind(&credential.backend_id)
        .bind(&credential.backend_name)
        .bind(&credential.backend_fingerprint)
        .bind(&credential.created_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let spent = sqlx::query(
            "UPDATE node_invite SET used_at = ?, used_by_backend_id = ?, credential_id = ? \
             WHERE id = ? AND used_at IS NULL AND revoked_at IS NULL",
        )
        .bind(&now)
        .bind(backend_id)
        .bind(credential.id.as_str())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if spent.rows_affected() != 1 {
            return Ok(Err(NodeInviteRefusal::Used));
        }
        tx.commit().await.map_err(db)?;
        Ok(Ok(()))
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0_u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
