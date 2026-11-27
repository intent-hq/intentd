//! Outstanding `invite.challenge` nonces (PROTOCOL §5.37).
//!
//! A challenge issues a nonce; the first `invite.prove` that names it
//! consumes it. The nonce is a row, so any process of a tenant can verify a
//! nonce another one issued, and the caps (per invite, per tenant) hold
//! across processes: [`Store::issue_invite_nonce`] purges expired rows,
//! counts and inserts in one write transaction (`quota.rs`).
//!
//! A row is keyed by the nonce's hash, never the nonce. A prove claims its
//! row first ([`Store::claim_invite_nonce`]): exactly one claim wins, and
//! the claimed row keeps counting against both caps while the prove asks
//! the forge. A verdict deletes it ([`Store::delete_invite_nonce`]); a forge
//! outage releases it for a retry ([`Store::release_invite_nonce`]). A
//! claim a crash left behind expires like any other row. Times are Unix
//! milliseconds from the caller's clock.

use crate::{QuotaWrite, Store};
use intent_core::{Error, InviteScope, Result};

/// One outstanding nonce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InviteNonce {
    pub invite_id: String,
    pub scope: InviteScope,
    /// The host's authorization generation when the nonce was issued; the
    /// join commit refuses if it moved.
    pub authorization_generation: u64,
    /// Wall-clock issue time (Unix ms): the proof must not predate it.
    pub issued_at_ms: i64,
    /// Unix ms; the nonce is spent from then on.
    pub expires_at_ms: i64,
}

/// The caps [`Store::issue_invite_nonce`] admits a new nonce under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InviteNonceCaps {
    /// Outstanding nonces of one invite.
    pub per_invite: u64,
    /// Outstanding nonces of the tenant (the whole daemon on `SQLite`).
    pub total: u64,
}

pub(crate) fn scope_word(scope: InviteScope) -> &'static str {
    match scope {
        InviteScope::Workspace => "workspace",
        InviteScope::Host => "host",
    }
}

pub(crate) fn scope_from_word(word: &str) -> Result<InviteScope> {
    match word {
        "workspace" => Ok(InviteScope::Workspace),
        "host" => Ok(InviteScope::Host),
        other => Err(Error::Internal(format!(
            "invite_nonce: unknown scope {other:?}"
        ))),
    }
}

/// Which cap a full [`Store::issue_invite_nonce`] hit, given both counts.
pub(crate) fn full(per_invite: u64, total: u64, caps: InviteNonceCaps) -> Option<QuotaWrite> {
    if per_invite >= caps.per_invite {
        return Some(QuotaWrite::Full { active: per_invite });
    }
    (total >= caps.total).then_some(QuotaWrite::Full { active: total })
}

/// The columns a claim returns, in [`NONCE_COLUMNS`] order.
pub(crate) type NonceRow = (String, String, i64, i64, i64);

/// The columns [`NonceRow`] reads.
pub(crate) const NONCE_COLUMNS: &str =
    "invite_id, scope, authorization_generation, issued_at_ms, expires_at_ms";

pub(crate) fn nonce_from_row(row: NonceRow) -> Result<InviteNonce> {
    let (invite_id, scope, generation, issued_at_ms, expires_at_ms) = row;
    Ok(InviteNonce {
        invite_id,
        scope: scope_from_word(&scope)?,
        authorization_generation: generation.cast_unsigned(),
        issued_at_ms,
        expires_at_ms,
    })
}

impl Store {
    /// Issue a nonce: in one write transaction, drop the expired nonces
    /// (`expires_at_ms <= now_ms`, claimed or not), count the outstanding
    /// ones of the invite and of the tenant, and insert `nonce_hash` unless
    /// either count is at its cap ([`QuotaWrite::Full`] with that count).
    /// A hash already present is [`QuotaWrite::Lost`].
    ///
    /// # Errors
    /// A database failure.
    pub async fn issue_invite_nonce(
        &self,
        nonce_hash: &str,
        nonce: &InviteNonce,
        now_ms: i64,
        caps: InviteNonceCaps,
    ) -> Result<QuotaWrite> {
        dispatch!(self, backend => backend.issue_invite_nonce(nonce_hash, nonce, now_ms, caps).await)
    }

    /// Claim `nonce_hash` for a prove: the row stays (and keeps counting)
    /// but no other claim gets it. `None` when no such row exists or another
    /// prove holds it. An expired row is still returned: the caller reports
    /// it expired and deletes it.
    ///
    /// # Errors
    /// A database failure or a malformed row.
    pub async fn claim_invite_nonce(
        &self,
        nonce_hash: &str,
        now_ms: i64,
    ) -> Result<Option<InviteNonce>> {
        dispatch!(self, backend => backend.claim_invite_nonce(nonce_hash, now_ms).await)
    }

    /// Give a claimed nonce back for a retry (the forge could not be
    /// consulted). Its lifetime keeps running. No-op when it is gone.
    ///
    /// # Errors
    /// A database failure.
    pub async fn release_invite_nonce(&self, nonce_hash: &str) -> Result<()> {
        dispatch!(self, backend => backend.release_invite_nonce(nonce_hash).await)
    }

    /// Spend a nonce (a prove reached its verdict). No-op when it is gone.
    ///
    /// # Errors
    /// A database failure.
    pub async fn delete_invite_nonce(&self, nonce_hash: &str) -> Result<()> {
        dispatch!(self, backend => backend.delete_invite_nonce(nonce_hash).await)
    }

    /// Outstanding nonces (claimed ones included, expired ones not yet
    /// purged included).
    ///
    /// # Errors
    /// A database failure.
    pub async fn count_invite_nonces(&self) -> Result<u64> {
        dispatch!(self, backend => backend.count_invite_nonces().await)
    }
}

fn db(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("invite_nonce: {e}"))
}

impl crate::SqliteStore {
    pub async fn issue_invite_nonce(
        &self,
        nonce_hash: &str,
        nonce: &InviteNonce,
        now_ms: i64,
        caps: InviteNonceCaps,
    ) -> Result<QuotaWrite> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        sqlx::query("DELETE FROM invite_nonce WHERE expires_at_ms <= ?")
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let (per_invite, total): (i64, i64) =
            sqlx::query_as("SELECT COALESCE(SUM(invite_id = ?), 0), COUNT(*) FROM invite_nonce")
                .bind(&nonce.invite_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if let Some(refused) = full(per_invite.cast_unsigned(), total.cast_unsigned(), caps) {
            tx.commit().await.map_err(db)?;
            return Ok(refused);
        }
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO invite_nonce (nonce_hash, invite_id, scope, \
             authorization_generation, issued_at_ms, expires_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(nonce_hash)
        .bind(&nonce.invite_id)
        .bind(scope_word(nonce.scope))
        .bind(nonce.authorization_generation.cast_signed())
        .bind(nonce.issued_at_ms)
        .bind(nonce.expires_at_ms)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            == 1;
        tx.commit().await.map_err(db)?;
        Ok(if inserted {
            QuotaWrite::Written
        } else {
            QuotaWrite::Lost
        })
    }

    pub async fn claim_invite_nonce(
        &self,
        nonce_hash: &str,
        now_ms: i64,
    ) -> Result<Option<InviteNonce>> {
        let sql = format!(
            "UPDATE invite_nonce SET claimed_at_ms = ? \
             WHERE nonce_hash = ? AND claimed_at_ms IS NULL RETURNING {NONCE_COLUMNS}"
        );
        let row: Option<NonceRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(now_ms)
            .bind(nonce_hash)
            .fetch_optional(self.write_pool())
            .await
            .map_err(db)?;
        row.map(nonce_from_row).transpose()
    }

    pub async fn release_invite_nonce(&self, nonce_hash: &str) -> Result<()> {
        sqlx::query("UPDATE invite_nonce SET claimed_at_ms = NULL WHERE nonce_hash = ?")
            .bind(nonce_hash)
            .execute(self.write_pool())
            .await
            .map_err(db)?;
        Ok(())
    }

    pub async fn delete_invite_nonce(&self, nonce_hash: &str) -> Result<()> {
        sqlx::query("DELETE FROM invite_nonce WHERE nonce_hash = ?")
            .bind(nonce_hash)
            .execute(self.write_pool())
            .await
            .map_err(db)?;
        Ok(())
    }

    pub async fn count_invite_nonces(&self) -> Result<u64> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM invite_nonce")
            .fetch_one(self.read_pool())
            .await
            .map_err(db)?;
        Ok(n.cast_unsigned())
    }
}
