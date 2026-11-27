//! Outstanding invite nonces (Postgres). Mirrors `invite_nonce_repo.rs`,
//! which states the rules. An issue locks the tenant's `invite-nonce` quota
//! row (`quotas.rs`) before it purges, counts and inserts, so two processes
//! of a tenant never both admit the last nonce under a cap.

use intent_core::Result;

use super::quotas::{lock_quota, QuotaScope};
use super::{db, PgStore};
use crate::invite_nonce_repo::{
    full, nonce_from_row, scope_word, InviteNonce, InviteNonceCaps, NonceRow,
};
use crate::QuotaWrite;

impl PgStore {
    pub async fn issue_invite_nonce(
        &self,
        nonce_hash: &str,
        nonce: &InviteNonce,
        now_ms: i64,
        caps: InviteNonceCaps,
    ) -> Result<QuotaWrite> {
        let mut tx = self.begin_write().await?;
        lock_quota(&mut tx, self.tenant(), QuotaScope::InviteNonce, "").await?;
        sqlx::query("DELETE FROM invite_nonce WHERE tenant_id = $1 AND expires_at_ms <= $2")
            .bind(self.tenant())
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let (per_invite, total): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE invite_id = $2 AND claimed_at_ms IS NULL), count(*) \
             FROM invite_nonce WHERE tenant_id = $1",
        )
        .bind(self.tenant())
        .bind(&nonce.invite_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if let Some(refused) = full(per_invite.cast_unsigned(), total.cast_unsigned(), caps) {
            tx.commit().await.map_err(db)?;
            return Ok(refused);
        }
        let inserted = sqlx::query(
            "INSERT INTO invite_nonce (tenant_id, nonce_hash, invite_id, scope, \
             authorization_generation, issued_at_ms, expires_at_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, nonce_hash) DO NOTHING",
        )
        .bind(self.tenant())
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
        let mut tx = self.begin_write().await?;
        let row: Option<NonceRow> = sqlx::query_as(
            "UPDATE invite_nonce SET claimed_at_ms = $3 \
             WHERE tenant_id = $1 AND nonce_hash = $2 AND claimed_at_ms IS NULL \
             RETURNING invite_id, scope, authorization_generation, issued_at_ms, expires_at_ms",
        )
        .bind(self.tenant())
        .bind(nonce_hash)
        .bind(now_ms)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.map(nonce_from_row).transpose()
    }

    pub async fn release_invite_nonce(&self, nonce_hash: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE invite_nonce SET claimed_at_ms = NULL \
             WHERE tenant_id = $1 AND nonce_hash = $2",
        )
        .bind(self.tenant())
        .bind(nonce_hash)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn delete_invite_nonce(&self, nonce_hash: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM invite_nonce WHERE tenant_id = $1 AND nonce_hash = $2")
            .bind(self.tenant())
            .bind(nonce_hash)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn count_invite_nonces(&self) -> Result<u64> {
        let mut tx = self.begin().await?;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM invite_nonce WHERE tenant_id = $1")
            .bind(self.tenant())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(n.cast_unsigned())
    }
}
