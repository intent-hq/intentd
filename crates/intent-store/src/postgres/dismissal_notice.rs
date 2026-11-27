//! Questions-dismissed notice claims (Postgres). Mirrors
//! `dismissal_notice_repo.rs`.

use intent_core::{now_iso, AgentId, Result};

use super::{db, PgStore};

impl PgStore {
    pub async fn claim_dismissal_notice(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let inserted = sqlx::query(
            "INSERT INTO agent_dismissal_notice (tenant_id, agent_id, message_id, created_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tenant_id, agent_id, message_id) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .bind(message_id)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(inserted > 0)
    }

    pub async fn release_dismissal_notice(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM agent_dismissal_notice \
             WHERE tenant_id = $1 AND agent_id = $2 AND message_id = $3",
        )
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }
}
