//! MCP OAuth token bags (Postgres). Mirrors `mcp_oauth_repo.rs`.

use intent_core::Result;

use super::{db, PgStore};

impl PgStore {
    pub async fn get_mcp_oauth_token(&self, server_id: &str) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let bag = sqlx::query_scalar(
            "SELECT token_bag FROM mcp_oauth_tokens WHERE tenant_id = $1 AND server_id = $2",
        )
        .bind(self.tenant())
        .bind(server_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(bag)
    }

    pub async fn set_mcp_oauth_token(
        &self,
        server_id: &str,
        token_bag: &str,
        updated_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO mcp_oauth_tokens (tenant_id, server_id, token_bag, updated_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tenant_id, server_id) DO UPDATE SET \
             token_bag = excluded.token_bag, updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(server_id)
        .bind(token_bag)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn delete_mcp_oauth_token(&self, server_id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let removed =
            sqlx::query("DELETE FROM mcp_oauth_tokens WHERE tenant_id = $1 AND server_id = $2")
                .bind(self.tenant())
                .bind(server_id)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(removed > 0)
    }

    pub async fn list_mcp_oauth_server_ids(&self) -> Result<Vec<String>> {
        let mut tx = self.begin().await?;
        let ids = sqlx::query_scalar(
            "SELECT server_id FROM mcp_oauth_tokens WHERE tenant_id = $1 \
             ORDER BY server_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ids)
    }
}
