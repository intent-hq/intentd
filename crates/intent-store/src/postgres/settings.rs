//! Settings state table and the daemon id (Postgres). Mirrors
//! `settings_repo.rs` and `Store::daemon_id`; every row belongs to the
//! store's tenant, so each tenant has its own settings and daemon id.

use intent_core::{Error, Result, WorkspaceId};

use super::{db, PgStore};

const DAEMON_ID_KEY: &str = "executor.daemonId";

impl PgStore {
    pub async fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let value =
            sqlx::query_scalar("SELECT value FROM settings WHERE tenant_id = $1 AND key = $2")
                .bind(self.tenant())
                .bind(key)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(value)
    }

    pub async fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO settings (tenant_id, key, value) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, key) DO UPDATE SET value = excluded.value",
        )
        .bind(self.tenant())
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn set_setting_if_absent(&self, key: &str, value: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let inserted = sqlx::query(
            "INSERT INTO settings (tenant_id, key, value) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, key) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(inserted > 0)
    }

    pub async fn delete_setting(&self, key: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let removed = sqlx::query("DELETE FROM settings WHERE tenant_id = $1 AND key = $2")
            .bind(self.tenant())
            .bind(key)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(removed > 0)
    }

    pub async fn get_agent_creation_preferences(
        &self,
        id: &WorkspaceId,
    ) -> Result<serde_json::Value> {
        self.get_setting(&crate::settings_repo::agent_creation_preferences_key(id))
            .await?
            .map_or_else(
                || Ok(serde_json::json!({})),
                |raw| {
                    serde_json::from_str(&raw).map_err(|e| {
                        Error::Internal(format!("decode agent creation preferences: {e}"))
                    })
                },
            )
    }

    pub async fn daemon_id(&self) -> Result<String> {
        let candidate = serde_json::Value::String(uuid::Uuid::new_v4().to_string()).to_string();
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO settings (tenant_id, key, value) VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, key) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(DAEMON_ID_KEY)
        .bind(&candidate)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let raw: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE tenant_id = $1 AND key = $2")
                .bind(self.tenant())
                .bind(DAEMON_ID_KEY)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        tx.commit().await.map_err(db)?;
        match serde_json::from_str::<serde_json::Value>(&raw) {
            Ok(serde_json::Value::String(id)) if !id.is_empty() => Ok(id),
            _ => Err(db("corrupt daemon id")),
        }
    }
}
