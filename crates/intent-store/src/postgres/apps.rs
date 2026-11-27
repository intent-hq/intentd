//! Intent apps library and per-app storage (Postgres). Mirrors `app_repo.rs`.

use intent_core::Result;

use super::{db, PgStore};
use crate::app_repo::{prefix_upper_bound, AppStorageWrite};

fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

impl PgStore {
    pub async fn get_app_record(&self, app_id: &str) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let record = sqlx::query_scalar(
            "SELECT record_json FROM intent_app WHERE tenant_id = $1 AND app_id = $2",
        )
        .bind(self.tenant())
        .bind(app_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    pub async fn list_app_records(&self) -> Result<Vec<String>> {
        let mut tx = self.begin().await?;
        let records = sqlx::query_scalar(
            "SELECT record_json FROM intent_app WHERE tenant_id = $1 \
             ORDER BY app_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(records)
    }

    pub async fn put_app_record(
        &self,
        app_id: &str,
        record_json: &str,
        updated_at: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO intent_app (tenant_id, app_id, record_json, updated_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tenant_id, app_id) DO UPDATE SET \
             record_json = excluded.record_json, updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(app_id)
        .bind(record_json)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn delete_app(&self, app_id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM intent_app_storage WHERE tenant_id = $1 AND app_id = $2")
            .bind(self.tenant())
            .bind(app_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let removed = sqlx::query("DELETE FROM intent_app WHERE tenant_id = $1 AND app_id = $2")
            .bind(self.tenant())
            .bind(app_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(removed > 0)
    }

    pub async fn app_storage_get(&self, app_id: &str, key: &str) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let value = sqlx::query_scalar(
            "SELECT value_json FROM intent_app_storage \
             WHERE tenant_id = $1 AND app_id = $2 AND key = $3",
        )
        .bind(self.tenant())
        .bind(app_id)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(value)
    }

    pub async fn app_storage_set(
        &self,
        app_id: &str,
        key: &str,
        value_json: &str,
        size_bytes: u64,
        limit_bytes: u64,
    ) -> Result<AppStorageWrite> {
        // The tenant writer lock serializes this read-check-write.
        let mut tx = self.begin_write().await?;
        let used: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0)::bigint FROM intent_app_storage \
             WHERE tenant_id = $1 AND app_id = $2",
        )
        .bind(self.tenant())
        .bind(app_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let old: Option<i64> = sqlx::query_scalar(
            "SELECT size_bytes FROM intent_app_storage \
             WHERE tenant_id = $1 AND app_id = $2 AND key = $3",
        )
        .bind(self.tenant())
        .bind(app_id)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let used = to_u64(used);
        let after = used
            .saturating_sub(old.map_or(0, to_u64))
            .saturating_add(size_bytes);
        if after > limit_bytes {
            tx.commit().await.map_err(db)?;
            return Ok(AppStorageWrite::OverQuota { used_bytes: used });
        }
        sqlx::query(
            "INSERT INTO intent_app_storage (tenant_id, app_id, key, value_json, size_bytes) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (tenant_id, app_id, key) DO UPDATE SET \
             value_json = excluded.value_json, size_bytes = excluded.size_bytes",
        )
        .bind(self.tenant())
        .bind(app_id)
        .bind(key)
        .bind(value_json)
        .bind(to_i64(size_bytes))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(AppStorageWrite::Written { used_bytes: after })
    }

    pub async fn app_storage_delete(&self, app_id: &str, key: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let removed = sqlx::query(
            "DELETE FROM intent_app_storage WHERE tenant_id = $1 AND app_id = $2 AND key = $3",
        )
        .bind(self.tenant())
        .bind(app_id)
        .bind(key)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
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
        let mut tx = self.begin().await?;
        let keys = sqlx::query_scalar(
            "SELECT key FROM intent_app_storage WHERE tenant_id = $1 AND app_id = $2 \
             AND key COLLATE \"C\" >= $3 \
             AND ($4::text IS NULL OR key COLLATE \"C\" < $4) \
             AND ($5::text IS NULL OR key COLLATE \"C\" > $5) \
             ORDER BY key COLLATE \"C\" LIMIT $6",
        )
        .bind(self.tenant())
        .bind(app_id)
        .bind(prefix)
        .bind(upper.as_deref())
        .bind(after)
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(keys)
    }
}
