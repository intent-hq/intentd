//! Logical clients (Postgres). Mirrors `client_repo.rs`.

use intent_core::{now_iso, Client, ClientHostInfo, ClientId, Error, Result};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::{db, PgStore};

impl PgStore {
    pub async fn upsert_client(
        &self,
        id: &ClientId,
        name: Option<&str>,
        capabilities: Option<&serde_json::Value>,
        host: &ClientHostInfo,
    ) -> Result<()> {
        let now = now_iso();
        let caps = match capabilities {
            Some(v) => serde_json::to_string(v)
                .map_err(|e| Error::Internal(format!("encode capabilities failed: {e}")))?,
            None => "{}".to_string(),
        };
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO client (tenant_id, id, name, capabilities, hostname, pretty_hostname, \
             device_kind, first_seen, last_seen, last_hello_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $8) \
             ON CONFLICT (tenant_id, id) DO UPDATE SET \
             name = excluded.name, capabilities = excluded.capabilities, \
             hostname = excluded.hostname, pretty_hostname = excluded.pretty_hostname, \
             device_kind = excluded.device_kind, last_seen = excluded.last_seen, \
             last_hello_at = excluded.last_hello_at",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(name)
        .bind(&caps)
        .bind(host.hostname.as_deref())
        .bind(host.pretty_hostname.as_deref())
        .bind(host.device_kind.as_deref())
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn ensure_client(&self, id: &ClientId) -> Result<()> {
        let now = now_iso();
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO client (tenant_id, id, first_seen, last_seen) VALUES ($1, $2, $3, $3) \
             ON CONFLICT (tenant_id, id) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_client(&self, id: &ClientId) -> Result<Option<Client>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT id, name, capabilities, hostname, pretty_hostname, device_kind, \
             first_seen, last_seen, last_hello_at FROM client WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(map_client_row).transpose()
    }
}

fn map_client_row(r: &PgRow) -> Result<Client> {
    let caps_text: String = r.try_get("capabilities").map_err(db)?;
    let capabilities = serde_json::from_str(&caps_text)
        .map_err(|e| Error::Internal(format!("decode capabilities failed: {e}")))?;
    let text = |column: &str| r.try_get::<Option<String>, _>(column).map_err(db);
    Ok(Client {
        id: ClientId(r.try_get("id").map_err(db)?),
        name: text("name")?,
        capabilities,
        host: ClientHostInfo {
            hostname: text("hostname")?,
            pretty_hostname: text("pretty_hostname")?,
            device_kind: text("device_kind")?,
        },
        first_seen: r.try_get("first_seen").map_err(db)?,
        last_seen: r.try_get("last_seen").map_err(db)?,
        last_hello_at: text("last_hello_at")?,
    })
}
