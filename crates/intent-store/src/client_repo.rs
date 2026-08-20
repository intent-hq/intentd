//! Logical client repository (§9.2, §16): the stable, client-supplied identity
//! that survives reconnects, persisted by the `client.hello` handshake. The
//! ephemeral per-connection id is transport-only and never stored here.

#[cfg(test)]
use intent_core::Client;
use intent_core::{now_iso, ClientId, Error, Result};
#[cfg(test)]
use sqlx::sqlite::SqliteRow;
#[cfg(test)]
use sqlx::Row;

use crate::Store;

impl Store {
    /// Upsert a logical client by id: insert with `first_seen`/`last_seen` set to
    /// now, or — on a re-hello — update `name`/`capabilities` and touch
    /// `last_seen` while preserving the original `first_seen`. `capabilities` is
    /// stored as a JSON-text bag (defaulting to `{}` when absent).
    pub async fn upsert_client(
        &self,
        id: &ClientId,
        name: Option<&str>,
        capabilities: Option<&serde_json::Value>,
    ) -> Result<()> {
        let now = now_iso();
        let caps = match capabilities {
            Some(v) => serde_json::to_string(v)
                .map_err(|e| Error::Internal(format!("encode capabilities failed: {e}")))?,
            None => "{}".to_string(),
        };
        sqlx::query(
            "INSERT INTO client (id, name, capabilities, first_seen, last_seen) \
             VALUES (?,?,?,?,?) \
             ON CONFLICT(id) DO UPDATE SET \
             name = excluded.name, capabilities = excluded.capabilities, \
             last_seen = excluded.last_seen",
        )
        .bind(&id.0)
        .bind(name)
        .bind(&caps)
        .bind(&now)
        .bind(&now)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert client failed: {e}")))?;
        Ok(())
    }

    /// Fetch a logical client by id (used by tests + diagnostics).
    #[cfg(test)]
    pub(crate) async fn get_client(&self, id: &ClientId) -> Result<Option<Client>> {
        let row = sqlx::query(
            "SELECT id, name, capabilities, first_seen, last_seen FROM client WHERE id = ?",
        )
        .bind(&id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get client failed: {e}")))?;
        row.as_ref().map(map_client_row).transpose()
    }
}

#[cfg(test)]
fn map_client_row(r: &SqliteRow) -> Result<Client> {
    let caps_text: String = r.get("capabilities");
    let capabilities = serde_json::from_str(&caps_text)
        .map_err(|e| Error::Internal(format!("decode capabilities failed: {e}")))?;
    Ok(Client {
        id: ClientId(r.get("id")),
        name: r.get("name"),
        capabilities,
        first_seen: r.get("first_seen"),
        last_seen: r.get("last_seen"),
    })
}
