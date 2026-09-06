//! Logical client repository (§9.2, §16): the stable, client-supplied identity
//! that survives reconnects, persisted by the `client.hello` handshake. The
//! ephemeral per-connection id is transport-only and never stored here.

use intent_core::{now_iso, Client, ClientHostInfo, ClientId, Error, Result};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

impl Store {
    /// Upsert a logical client by id: insert with `first_seen`/`last_seen` set to
    /// now, or — on a re-hello — update `name`/`capabilities`/`host` and touch
    /// `last_seen` while preserving the original `first_seen`. `capabilities` is
    /// stored as a JSON-text bag (defaulting to `{}` when absent); the `host`
    /// triple is refreshed wholesale (a hello that omits a field clears it).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
        sqlx::query(
            "INSERT INTO client (id, name, capabilities, hostname, pretty_hostname, \
             device_kind, first_seen, last_seen) \
             VALUES (?,?,?,?,?,?,?,?) \
             ON CONFLICT(id) DO UPDATE SET \
             name = excluded.name, capabilities = excluded.capabilities, \
             hostname = excluded.hostname, pretty_hostname = excluded.pretty_hostname, \
             device_kind = excluded.device_kind, last_seen = excluded.last_seen",
        )
        .bind(&id.0)
        .bind(name)
        .bind(&caps)
        .bind(host.hostname.as_deref())
        .bind(host.pretty_hostname.as_deref())
        .bind(host.device_kind.as_deref())
        .bind(&now)
        .bind(&now)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert client failed: {e}")))?;
        Ok(())
    }

    /// Fetch a logical client by id — `None` when it never completed a
    /// `client.hello` (the `workspace.setBrowserClient` "never seen" guard
    /// and the offline-pin display name lookup).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_client(&self, id: &ClientId) -> Result<Option<Client>> {
        let row = sqlx::query(
            "SELECT id, name, capabilities, hostname, pretty_hostname, device_kind, \
             first_seen, last_seen FROM client WHERE id = ?",
        )
        .bind(&id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get client failed: {e}")))?;
        row.as_ref().map(map_client_row).transpose()
    }
}

fn map_client_row(r: &SqliteRow) -> Result<Client> {
    let caps_text: String = r.get("capabilities");
    let capabilities = serde_json::from_str(&caps_text)
        .map_err(|e| Error::Internal(format!("decode capabilities failed: {e}")))?;
    Ok(Client {
        id: ClientId(r.get("id")),
        name: r.get("name"),
        capabilities,
        host: ClientHostInfo {
            hostname: r.get("hostname"),
            pretty_hostname: r.get("pretty_hostname"),
            device_kind: r.get("device_kind"),
        },
        first_seen: r.get("first_seen"),
        last_seen: r.get("last_seen"),
    })
}
