//! Per-MCP-server OAuth token bags (PROTOCOL §5.22 companion). The
//! `mcp.oauth.*` RPC family manages the opaque OAuth bag associated with each
//! external MCP server id; bags are secret material. Every wire response is
//! **presence-only** — the bag itself never leaves the daemon over the wire.
//! Storage lives in the `mcp_oauth_tokens` `SQLite` table (§9.4); internal
//! consumers (e.g. an outbound HTTP request built inside the daemon) read the
//! raw bag through [`Store::get_mcp_oauth_token`], never through the wire.

use intent_core::{now_iso, Error, Result};
use intent_store::Store;
use serde_json::{json, Value};

use crate::settings::REDACTED_PLACEHOLDER;

/// Stateless executor for the `mcp.oauth.*` namespace over the [`Store`].
/// Construct one per call from the long-lived `Services`.
pub(crate) struct McpOauthService<'a> {
    store: &'a Store,
}

impl<'a> McpOauthService<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Ensure `server_id` is a non-empty string; empty ids never round-trip
    /// because the FE keys bags by server id.
    fn require_server_id(server_id: &str) -> Result<&str> {
        if server_id.is_empty() {
            return Err(Error::InvalidParams("serverId is required".to_string()));
        }
        Ok(server_id)
    }

    /// `mcp.oauth.list` → `{ tokens: [{ serverId, value }] }` — one entry per
    /// stored bag, sorted by `serverId`. `value` is always the redaction
    /// placeholder (a bag is stored iff a plaintext existed).
    pub(crate) async fn list(&self) -> Result<Value> {
        let ids = self.store.list_mcp_oauth_server_ids().await?;
        let tokens: Vec<Value> = ids
            .into_iter()
            .map(|server_id| {
                json!({
                    "serverId": server_id,
                    "value": REDACTED_PLACEHOLDER,
                })
            })
            .collect();
        Ok(json!({ "tokens": tokens }))
    }

    /// `mcp.oauth.get` → `{ serverId, value }`. `value` is the redaction
    /// placeholder when a bag exists and `null` when it does not. Never
    /// echoes bag contents on the wire.
    pub(crate) async fn get(&self, server_id: &str) -> Result<Value> {
        let server_id = Self::require_server_id(server_id)?;
        let value = match self.store.get_mcp_oauth_token(server_id).await? {
            Some(_) => json!(REDACTED_PLACEHOLDER),
            None => Value::Null,
        };
        Ok(json!({ "serverId": server_id, "value": value }))
    }

    /// `mcp.oauth.set` → persist `token_bag` for `server_id` and return
    /// `{ serverId, value }` with the redaction placeholder as `value`. The
    /// bag itself is **never** echoed. Accepts any JSON body (object / array /
    /// scalar) so the FE's bag shape can evolve without a daemon change.
    pub(crate) async fn set(&self, server_id: &str, token_bag: Value) -> Result<Value> {
        let server_id = Self::require_server_id(server_id)?;
        let raw = serde_json::to_string(&token_bag)
            .map_err(|e| Error::Internal(format!("encode mcp oauth token failed: {e}")))?;
        self.store
            .set_mcp_oauth_token(server_id, &raw, &now_iso())
            .await?;
        Ok(json!({ "serverId": server_id, "value": REDACTED_PLACEHOLDER }))
    }

    /// `mcp.oauth.delete` → drop the persisted bag for `server_id`. Idempotent:
    /// missing bags succeed with `{ success: true }`.
    pub(crate) async fn delete(&self, server_id: &str) -> Result<Value> {
        let server_id = Self::require_server_id(server_id)?;
        self.store.delete_mcp_oauth_token(server_id).await?;
        Ok(json!({ "success": true }))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use intent_store::Store;
    use uuid::Uuid;

    use super::*;

    /// Dummy bag literal used across the tests — asserted to be absent from
    /// every wire response so a real bag would be caught by the same guards.
    const DUMMY_BAG_LITERAL: &str = "dummy-oauth-payload-marker";

    struct TempDb {
        path: PathBuf,
    }
    impl TempDb {
        fn new() -> Self {
            Self {
                path: std::env::temp_dir().join(format!("intentd-oauth-{}.db", Uuid::new_v4())),
            }
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    async fn open() -> (TempDb, Store) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        (tmp, store)
    }

    fn contains_dummy(v: &Value) -> bool {
        serde_json::to_string(v)
            .unwrap()
            .contains(DUMMY_BAG_LITERAL)
    }

    #[tokio::test]
    async fn empty_list_when_no_tokens_stored() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let out = svc.list().await.unwrap();
        assert_eq!(out["tokens"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn get_missing_server_id_is_null_value() {
        let (_tmp, store) = open().await;
        let out = McpOauthService::new(&store).get("ghost").await.unwrap();
        assert_eq!(out, json!({ "serverId": "ghost", "value": Value::Null }));
    }

    #[tokio::test]
    async fn set_persists_and_redacts_bag_on_the_wire() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let bag = json!({
            "access_token": DUMMY_BAG_LITERAL,
            "refresh_token": DUMMY_BAG_LITERAL,
            "expires_at": 1_700_000_000_u64,
            "token_type": "Bearer",
        });
        let out = svc.set("srv-linear", bag).await.unwrap();
        assert_eq!(out["serverId"], json!("srv-linear"));
        assert_eq!(out["value"], json!(REDACTED_PLACEHOLDER));
        assert!(
            !contains_dummy(&out),
            "set() response leaked dummy bag literal"
        );
        // The raw bag is retrievable through the store for internal use.
        let raw = store
            .get_mcp_oauth_token("srv-linear")
            .await
            .unwrap()
            .expect("bag persisted");
        assert!(raw.contains(DUMMY_BAG_LITERAL));
    }

    #[tokio::test]
    async fn get_after_set_stays_redacted() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set("srv-x", json!({ "access_token": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let got = svc.get("srv-x").await.unwrap();
        assert_eq!(got["value"], json!(REDACTED_PLACEHOLDER));
        assert!(!contains_dummy(&got), "get() leaked dummy bag literal");
    }

    #[tokio::test]
    async fn list_lists_stored_server_ids_redacted_sorted() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set("b", json!({ "t": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        svc.set("a", json!({ "t": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let out = svc.list().await.unwrap();
        assert!(!contains_dummy(&out), "list() leaked dummy bag literal");
        let arr = out["tokens"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["serverId"], json!("a"));
        assert_eq!(arr[0]["value"], json!(REDACTED_PLACEHOLDER));
        assert_eq!(arr[1]["serverId"], json!("b"));
    }

    #[tokio::test]
    async fn delete_is_idempotent_and_removes_the_bag() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        // Absent: idempotent success.
        let out = svc.delete("nope").await.unwrap();
        assert_eq!(out, json!({ "success": true }));
        // Present: removed.
        svc.set("srv", json!({ "t": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let out = svc.delete("srv").await.unwrap();
        assert_eq!(out, json!({ "success": true }));
        assert!(store.get_mcp_oauth_token("srv").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_server_id_rejected() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        for res in [
            svc.get("").await,
            svc.set("", json!({})).await,
            svc.delete("").await,
        ] {
            let err = res.unwrap_err();
            assert!(matches!(err, Error::InvalidParams(_)));
        }
    }

    #[tokio::test]
    async fn set_replaces_previous_bag() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set("srv", json!({ "v": 1 })).await.unwrap();
        svc.set("srv", json!({ "v": 2, "marker": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let raw = store
            .get_mcp_oauth_token("srv")
            .await
            .unwrap()
            .expect("bag persisted");
        assert!(raw.contains("\"v\":2"));
        assert!(raw.contains(DUMMY_BAG_LITERAL));
    }
}
