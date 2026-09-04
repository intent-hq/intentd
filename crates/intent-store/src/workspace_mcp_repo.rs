//! Per-workspace MCP server disable repository (§5.22). One row in
//! `workspace_mcp_disabled_server` per (workspace, server) pair means the
//! server is disabled in that workspace; absence means the workspace tracks
//! the global `mcp.disabledServers` setting. Global disable always wins —
//! this table only narrows an otherwise-enabled server. Rows cascade with
//! their workspace.

use intent_core::{now_iso, Error, Result, WorkspaceId};
use sqlx::Row;

use crate::Store;

impl Store {
    /// Set or clear the per-workspace disabled marker for `server_id` in
    /// workspace `id`. Idempotent in both directions: disabling an
    /// already-disabled pair and enabling a never-disabled pair are no-op
    /// successes.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the workspace does not exist;
    /// `Error::Internal` if the database operation fails.
    pub async fn set_workspace_mcp_server_disabled(
        &self,
        id: &WorkspaceId,
        server_id: &str,
        disabled: bool,
    ) -> Result<()> {
        // Explicit existence check: an INSERT FK violation would surface as
        // an opaque `Error::Internal`, and the DELETE arm has no FK to trip.
        let exists = sqlx::query("SELECT 1 FROM workspace WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("check workspace exists failed: {e}")))?;
        if exists.is_none() {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        if disabled {
            sqlx::query(
                "INSERT INTO workspace_mcp_disabled_server (workspace_id, server_id, created_at) \
                 VALUES (?,?,?) ON CONFLICT(workspace_id, server_id) DO NOTHING",
            )
            .bind(&id.0)
            .bind(server_id)
            .bind(now_iso())
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set workspace mcp disabled failed: {e}")))?;
        } else {
            sqlx::query(
                "DELETE FROM workspace_mcp_disabled_server \
                 WHERE workspace_id = ? AND server_id = ?",
            )
            .bind(&id.0)
            .bind(server_id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("clear workspace mcp disabled failed: {e}")))?;
        }
        Ok(())
    }

    /// Every server id disabled in workspace `id`, sorted ascending for a
    /// stable wire order. Empty for unknown workspaces — reads are lenient so
    /// list paths never fail on a stale id; writes go through the strict
    /// [`Store::set_workspace_mcp_server_disabled`].
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn workspace_mcp_disabled_servers(&self, id: &WorkspaceId) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT server_id FROM workspace_mcp_disabled_server \
             WHERE workspace_id = ? ORDER BY server_id",
        )
        .bind(&id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list workspace mcp disabled failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("server_id"))
            .collect())
    }

    /// Whether `server_id` is disabled in workspace `id`. Lenient like the
    /// list read: `false` for unknown workspaces.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn workspace_mcp_server_disabled(
        &self,
        id: &WorkspaceId,
        server_id: &str,
    ) -> Result<bool> {
        let row = sqlx::query(
            "SELECT 1 FROM workspace_mcp_disabled_server \
             WHERE workspace_id = ? AND server_id = ?",
        )
        .bind(&id.0)
        .bind(server_id)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get workspace mcp disabled failed: {e}")))?;
        Ok(row.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    use uuid::Uuid;

    /// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
    /// which is private to that module).
    struct TempDb {
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("test-ws-mcp-{}.db", Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let mut sidecar = self.path.clone().into_os_string();
                sidecar.push(suffix);
                let _ = std::fs::remove_file(&sidecar);
            }
        }
    }

    fn test_workspace(ws_id: &WorkspaceId, ts: &str) -> Workspace {
        Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            execution_environment: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    async fn store_with_workspace() -> (TempDb, Store, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ts = now_iso();
        let ws_id = WorkspaceId(format!("ws-{}", Uuid::new_v4()));
        store
            .insert_workspace(&test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        (tmp, store, ws_id)
    }

    #[tokio::test]
    async fn disable_enable_round_trip() {
        let (_tmp, store, ws) = store_with_workspace().await;
        assert!(store
            .workspace_mcp_disabled_servers(&ws)
            .await
            .unwrap()
            .is_empty());
        assert!(!store
            .workspace_mcp_server_disabled(&ws, "s1")
            .await
            .unwrap());

        store
            .set_workspace_mcp_server_disabled(&ws, "s1", true)
            .await
            .unwrap();
        assert!(store
            .workspace_mcp_server_disabled(&ws, "s1")
            .await
            .unwrap());
        assert_eq!(
            store.workspace_mcp_disabled_servers(&ws).await.unwrap(),
            vec!["s1".to_string()]
        );

        store
            .set_workspace_mcp_server_disabled(&ws, "s1", false)
            .await
            .unwrap();
        assert!(!store
            .workspace_mcp_server_disabled(&ws, "s1")
            .await
            .unwrap());
        assert!(store
            .workspace_mcp_disabled_servers(&ws)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn disable_is_idempotent_and_list_sorted() {
        let (_tmp, store, ws) = store_with_workspace().await;
        for id in ["b", "a", "b"] {
            store
                .set_workspace_mcp_server_disabled(&ws, id, true)
                .await
                .unwrap();
        }
        assert_eq!(
            store.workspace_mcp_disabled_servers(&ws).await.unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        // Enabling a never-disabled pair is a no-op success.
        store
            .set_workspace_mcp_server_disabled(&ws, "zzz", false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn write_unknown_workspace_is_not_found_reads_lenient() {
        let (_tmp, store, _ws) = store_with_workspace().await;
        let ghost = WorkspaceId("ws-ghost".to_string());
        let err = store
            .set_workspace_mcp_server_disabled(&ghost, "s1", true)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
        // Reads never fail on an unknown id.
        assert!(store
            .workspace_mcp_disabled_servers(&ghost)
            .await
            .unwrap()
            .is_empty());
        assert!(!store
            .workspace_mcp_server_disabled(&ghost, "s1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn rows_scoped_per_workspace_and_cascade_on_delete() {
        let (_tmp, store, ws1) = store_with_workspace().await;
        let ts = now_iso();
        let ws2 = WorkspaceId(format!("ws-{}", Uuid::new_v4()));
        store
            .insert_workspace(&test_workspace(&ws2, &ts))
            .await
            .unwrap();

        store
            .set_workspace_mcp_server_disabled(&ws1, "s1", true)
            .await
            .unwrap();
        store
            .set_workspace_mcp_server_disabled(&ws2, "s2", true)
            .await
            .unwrap();
        assert_eq!(
            store.workspace_mcp_disabled_servers(&ws1).await.unwrap(),
            vec!["s1".to_string()]
        );
        assert_eq!(
            store.workspace_mcp_disabled_servers(&ws2).await.unwrap(),
            vec!["s2".to_string()]
        );

        // Deleting the workspace row cascades its disabled markers.
        sqlx::query("DELETE FROM workspace WHERE id = ?")
            .bind(&ws1.0)
            .execute(store.write_pool())
            .await
            .unwrap();
        assert!(store
            .workspace_mcp_disabled_servers(&ws1)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store.workspace_mcp_disabled_servers(&ws2).await.unwrap(),
            vec!["s2".to_string()]
        );
    }
}
