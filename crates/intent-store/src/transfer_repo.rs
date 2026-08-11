//! Row statistics for the workspace transfer manifest (`workspace.transfer.plan`).
//!
//! Enumerates every workspace-scoped table that rides in a transfer archive —
//! deliberately excluding `event` (event history stays on the source, spec
//! "Resolved Design Decisions" #3) and non-workspace-scoped tables (settings,
//! secrets, `known_repo`, usage stats, idempotency keys, clients).

use intent_core::transfer::TransferTableStat;
use intent_core::{Error, Result, WorkspaceId};

use crate::Store;

/// Workspace-scoped tables included in a transfer, each with the SQL predicate
/// that scopes its rows to one workspace (`?1` = workspace id). `agent_message`
/// and `agent_queue` have no `workspace_id` column and scope through their
/// owning `agent_session`; `completion_watch` is workspace-scoped from either
/// end of the parent/child pair.
pub const TRANSFER_TABLES: &[(&str, &str)] = &[
    ("workspace", "id = ?1"),
    ("note", "workspace_id = ?1"),
    ("note_version", "workspace_id = ?1"),
    ("note_line_attribution", "workspace_id = ?1"),
    ("comment", "workspace_id = ?1"),
    ("draft", "workspace_id = ?1"),
    ("agent_session", "workspace_id = ?1"),
    (
        "agent_message",
        "agent_id IN (SELECT id FROM agent_session WHERE workspace_id = ?1)",
    ),
    (
        "agent_queue",
        "agent_id IN (SELECT id FROM agent_session WHERE workspace_id = ?1)",
    ),
    ("interrupted_agent", "workspace_id = ?1"),
    ("delegation_group", "workspace_id = ?1"),
    (
        "completion_watch",
        "parent_workspace_id = ?1 OR child_workspace_id = ?1",
    ),
    ("event_subscription", "workspace_id = ?1"),
    ("hook", "workspace_id = ?1"),
    ("pr_monitor", "workspace_id = ?1"),
    ("script", "workspace_id = ?1"),
    ("task_agent_link", "workspace_id = ?1"),
    ("sandbox", "workspace_id = ?1"),
    ("tracked_changes", "workspace_id = ?1"),
    ("diffs", "workspace_id = ?1"),
    ("workspace_metrics", "workspace_id = ?1"),
    ("agent_metrics", "workspace_id = ?1"),
    ("workspace_context_item", "workspace_id = ?1"),
    ("workspace_ui_context", "workspace_id = ?1"),
];

impl Store {
    /// Per-table row count + approximate serialized byte size for one
    /// workspace, over [`TRANSFER_TABLES`]. `approx_bytes` sums
    /// `LENGTH(CAST(col AS BLOB))` across every column of every scoped row —
    /// an estimate of the payload carried by an export, not on-disk size.
    /// Read-only. Tables with zero rows are still listed (count 0), so the
    /// manifest shape is stable across workspaces.
    pub async fn transfer_table_stats(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TransferTableStat>> {
        let mut stats = Vec::with_capacity(TRANSFER_TABLES.len());
        for (table, predicate) in TRANSFER_TABLES {
            let columns = self.table_columns(table).await?;
            let size_expr = if columns.is_empty() {
                "0".to_string()
            } else {
                columns
                    .iter()
                    .map(|c| format!("COALESCE(LENGTH(CAST(\"{c}\" AS BLOB)), 0)"))
                    .collect::<Vec<_>>()
                    .join(" + ")
            };
            let sql = format!(
                "SELECT COUNT(*) AS n, COALESCE(SUM({size_expr}), 0) AS b \
                 FROM \"{table}\" WHERE {predicate}"
            );
            let row = sqlx::query_as::<_, (i64, i64)>(&sql)
                .bind(&workspace_id.0)
                .fetch_one(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("transfer stats for {table} failed: {e}")))?;
            stats.push(TransferTableStat {
                name: (*table).to_string(),
                row_count: row.0,
                approx_bytes: row.1,
            });
        }
        Ok(stats)
    }

    /// Column names of `table` in declaration order (via `PRAGMA table_info`).
    async fn table_columns(&self, table: &str) -> Result<Vec<String>> {
        let rows = sqlx::query_as::<_, (i64, String)>(&format!(
            "SELECT cid, name FROM pragma_table_info('{table}') ORDER BY cid"
        ))
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("table_info for {table} failed: {e}")))?;
        Ok(rows.into_iter().map(|(_, name)| name).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::TRANSFER_TABLES;
    use crate::Store;
    use intent_core::WorkspaceId;
    use uuid::Uuid;

    /// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
    /// which is private to that module).
    struct TempDb {
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("test-transfer-{}.db", Uuid::new_v4()));
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

    async fn run(store: &Store, sql: String) {
        sqlx::query(&sql)
            .execute(store.write_pool())
            .await
            .unwrap_or_else(|e| panic!("seed failed: {sql}: {e}"));
    }

    /// Seed one row into every [`TRANSFER_TABLES`] entry for `ws` — two for
    /// `completion_watch` (one parent-side, one child-side), each paired with
    /// a synthetic peer workspace id unique to `ws` so the rows can never
    /// match another seeded workspace's predicate. All values are fixed test
    /// literals, so string interpolation into SQL is safe here.
    async fn seed(store: &Store, ws: &str) {
        let t = "2026-01-01T00:00:00Z";
        let agent = format!("agent-{ws}");
        let client = format!("client-{ws}");
        let peer = format!("peer-{ws}");
        for sql in [
            format!("INSERT INTO workspace (id, title, branch, created_at, updated_at) VALUES ('{ws}', 'T', 'main', '{t}', '{t}')"),
            format!("INSERT INTO client (id, first_seen, last_seen) VALUES ('{client}', '{t}', '{t}')"),
            format!("INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) VALUES ('{agent}', '{ws}', 'A', 'idle', '{t}', '{t}')"),
            format!("INSERT INTO note (id, workspace_id, title, content, created_at, updated_at) VALUES ('n1', '{ws}', 'N', 'body', '{t}', '{t}')"),
            format!("INSERT INTO note_version (note_id, workspace_id, v, date, author_id, author_name, author_type, title, content) VALUES ('n1', '{ws}', 1, '{t}', 'u', 'U', 'user', 'N', 'body')"),
            format!("INSERT INTO note_line_attribution (note_id, workspace_id, computed_at, attributions_json) VALUES ('n1', '{ws}', '{t}', '[]')"),
            format!("INSERT INTO comment (id, thread_id, note_id, workspace_id, kind, content, author, author_type, anchor_json, created_at, updated_at) VALUES ('c-{ws}', 'th', 'n1', '{ws}', 'comment', 'hi', 'u', 'user', '{{}}', '{t}', '{t}')"),
            format!("INSERT INTO draft (workspace_id, agent_id, client_id, text, updated_at) VALUES ('{ws}', '{agent}', '{client}', 'd', '{t}')"),
            format!("INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) VALUES ('m-{ws}', '{agent}', 1, 'user', '[]', '{t}')"),
            format!("INSERT INTO agent_queue (id, agent_id, position, payload, created_at) VALUES ('q-{ws}', '{agent}', 0, '{{}}', '{t}')"),
            format!("INSERT INTO interrupted_agent (agent_id, workspace_id, prev_status, interrupted_at) VALUES ('{agent}', '{ws}', 'working', '{t}')"),
            format!("INSERT INTO delegation_group (group_id, workspace_id, parent_agent_id, await_mode, expected_agent_ids, created_at, updated_at) VALUES ('g-{ws}', '{ws}', '{agent}', 'after_all', '[]', '{t}', '{t}')"),
            format!("INSERT INTO completion_watch (id, parent_workspace_id, child_workspace_id, parent_agent_id, child_agent_id, created_at) VALUES ('cwp-{ws}', '{ws}', '{peer}', '{agent}', 'child', '{t}')"),
            format!("INSERT INTO completion_watch (id, parent_workspace_id, child_workspace_id, parent_agent_id, child_agent_id, created_at) VALUES ('cwc-{ws}', '{peer}', '{ws}', 'parent', '{agent}', '{t}')"),
            format!(r#"INSERT INTO event_subscription (id, workspace_id, subscriber_agent_id, event_types, created_at) VALUES ('es-{ws}', '{ws}', '{agent}', '["note:*"]', '{t}')"#),
            format!("INSERT INTO hook (hook_id, workspace_id, agent_id, name, code, delay_ms, state, created_at) VALUES ('h-{ws}', '{ws}', '{agent}', 'H', 'return', 10000, 'scheduled', '{t}')"),
            format!("INSERT INTO pr_monitor (monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number, state, created_at, updated_at) VALUES ('pm-{ws}', '{ws}', '{agent}', 'o', 'r', 1, 'active', '{t}', '{t}')"),
            format!("INSERT INTO script (id, workspace_id, name, command, mode, source, created_at) VALUES ('s-{ws}', '{ws}', 'S', 'true', 'command', 'user', '{t}')"),
            format!("INSERT INTO task_agent_link (workspace_id, note_id, task_key, task_text, agent_id, created_at) VALUES ('{ws}', 'n1', 'k', 'do', '{agent}', 1)"),
            format!("INSERT INTO sandbox (id, workspace_id, agent_id, path, branch, base_commit_sha, created_at, updated_at) VALUES ('sb-{ws}', '{ws}', '{agent}', '/tmp/sb', 'sb/a', 'abc', '{t}', '{t}')"),
            format!("INSERT INTO tracked_changes (id, workspace_id, path, stage, status, created_at, updated_at) VALUES ('tc-{ws}', '{ws}', 'a.txt', 'unstaged', 'modified', '{t}', '{t}')"),
            format!("INSERT INTO diffs (id, workspace_id, file_path, created_at, updated_at) VALUES ('df-{ws}', '{ws}', 'a.txt', '{t}', '{t}')"),
            format!("INSERT INTO workspace_metrics (workspace_id, updated_at) VALUES ('{ws}', '{t}')"),
            format!("INSERT INTO agent_metrics (agent_id, workspace_id, updated_at) VALUES ('{agent}', '{ws}', '{t}')"),
            format!("INSERT INTO workspace_context_item (workspace_id, id, ordinal, payload) VALUES ('{ws}', 'ci', 0, '{{}}')"),
            format!("INSERT INTO workspace_ui_context (workspace_id, payload) VALUES ('{ws}', '{{}}')"),
        ] {
            run(store, sql).await;
        }
    }

    /// Every [`TRANSFER_TABLES`] predicate actually selects rows: with every
    /// table seeded for two workspaces, each table reports exactly the target
    /// workspace's rows — 1 everywhere, 2 for `completion_watch` (parent-side
    /// OR child-side both match) — with a positive byte estimate. The second
    /// workspace's rows prove the predicates also *exclude* foreign rows
    /// (notably the `agent_queue`/`agent_message` session subquery and the
    /// `completion_watch` parent/child pair).
    #[tokio::test]
    async fn transfer_table_stats_counts_seeded_rows_in_every_table() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        seed(&store, "ws-a").await;
        seed(&store, "ws-b").await;

        let stats = store
            .transfer_table_stats(&WorkspaceId("ws-a".to_string()))
            .await
            .expect("stats");

        assert_eq!(stats.len(), TRANSFER_TABLES.len());
        for (i, (name, _)) in TRANSFER_TABLES.iter().enumerate() {
            assert_eq!(stats[i].name, *name, "stats follow TRANSFER_TABLES order");
            let expected = if *name == "completion_watch" { 2 } else { 1 };
            assert_eq!(
                stats[i].row_count, expected,
                "table {name} must count exactly the target workspace's rows"
            );
            assert!(
                stats[i].approx_bytes > 0,
                "table {name} must report a positive byte estimate"
            );
        }
    }
}
