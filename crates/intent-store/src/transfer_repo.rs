//! Row statistics for the workspace transfer manifest (`workspace.transfer.plan`)
//! plus the generic row export/import used by the transfer archive: export
//! serializes every [`TRANSFER_TABLES`] row to JSON (`rows/<table>.jsonl` in
//! the archive), import inserts transformed rows back in one transaction.
//!
//! Enumerates every workspace-scoped table that rides in a transfer archive —
//! deliberately excluding `event` (event history stays on the source, spec
//! "Resolved Design Decisions" #3) and non-workspace-scoped tables (settings,
//! secrets, `known_repo`, usage stats, idempotency keys, clients).

use intent_core::transfer::TransferTableStat;
use intent_core::{Error, Result, WorkspaceId};
use sqlx::{Column, Row, TypeInfo, ValueRef};

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

    /// Export every [`TRANSFER_TABLES`] row for one workspace as JSON objects
    /// (`column name → value`), in table order. Values map SQLite storage
    /// classes to JSON: NULL → `null`, INTEGER → number, REAL → number,
    /// TEXT → string, BLOB → `{ "$base64": "<bytes>" }`. This is the row
    /// payload the export archive writes to `rows/<table>.jsonl` and
    /// [`Store::transfer_import_rows`] round-trips on the target.
    pub async fn transfer_export_rows(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<(String, Vec<serde_json::Value>)>> {
        let mut out = Vec::with_capacity(TRANSFER_TABLES.len());
        for (table, predicate) in TRANSFER_TABLES {
            let sql = format!("SELECT * FROM \"{table}\" WHERE {predicate}");
            let rows = sqlx::query(&sql)
                .bind(&workspace_id.0)
                .fetch_all(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("transfer export {table} failed: {e}")))?;
            let mut objects = Vec::with_capacity(rows.len());
            for row in rows {
                objects.push(row_to_json(table, &row)?);
            }
            out.push(((*table).to_string(), objects));
        }
        Ok(out)
    }

    /// Insert transformed transfer rows into their tables inside ONE
    /// transaction — the atomic heart of `workspace.import.commit`. `rows`
    /// entries must name [`TRANSFER_TABLES`] members (anything else —
    /// including `event` — is rejected before any write) and are inserted in
    /// canonical [`TRANSFER_TABLES`] order regardless of input order, so FK
    /// parents (`workspace`, `agent_session`, `note`) always land before
    /// their children. Every row's keys are validated against the live
    /// schema. Nothing is visible unless the whole batch commits. Returns
    /// the number of rows inserted.
    pub async fn transfer_import_rows(
        &self,
        rows: &[(String, Vec<serde_json::Value>)],
    ) -> Result<usize> {
        for (table, _) in rows {
            if !TRANSFER_TABLES.iter().any(|(t, _)| t == table) {
                return Err(Error::InvalidParams(format!(
                    "transfer import: table {table} is not part of the transfer set"
                )));
            }
        }
        // Pre-fetch schemas outside the transaction (read-only pragma).
        let mut schemas = std::collections::HashMap::new();
        for (table, _) in TRANSFER_TABLES {
            schemas.insert(*table, self.table_columns(table).await?);
        }

        let mut tx = self
            .write_pool()
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("transfer import begin failed: {e}")))?;
        let mut inserted = 0usize;
        for (table, _) in TRANSFER_TABLES {
            let Some((_, objects)) = rows.iter().find(|(t, _)| t == table) else {
                continue;
            };
            let schema = &schemas[table];
            for object in objects {
                let map = object.as_object().ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "transfer import: {table} row is not a JSON object"
                    ))
                })?;
                let mut columns = Vec::with_capacity(map.len());
                for key in map.keys() {
                    if !schema.contains(key) {
                        return Err(Error::InvalidParams(format!(
                            "transfer import: {table} has no column {key}"
                        )));
                    }
                    columns.push(format!("\"{key}\""));
                }
                if columns.is_empty() {
                    continue;
                }
                let placeholders = vec!["?"; columns.len()].join(",");
                let sql = format!(
                    "INSERT INTO \"{table}\" ({}) VALUES ({placeholders})",
                    columns.join(",")
                );
                let mut query = sqlx::query(&sql);
                for (key, value) in map {
                    query = bind_json_value(query, table, key, value)?;
                }
                query.execute(&mut *tx).await.map_err(|e| {
                    Error::Internal(format!("transfer import insert into {table} failed: {e}"))
                })?;
                inserted += 1;
            }
        }
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("transfer import commit failed: {e}")))?;
        Ok(inserted)
    }
}

/// Serialize one SQLite row to a JSON object keyed by column name (see
/// [`Store::transfer_export_rows`] for the storage-class mapping).
fn row_to_json(table: &str, row: &sqlx::sqlite::SqliteRow) -> Result<serde_json::Value> {
    let mut object = serde_json::Map::with_capacity(row.columns().len());
    for (i, column) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(i).map_err(|e| {
            Error::Internal(format!("transfer export {table}.{}: {e}", column.name()))
        })?;
        let value = if raw.is_null() {
            serde_json::Value::Null
        } else {
            match raw.type_info().name() {
                "INTEGER" | "BOOLEAN" => serde_json::json!(row
                    .try_get::<i64, _>(i)
                    .map_err(|e| Error::Internal(format!("transfer export {table}: {e}")))?),
                "REAL" => serde_json::json!(row
                    .try_get::<f64, _>(i)
                    .map_err(|e| Error::Internal(format!("transfer export {table}: {e}")))?),
                "BLOB" => {
                    let bytes = row
                        .try_get::<Vec<u8>, _>(i)
                        .map_err(|e| Error::Internal(format!("transfer export {table}: {e}")))?;
                    serde_json::json!({ "$base64": base64_encode(&bytes) })
                }
                _ => serde_json::json!(row
                    .try_get::<String, _>(i)
                    .map_err(|e| Error::Internal(format!("transfer export {table}: {e}")))?),
            }
        };
        object.insert(column.name().to_string(), value);
    }
    Ok(serde_json::Value::Object(object))
}

/// Bind one exported JSON value back to its SQLite storage class (the inverse
/// of [`row_to_json`]).
fn bind_json_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    table: &str,
    key: &str,
    value: &serde_json::Value,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>> {
    Ok(match value {
        serde_json::Value::Null => query.bind(Option::<String>::None),
        serde_json::Value::Bool(b) => query.bind(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(f) = n.as_f64() {
                query.bind(f)
            } else {
                return Err(Error::InvalidParams(format!(
                    "transfer import: {table}.{key} has an unrepresentable number"
                )));
            }
        }
        serde_json::Value::String(s) => query.bind(s.clone()),
        serde_json::Value::Object(o) => {
            let Some(serde_json::Value::String(encoded)) = o.get("$base64") else {
                return Err(Error::InvalidParams(format!(
                    "transfer import: {table}.{key} object value is not a $base64 blob"
                )));
            };
            query.bind(base64_decode(encoded).ok_or_else(|| {
                Error::InvalidParams(format!("transfer import: {table}.{key} invalid base64"))
            })?)
        }
        serde_json::Value::Array(_) => {
            return Err(Error::InvalidParams(format!(
                "transfer import: {table}.{key} array values are not supported"
            )));
        }
    })
}

/// Minimal standard-alphabet base64 encode (BLOB columns only; keeps
/// intent-store free of a base64 dependency for a path that in practice
/// never fires — no transfer table declares a BLOB column today).
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Inverse of [`base64_encode`]; `None` on any malformed input.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for chunk in s.chunks(4) {
        if chunk.len() == 1 {
            return None;
        }
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= val(c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
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

    /// Rows exported from a seeded workspace re-import into a fresh store and
    /// come back byte-identical on a second export (full JSON round-trip:
    /// every table, every column, every storage class the seed exercises).
    /// `draft` rows are emptied first — drafts FK onto `client`, which is not
    /// part of the transfer set, so the import transform layer drops them
    /// (transient UI state; the owning client never exists on the target).
    #[tokio::test]
    async fn transfer_rows_round_trip_between_stores() {
        let src_db = TempDb::new();
        let src = Store::open(&src_db.path).await.expect("open source");
        seed(&src, "ws-rt").await;
        let ws = WorkspaceId("ws-rt".to_string());

        let mut exported = src.transfer_export_rows(&ws).await.expect("export");
        assert_eq!(exported.len(), TRANSFER_TABLES.len());
        for (table, rows) in &mut exported {
            if table == "draft" {
                assert_eq!(rows.len(), 1, "seed must have exercised draft export");
                rows.clear();
            }
        }
        let total: usize = exported.iter().map(|(_, rows)| rows.len()).sum();
        assert!(total > 0);

        let dst_db = TempDb::new();
        let dst = Store::open(&dst_db.path).await.expect("open target");
        let inserted = dst.transfer_import_rows(&exported).await.expect("import");
        assert_eq!(inserted, total);

        let re_exported = dst.transfer_export_rows(&ws).await.expect("re-export");
        assert_eq!(exported, re_exported, "round-trip must be lossless");
    }

    /// The import transaction is atomic: a batch whose LAST table row
    /// violates the schema leaves nothing behind — not even the earlier
    /// valid workspace row.
    #[tokio::test]
    async fn transfer_import_rolls_back_whole_batch_on_failure() {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open");
        let t = "2026-01-01T00:00:00Z";
        let rows = vec![
            (
                "workspace".to_string(),
                vec![serde_json::json!({
                    "id": "ws-x", "title": "T", "branch": "main",
                    "created_at": t, "updated_at": t
                })],
            ),
            (
                "workspace_ui_context".to_string(),
                // NOT NULL `payload` omitted → insert fails after workspace
                // already inserted inside the same transaction.
                vec![serde_json::json!({ "workspace_id": "ws-x" })],
            ),
        ];
        store
            .transfer_import_rows(&rows)
            .await
            .expect_err("batch must fail");
        let stats = store
            .transfer_table_stats(&WorkspaceId("ws-x".to_string()))
            .await
            .expect("stats");
        assert!(
            stats.iter().all(|s| s.row_count == 0),
            "failed import must leave no rows behind"
        );
    }

    /// Unknown tables — including `event` — are rejected before any write.
    #[tokio::test]
    async fn transfer_import_rejects_non_transfer_tables() {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open");
        for table in ["event", "settings", "nope"] {
            let rows = vec![(table.to_string(), vec![serde_json::json!({})])];
            let err = store.transfer_import_rows(&rows).await.expect_err(table);
            assert!(matches!(err, intent_core::Error::InvalidParams(_)));
        }
    }
}
