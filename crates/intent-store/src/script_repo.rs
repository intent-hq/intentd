//! Script-definition registry (`script.*`, PROTOCOL §5.8). Parity with the
//! FE's `.workspace/scripts.json` persistence: definitions survive a daemon
//! restart and are hydrated into the runtime registry on boot. Runtime state
//! is transient and never persisted — except the `was_running` marker
//! (stored-on-write), which records that a service-mode script was running
//! when the daemon died so hydration can surface `previouslyRunning`.

use std::collections::BTreeMap;

use intent_core::{Error, Result, Script};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, Store};

const SCRIPT_COLUMNS: &str = "id, workspace_id, name, command, cwd, env, mode, category, \
    source, auto_start, created_at, updated_at";

impl Store {
    /// Insert or replace a script definition, keyed on `id` (mirrors the FE
    /// `upsertScript`: an existing id is fully replaced). The replace resets
    /// the `was_running` marker to its default (cleared) — an upserted
    /// definition starts a fresh runtime life.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_script(&self, s: &Script) -> Result<()> {
        let sql = format!(
            "INSERT OR REPLACE INTO script ({SCRIPT_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        sqlx::query(&sql)
            .bind(&s.id)
            .bind(&s.workspace_id)
            .bind(&s.name)
            .bind(&s.command)
            .bind(&s.cwd)
            .bind(env_to_db(s)?)
            .bind(enum_to_db(&s.mode)?)
            .bind(&s.category)
            .bind(&s.source)
            .bind(s.auto_start.map(|b| b as i64))
            .bind(&s.created_at)
            .bind(&s.updated_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("upsert script failed: {e}")))?;
        Ok(())
    }

    /// Bulk [`upsert_script`](Self::upsert_script): insert or replace many
    /// definitions in chunked multi-row statements inside one transaction, so
    /// persisting N scripts costs O(1) statements and is all-or-nothing
    /// (intent-hq/monorepo#1778 — the `script.list` bootstrap used to trip the
    /// per-dispatch statement budget with one INSERT per repo-config script).
    /// Chunked to stay under the bundled `SQLite`'s `SQLITE_MAX_VARIABLE_NUMBER`
    /// (32766 since 3.32).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_scripts(&self, scripts: &[Script]) -> Result<()> {
        // Per-row bind count derived from SCRIPT_COLUMNS so the placeholder
        // row and chunk math cannot drift if the persisted set changes.
        // 2048 rows × 12 binds = 24576, well under the 32766 cap; one chunk
        // covers any plausible repo config, so the statement count stays flat.
        const ROWS_PER_STATEMENT: usize = 2048;
        let binds_per_row = SCRIPT_COLUMNS.split(',').count();
        let row = format!("({})", vec!["?"; binds_per_row].join(","));
        let mut tx = self
            .write_pool()
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("bulk upsert scripts begin failed: {e}")))?;
        for chunk in scripts.chunks(ROWS_PER_STATEMENT) {
            let placeholders = vec![row.as_str(); chunk.len()].join(",");
            let sql =
                format!("INSERT OR REPLACE INTO script ({SCRIPT_COLUMNS}) VALUES {placeholders}");
            let mut query = sqlx::query(&sql);
            for s in chunk {
                query = query
                    .bind(&s.id)
                    .bind(&s.workspace_id)
                    .bind(&s.name)
                    .bind(&s.command)
                    .bind(&s.cwd)
                    .bind(env_to_db(s)?)
                    .bind(enum_to_db(&s.mode)?)
                    .bind(&s.category)
                    .bind(&s.source)
                    .bind(s.auto_start.map(|b| b as i64))
                    .bind(&s.created_at)
                    .bind(&s.updated_at);
            }
            query
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("bulk upsert scripts failed: {e}")))?;
        }
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("bulk upsert scripts commit failed: {e}")))?;
        Ok(())
    }

    /// Delete a script definition by `id` (FE `removeScript`). Returns whether
    /// a row was actually removed; deleting an unknown id is not an error.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn remove_script(&self, id: &str) -> Result<bool> {
        let res = sqlx::query("DELETE FROM script WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("remove script failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// List every persisted script definition (all workspaces), oldest first —
    /// the boot-time hydration read.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_all_scripts(&self) -> Result<Vec<Script>> {
        let sql = format!("SELECT {SCRIPT_COLUMNS} FROM script ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list scripts failed: {e}")))?;
        rows.iter().map(map_script_row).collect()
    }

    /// Set or clear the service was-running marker (stored-on-write): set on a
    /// service-mode script's successful start, cleared on user `script.stop`
    /// and natural exit (`script.remove` deletes the row). Scoped to
    /// `workspace_id` — the runtime registry permits the same client-supplied
    /// id in separate workspaces, so an id-only write could mark a row owned
    /// by another workspace. Unknown ids are a no-op, not an error.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn set_script_was_running(
        &self,
        workspace_id: &str,
        id: &str,
        was_running: bool,
    ) -> Result<()> {
        sqlx::query("UPDATE script SET was_running = ? WHERE id = ? AND workspace_id = ?")
            .bind(was_running as i64)
            .bind(id)
            .bind(workspace_id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set script was_running failed: {e}")))?;
        Ok(())
    }

    /// `(workspace_id, id)` pairs of scripts still carrying the was-running
    /// marker — the boot-time hydration read behind the `previouslyRunning`
    /// runtime field. Workspace-qualified because the same client-supplied id
    /// may exist in separate workspaces.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_was_running_script_ids(&self) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query("SELECT workspace_id, id FROM script WHERE was_running = 1")
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list was-running scripts failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|r| (r.get("workspace_id"), r.get("id")))
            .collect())
    }
}

/// Encode the optional `env` string map to a JSON TEXT column.
fn env_to_db(s: &Script) -> Result<Option<String>> {
    s.env
        .as_ref()
        .map(|env| {
            serde_json::to_string(env)
                .map_err(|e| Error::Internal(format!("encode script env failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `env` JSON TEXT column.
fn env_from_db(s: Option<String>) -> Result<Option<BTreeMap<String, String>>> {
    s.map(|json| {
        serde_json::from_str::<BTreeMap<String, String>>(&json)
            .map_err(|e| Error::Internal(format!("decode script env failed: {e}")))
    })
    .transpose()
}

fn map_script_row(r: &SqliteRow) -> Result<Script> {
    let col = |name: &str| -> Result<Option<String>> {
        r.try_get::<Option<String>, _>(name)
            .map_err(|e| Error::Internal(format!("column {name}: {e}")))
    };
    Ok(Script {
        id: r.get("id"),
        workspace_id: r.get("workspace_id"),
        name: r.get("name"),
        command: r.get("command"),
        cwd: col("cwd")?,
        env: env_from_db(col("env")?)?,
        mode: enum_from_db(&r.get::<String, _>("mode"))?,
        category: col("category")?,
        source: r.get("source"),
        auto_start: r
            .try_get::<Option<i64>, _>("auto_start")
            .map_err(|e| Error::Internal(format!("column auto_start: {e}")))?
            .map(|v| v != 0),
        created_at: r.get("created_at"),
        updated_at: col("updated_at")?,
    })
}
