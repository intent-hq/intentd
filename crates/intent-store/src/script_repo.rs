//! Script-definition registry (`script.*`, PROTOCOL §5.8). Parity with the
//! FE's `.workspace/scripts.json` persistence: definitions survive a daemon
//! restart and are hydrated into the runtime registry on boot. Runtime state
//! is transient and never persisted.

use std::collections::BTreeMap;

use intent_core::{Error, Result, Script};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, Store};

const SCRIPT_COLUMNS: &str = "id, workspace_id, name, command, cwd, env, mode, category, \
    source, auto_start, created_at, updated_at";

impl Store {
    /// Insert or replace a script definition, keyed on `id` (mirrors the FE
    /// `upsertScript`: an existing id is fully replaced).
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

    /// Delete a script definition by `id` (FE `removeScript`). Returns whether
    /// a row was actually removed; deleting an unknown id is not an error.
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
    pub async fn list_all_scripts(&self) -> Result<Vec<Script>> {
        let sql = format!("SELECT {SCRIPT_COLUMNS} FROM script ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list scripts failed: {e}")))?;
        rows.iter().map(map_script_row).collect()
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
