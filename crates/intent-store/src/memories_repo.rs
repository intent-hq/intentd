//! Long-term agent memory repository (§9.2, §9.12, §18.5). Internal-only: there
//! is no `memories.*` RPC in v1; rows are written/read internally and back the
//! `search.memories` adapter (PROTOCOL §5.15). A `NULL` `workspace_id` denotes a
//! global memory.

use intent_core::{now_iso, Error, Memory, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{tags_from_db, tags_to_db, Store};

const MEMORY_COLUMNS: &str = "id, workspace_id, content, tags, created_at, updated_at";

impl Store {
    /// Insert a memory row. `created_at` defaults to now when empty; `updated_at`
    /// is left as written (`None` for a fresh row).
    pub async fn insert_memory(&self, memory: &Memory) -> Result<()> {
        let workspace_id = memory.workspace_id.as_ref().map(|w| w.0.clone());
        let created_at = if memory.created_at.is_empty() {
            now_iso()
        } else {
            memory.created_at.clone()
        };
        let sql = format!("INSERT INTO memories ({MEMORY_COLUMNS}) VALUES (?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&memory.id)
            .bind(workspace_id)
            .bind(&memory.content)
            .bind(tags_to_db(&memory.tags)?)
            .bind(created_at)
            .bind(&memory.updated_at)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("insert memory failed: {e}")))?;
        Ok(())
    }

    /// List memories, oldest first. With `Some(ws)` the result is scoped to that
    /// workspace; with `None` every memory across workspaces is returned. Backs
    /// the `search.memories` adapter (PROTOCOL §5.15).
    pub async fn list_memories(&self, workspace_id: Option<&WorkspaceId>) -> Result<Vec<Memory>> {
        let rows = match workspace_id {
            Some(ws) => {
                let sql = format!(
                    "SELECT {MEMORY_COLUMNS} FROM memories WHERE workspace_id = ? ORDER BY created_at"
                );
                sqlx::query(&sql)
                    .bind(&ws.0)
                    .fetch_all(self.pool())
                    .await
            }
            None => {
                let sql = format!("SELECT {MEMORY_COLUMNS} FROM memories ORDER BY created_at");
                sqlx::query(&sql).fetch_all(self.pool()).await
            }
        }
        .map_err(|e| Error::Internal(format!("list memories failed: {e}")))?;
        rows.iter().map(map_memory_row).collect()
    }
}

fn map_memory_row(r: &SqliteRow) -> Result<Memory> {
    let workspace_id: Option<String> = r.get("workspace_id");
    Ok(Memory {
        id: r.get("id"),
        workspace_id: workspace_id.map(WorkspaceId),
        content: r.get("content"),
        tags: tags_from_db(&r.get::<String, _>("tags"))?,
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    })
}
