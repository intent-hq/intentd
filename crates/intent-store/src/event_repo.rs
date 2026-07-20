//! Event repository: append-only insert + read-only queries (§9.2 / §10).
//!
//! The log is append-only: this module exposes [`Store::insert_event`] and a
//! set of query methods, but deliberately **no** update or delete path. Ids are
//! minted as UUIDv7 so they sort by creation time. `actor` and `data` are stored
//! as JSON (`actor` / `data_json` columns), round-tripping the full TS/iOS wire
//! shape; filters over them use SQLite's `json_extract`.

use intent_core::{ActorType, Error, Event, EventActor, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::{QueryBuilder, Row, Sqlite};
use uuid::Uuid;

use crate::{enum_to_db, Store};

const EVENT_COLUMNS: &str = "id, workspace_id, timestamp, event_type, actor, session_id, \
    correlation_id, parent_event_id, metadata_json, data_json";

/// Input to [`Store::insert_event`]: an event without its id. The repository
/// mints a UUIDv7 `id` and returns the persisted [`Event`].
#[derive(Debug, Clone)]
pub struct NewEvent {
    pub workspace_id: WorkspaceId,
    pub timestamp: String,
    pub event_type: String,
    pub actor: EventActor,
    pub session_id: Option<String>,
    pub correlation_id: Option<String>,
    pub parent_event_id: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub data: serde_json::Value,
}

/// Filter for [`Store::query_events`]. All set fields are AND-combined; unset
/// fields are ignored. Results are ordered newest-first (`timestamp` DESC).
#[derive(Debug, Clone, Default)]
pub struct EventQuery {
    pub workspace_id: Option<WorkspaceId>,
    pub event_types: Vec<String>,
    pub actor_type: Option<ActorType>,
    pub actor_id: Option<String>,
    pub session_id: Option<String>,
    pub correlation_id: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub path_prefix: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

impl Store {
    /// Append an event to the log, minting a UUIDv7 id, and return it.
    pub async fn insert_event(&self, ev: &NewEvent) -> Result<Event> {
        let id = Uuid::now_v7().to_string();
        let actor_json = serde_json::to_string(&ev.actor)
            .map_err(|e| Error::Internal(format!("encode actor failed: {e}")))?;
        let data_json = serde_json::to_string(&ev.data)
            .map_err(|e| Error::Internal(format!("encode data_json failed: {e}")))?;
        let metadata_json = ev
            .metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| Error::Internal(format!("encode metadata_json failed: {e}")))?;
        let sql = format!("INSERT INTO event ({EVENT_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&id)
            .bind(&ev.workspace_id.0)
            .bind(&ev.timestamp)
            .bind(&ev.event_type)
            .bind(&actor_json)
            .bind(&ev.session_id)
            .bind(&ev.correlation_id)
            .bind(&ev.parent_event_id)
            .bind(&metadata_json)
            .bind(&data_json)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert event failed: {e}")))?;
        Ok(Event {
            id,
            workspace_id: ev.workspace_id.clone(),
            timestamp: ev.timestamp.clone(),
            event_type: ev.event_type.clone(),
            actor: ev.actor.clone(),
            session_id: ev.session_id.clone(),
            correlation_id: ev.correlation_id.clone(),
            parent_event_id: ev.parent_event_id.clone(),
            metadata: ev.metadata.clone(),
            data: ev.data.clone(),
        })
    }

    /// Batch-insert multiple events in a single transaction, preserving order.
    /// Each event gets a freshly minted UUIDv7 id. Returns the persisted events
    /// in insertion order. Empty input returns an empty vec.
    pub async fn insert_events(&self, events: &[NewEvent]) -> Result<Vec<Event>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare all event data: mint ids and encode JSON upfront (fail fast).
        let mut prepared: Vec<(String, String, String, Option<String>)> = Vec::new();
        for ev in events {
            let id = Uuid::now_v7().to_string();
            let actor_json = serde_json::to_string(&ev.actor)
                .map_err(|e| Error::Internal(format!("encode actor failed: {e}")))?;
            let data_json = serde_json::to_string(&ev.data)
                .map_err(|e| Error::Internal(format!("encode data_json failed: {e}")))?;
            let metadata_json = ev
                .metadata
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| Error::Internal(format!("encode metadata_json failed: {e}")))?;
            prepared.push((id, actor_json, data_json, metadata_json));
        }

        // Build multi-row INSERT within a single BEGIN IMMEDIATE transaction.
        // IMMEDIATE mode acquires the exclusive write lock upfront (avoiding the
        // DEFERRED-mode lock upgrade race). With max_connections=1 on the write
        // pool, concurrent insert_events calls serialize at pool.acquire() instead
        // of hitting SQLITE_BUSY during transaction upgrade.
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("acquire connection failed: {e}")))?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("begin IMMEDIATE failed: {e}")))?;

        // Execute multi-row insert; rollback explicitly on error.
        let result = async {
            let mut qb: QueryBuilder<Sqlite> =
                QueryBuilder::new(format!("INSERT INTO event ({EVENT_COLUMNS}) "));
            qb.push_values(events.iter().zip(&prepared), |mut b, (ev, prep)| {
                b.push_bind(&prep.0) // id
                    .push_bind(&ev.workspace_id.0)
                    .push_bind(&ev.timestamp)
                    .push_bind(&ev.event_type)
                    .push_bind(&prep.1) // actor_json
                    .push_bind(&ev.session_id)
                    .push_bind(&ev.correlation_id)
                    .push_bind(&ev.parent_event_id)
                    .push_bind(&prep.3) // metadata_json
                    .push_bind(&prep.2); // data_json
            });

            qb.build()
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("insert events failed: {e}")))
        }
        .await;

        match result {
            Ok(_) => {
                sqlx::query("COMMIT")
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| Error::Internal(format!("commit failed: {e}")))?;
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                return Err(e);
            }
        }

        // Reconstruct Event structs in insertion order.
        let mut result = Vec::with_capacity(events.len());
        for (ev, prep) in events.iter().zip(&prepared) {
            result.push(Event {
                id: prep.0.clone(),
                workspace_id: ev.workspace_id.clone(),
                timestamp: ev.timestamp.clone(),
                event_type: ev.event_type.clone(),
                actor: ev.actor.clone(),
                session_id: ev.session_id.clone(),
                correlation_id: ev.correlation_id.clone(),
                parent_event_id: ev.parent_event_id.clone(),
                metadata: ev.metadata.clone(),
                data: ev.data.clone(),
            });
        }
        Ok(result)
    }

    /// Run a filtered event query (newest-first). See [`EventQuery`].
    pub async fn query_events(&self, q: &EventQuery) -> Result<Vec<Event>> {
        let mut qb: QueryBuilder<Sqlite> =
            QueryBuilder::new(format!("SELECT {EVENT_COLUMNS} FROM event WHERE 1=1"));
        if let Some(ws) = &q.workspace_id {
            qb.push(" AND workspace_id = ").push_bind(ws.0.clone());
        }
        match q.event_types.as_slice() {
            [] => {}
            [one] => {
                qb.push(" AND event_type = ").push_bind(one.clone());
            }
            many => {
                qb.push(" AND event_type IN (");
                let mut sep = qb.separated(", ");
                for t in many {
                    sep.push_bind(t.clone());
                }
                qb.push(")");
            }
        }
        if let Some(at) = &q.actor_type {
            qb.push(" AND json_extract(actor, '$.type') = ")
                .push_bind(enum_to_db(at)?);
        }
        if let Some(aid) = &q.actor_id {
            qb.push(" AND json_extract(actor, '$.id') = ")
                .push_bind(aid.clone());
        }
        if let Some(s) = &q.session_id {
            qb.push(" AND session_id = ").push_bind(s.clone());
        }
        if let Some(c) = &q.correlation_id {
            qb.push(" AND correlation_id = ").push_bind(c.clone());
        }
        if let Some(since) = &q.since {
            qb.push(" AND timestamp >= ").push_bind(since.clone());
        }
        if let Some(until) = &q.until {
            qb.push(" AND timestamp <= ").push_bind(until.clone());
        }
        if let Some(prefix) = &q.path_prefix {
            let pat = format!("{}%", escape_like(prefix));
            qb.push(" AND (json_extract(data_json, '$.path') LIKE ")
                .push_bind(pat.clone())
                .push(" ESCAPE '\\' OR json_extract(data_json, '$.relativePath') LIKE ")
                .push_bind(pat)
                .push(" ESCAPE '\\')");
        }
        qb.push(" ORDER BY timestamp DESC, id DESC");
        if let Some(limit) = q.limit {
            qb.push(" LIMIT ").push_bind(limit);
            if let Some(offset) = q.offset {
                qb.push(" OFFSET ").push_bind(offset);
            }
        }
        let rows = qb
            .build()
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("query events failed: {e}")))?;
        rows.iter().map(map_event_row).collect()
    }

    /// Retention/compaction sweep (§10.2 / finding F4): delete high-volume
    /// ephemeral event families (`agent:stream:*`, `file:*`, `terminal:data`,
    /// `host:exec:*`) whose `timestamp` is strictly older than `cutoff` (an
    /// RFC-3339 string), and return the number of rows removed. Lifecycle/tool/
    /// note/task/workspace events are preserved regardless of age. This is the
    /// sole delete path on the otherwise append-only log; it is deliberately
    /// scoped to high-volume families that can be safely trimmed so the log stays
    /// the source of truth for everything else. Runs as a single statement
    /// (implicitly transactional) and is idempotent — a re-run with the same
    /// cutoff removes nothing more.
    pub async fn delete_ephemeral_events_before(&self, cutoff: &str) -> Result<u64> {
        // High-volume event families eligible for retention sweep:
        // - agent:stream:* — streaming chunks
        // - file:* — file watcher events (finding F4: 87% of event table)
        // - terminal:data — live PTY output
        // - host:exec:* — streaming command output
        let result = sqlx::query(
            "DELETE FROM event WHERE timestamp < ? AND (
                event_type LIKE ? OR
                event_type LIKE ? OR
                event_type = ? OR
                event_type LIKE ?
            )",
        )
        .bind(cutoff)
        .bind(format!("{}%", intent_core::events::AGENT_STREAM_PREFIX))
        .bind("file:%")
        .bind(intent_core::events::TERMINAL_DATA)
        .bind("host:exec:%")
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("ephemeral event retention sweep failed: {e}")))?;
        Ok(result.rows_affected())
    }

    /// Legacy alias for `delete_ephemeral_events_before`. Preserved for
    /// backward compatibility during the transition; new callers should use
    /// `delete_ephemeral_events_before` directly.
    pub async fn delete_stream_events_before(&self, cutoff: &str) -> Result<u64> {
        self.delete_ephemeral_events_before(cutoff).await
    }

    /// Most-recent `file:changed` events for a workspace (newest first).
    pub async fn recent_files(&self, workspace_id: &WorkspaceId, limit: i64) -> Result<Vec<Event>> {
        self.query_events(&EventQuery {
            workspace_id: Some(workspace_id.clone()),
            event_types: vec![intent_core::events::FILE_CHANGED.to_string()],
            limit: Some(limit),
            ..Default::default()
        })
        .await
    }

    /// Most-recent events for a workspace regardless of type (newest first).
    pub async fn events_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
        limit: i64,
    ) -> Result<Vec<Event>> {
        self.query_events(&EventQuery {
            workspace_id: Some(workspace_id.clone()),
            limit: Some(limit),
            ..Default::default()
        })
        .await
    }

    /// Most-recent events of a single `event_type` for a workspace.
    pub async fn events_by_type(
        &self,
        workspace_id: &WorkspaceId,
        event_type: &str,
        limit: i64,
    ) -> Result<Vec<Event>> {
        self.query_events(&EventQuery {
            workspace_id: Some(workspace_id.clone()),
            event_types: vec![event_type.to_string()],
            limit: Some(limit),
            ..Default::default()
        })
        .await
    }

    /// Most-recent `file:changed` events under a directory prefix (newest first).
    pub async fn directory_changes(
        &self,
        workspace_id: &WorkspaceId,
        dir_prefix: &str,
        limit: i64,
    ) -> Result<Vec<Event>> {
        self.query_events(&EventQuery {
            workspace_id: Some(workspace_id.clone()),
            event_types: vec![intent_core::events::FILE_CHANGED.to_string()],
            path_prefix: Some(dir_prefix.to_string()),
            limit: Some(limit),
            ..Default::default()
        })
        .await
    }
}

/// Escape LIKE wildcards so a prefix is matched literally (paired with
/// `ESCAPE '\'` in the query).
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn map_event_row(row: &SqliteRow) -> Result<Event> {
    let actor: EventActor = serde_json::from_str(&col::<String>(row, "actor")?)
        .map_err(|e| Error::Internal(format!("decode actor failed: {e}")))?;
    let data: serde_json::Value = serde_json::from_str(&col::<String>(row, "data_json")?)
        .map_err(|e| Error::Internal(format!("decode data_json failed: {e}")))?;
    let metadata: Option<serde_json::Value> = col::<Option<String>>(row, "metadata_json")?
        .map(|s| serde_json::from_str(&s))
        .transpose()
        .map_err(|e| Error::Internal(format!("decode metadata_json failed: {e}")))?;
    Ok(Event {
        id: col(row, "id")?,
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        timestamp: col(row, "timestamp")?,
        event_type: col(row, "event_type")?,
        actor,
        session_id: col(row, "session_id")?,
        correlation_id: col(row, "correlation_id")?,
        parent_event_id: col(row, "parent_event_id")?,
        metadata,
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::ActorType;
    use serde_json::json;
    use std::path::PathBuf;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("intentd-event-repo-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(PathBuf::from(format!(
                    "{}{}",
                    self.path.display(),
                    suffix
                )));
            }
        }
    }

    fn new_event(event_type: &str, data: serde_json::Value) -> NewEvent {
        NewEvent {
            workspace_id: WorkspaceId::from("ws-test"),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: event_type.to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some("agent-1".to_string()),
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        }
    }

    #[tokio::test]
    async fn insert_events_preserves_order_and_mints_ids() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");

        let events = vec![
            new_event("test:first", json!({"seq": 1})),
            new_event("test:second", json!({"seq": 2})),
            new_event("test:third", json!({"seq": 3})),
        ];

        let inserted = store.insert_events(&events).await.expect("insert_events");
        assert_eq!(inserted.len(), 3, "should return 3 events");

        // Each event should have a unique minted id (UUIDv7).
        for evt in &inserted {
            assert!(!evt.id.is_empty(), "id should be minted");
            assert!(Uuid::parse_str(&evt.id).is_ok(), "id should be valid UUID");
        }

        // All ids should be unique.
        let ids: Vec<&str> = inserted.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(
            ids.len(),
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            "all ids should be unique"
        );

        // Events should match input order.
        assert_eq!(inserted[0].event_type, "test:first");
        assert_eq!(inserted[1].event_type, "test:second");
        assert_eq!(inserted[2].event_type, "test:third");

        // Verify data round-trips correctly.
        assert_eq!(inserted[0].data["seq"], 1);
        assert_eq!(inserted[1].data["seq"], 2);
        assert_eq!(inserted[2].data["seq"], 3);

        // Verify all events are persisted in the store.
        let queried = store
            .query_events(&EventQuery {
                workspace_id: Some(WorkspaceId::from("ws-test")),
                ..Default::default()
            })
            .await
            .expect("query");
        assert_eq!(queried.len(), 3, "all 3 events should be in store");
    }

    #[tokio::test]
    async fn insert_events_empty_input_returns_empty() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");

        let inserted = store.insert_events(&[]).await.expect("insert_events");
        assert_eq!(inserted.len(), 0, "empty input should return empty vec");
    }

    #[tokio::test]
    async fn insert_events_is_transactional() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");

        // Create 10 events in one batch.
        let events: Vec<_> = (0..10)
            .map(|i| new_event("test:batch", json!({"batch_id": i})))
            .collect();

        let inserted = store.insert_events(&events).await.expect("insert_events");
        assert_eq!(inserted.len(), 10, "should insert all 10 events");

        // All 10 should be atomically visible.
        let queried = store
            .query_events(&EventQuery {
                workspace_id: Some(WorkspaceId::from("ws-test")),
                event_types: vec!["test:batch".to_string()],
                ..Default::default()
            })
            .await
            .expect("query");
        assert_eq!(queried.len(), 10, "all 10 events should be visible");
    }
}
