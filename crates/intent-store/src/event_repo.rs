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
    correlation_id, parent_event_id, data_json";

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
        let sql = format!("INSERT INTO event ({EVENT_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&id)
            .bind(&ev.workspace_id.0)
            .bind(&ev.timestamp)
            .bind(&ev.event_type)
            .bind(&actor_json)
            .bind(&ev.session_id)
            .bind(&ev.correlation_id)
            .bind(&ev.parent_event_id)
            .bind(&data_json)
            .execute(self.pool())
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
            data: ev.data.clone(),
        })
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
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("query events failed: {e}")))?;
        rows.iter().map(map_event_row).collect()
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
    Ok(Event {
        id: col(row, "id")?,
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        timestamp: col(row, "timestamp")?,
        event_type: col(row, "event_type")?,
        actor,
        session_id: col(row, "session_id")?,
        correlation_id: col(row, "correlation_id")?,
        parent_event_id: col(row, "parent_event_id")?,
        data,
    })
}
