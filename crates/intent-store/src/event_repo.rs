//! Event repository: append-only insert + read-only queries (§9.2 / §10).
//!
//! The log is append-only: this module exposes [`Store::insert_event`] and a
//! set of query methods, but deliberately **no** update or delete path. Ids are
//! minted as `UUIDv7` so they sort by creation time. `actor` and `data` are stored
//! as JSON (`actor` / `data_json` columns), round-tripping the full TS/iOS wire
//! shape; filters over them use `SQLite`'s `json_extract`.

use intent_core::{ActorType, Error, Event, EventActor, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::{QueryBuilder, Row, Sqlite};
use uuid::Uuid;

use crate::{enum_to_db, Store};

const EVENT_COLUMNS: &str = "id, workspace_id, timestamp, event_type, actor, session_id, \
    correlation_id, parent_event_id, metadata_json, data_json";

/// Max rows removed per retention DELETE statement. Each chunk is its own
/// implicit transaction, so the single-connection write pool is released
/// between chunks and concurrent writers stay responsive (the old
/// single-statement full-table-scan sweep held the pool 2–3.5s on a 1.2GB DB).
pub(crate) const RETENTION_DELETE_CHUNK: i64 = 1000;

/// Input to [`Store::insert_event`]: an event without its id. The repository
/// mints a `UUIDv7` `id` and returns the persisted [`Event`].
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
    /// Prefix match over `event_type` (e.g. `"note:"` for the note category),
    /// compiled to a case-sensitive half-open range scan
    /// (`event_type >= '<prefix>' AND event_type < '<upper>'`) served by the
    /// BINARY-collated `idx_event_type_time` index — same pattern as the
    /// retention path's [`Store::delete_type_prefix_before`].
    pub event_type_prefix: Option<String>,
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
    /// Append an event to the log, minting a `UUIDv7` id, and return it.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    /// Each event gets a freshly minted `UUIDv7` id. Returns the persisted events
    /// in insertion order. Empty input returns an empty vec.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
        // NOTE: this exact wording ("acquire connection failed: …", with sqlx's
        // "pool timed out" inside) is matched by `is_transient_insert_error` in
        // intent-services' events/bus.rs to classify the failure as retryable —
        // keep them in sync if the message changes.
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

        // COMMIT on success (with rollback, and detach+close on double
        // failure, if the COMMIT itself fails — monorepo#670) or roll back
        // the failed body (monorepo#680), so the sole write-pool connection
        // is never returned holding an open transaction.
        crate::commit_with_rollback_guard(conn, result, "commit failed").await?;

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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
        if let Some(prefix) = &q.event_type_prefix {
            // Half-open range instead of LIKE: default LIKE is ASCII
            // case-insensitive (subscribe's `event_type_matches` is
            // case-sensitive `starts_with`) and cannot be served by the
            // BINARY-collated `idx_event_type_time` index (see
            // `delete_type_prefix_before`).
            match prefix_upper_bound(prefix) {
                Some(upper) => {
                    qb.push(" AND event_type >= ")
                        .push_bind(prefix.clone())
                        .push(" AND event_type < ")
                        .push_bind(upper);
                }
                None => {
                    // No computable upper bound (non-ASCII or 0x7F-terminated
                    // prefix — unreachable via `event.query`, whose prefixes
                    // always end in `:`): fall back to a case-sensitive
                    // substr comparison, correct but not index-served.
                    qb.push(" AND substr(event_type, 1, ")
                        .push_bind(prefix.chars().count() as i64)
                        .push(") = ")
                        .push_bind(prefix.clone());
                }
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
    /// `host:exec:*`, `script:output`, plus the high-churn state-notification
    /// families listed in the body — including `workspace:updated` and
    /// `workspace:tokenUsage-changed`) whose `timestamp` is strictly older
    /// than `cutoff` (an RFC-3339 string), and return the number of rows
    /// removed. Lifecycle/audit events (`workspace:created`/`deleted`/
    /// `archived`, `agent:created`/`deleted`/`completed`/`failed`, note/task/
    /// comment/git families, ...) are preserved regardless of age
    /// (`agent:tool:call` has its own TTL via
    /// [`Store::delete_tool_call_events_before`]). This is deliberately scoped
    /// to high-volume families that can be safely trimmed so the log stays the
    /// source of truth for everything else. Each family is deleted separately
    /// in index-driven chunks (see
    /// [`Store::delete_events_by_type_range_before`]) so no single write
    /// transaction holds the pool for long. Idempotent — a re-run with the same
    /// cutoff removes nothing more.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if a chunked delete fails.
    pub async fn delete_ephemeral_events_before(&self, cutoff: &str) -> Result<u64> {
        // High-volume event families eligible for retention sweep:
        // - agent:stream:* — streaming chunks
        // - file:* — file watcher events (finding F4: 87% of event table)
        // - terminal:data — live PTY output
        // - host:exec:* — streaming command output
        // - script:output — script PTY output chunks (monorepo#620: same
        //   live-output shape as terminal:data; was leaking pre-fix and
        //   dominated the event table on long-lived daemons). Exact type,
        //   not a `script:` prefix, so `script:state` (lifecycle) survives.
        //
        // `terminal:data`, `script:output`, and the `host:exec:stdout`/
        // `host:exec:stderr` chunks are now published transient
        // (broadcast-only, never persisted) at the emit site; their arms here
        // stay so rows persisted by older daemon versions still age out.
        let mut removed = 0;
        removed += self
            .delete_type_prefix_before(intent_core::events::AGENT_STREAM_PREFIX, cutoff)
            .await?;
        removed += self.delete_type_prefix_before("file:", cutoff).await?;
        removed += self
            .delete_exact_type_before(intent_core::events::TERMINAL_DATA, cutoff)
            .await?;
        removed += self.delete_type_prefix_before("host:exec:", cutoff).await?;
        removed += self
            .delete_exact_type_before(intent_core::events::SCRIPT_OUTPUT, cutoff)
            .await?;
        // High-churn state-notification families (spec P3: 20k+ rows each on
        // the dev seat, never previously swept). Every consumer takes these
        // from the live `events.subscribe` bus and rehydrates current state
        // from its authoritative table (workspace/draft/agent_session/
        // event_subscription/settings rows) — nothing reads them back from
        // the persisted log by type, so a 72h window loses no history that
        // matters. Exact types only: their lifecycle siblings
        // (`workspace:created/deleted/archived`, `agent:created/deleted/
        // completed/failed`, `agent:queue:processing`, ...) remain audit
        // history and are never swept.
        for event_type in [
            intent_core::events::WORKSPACE_UPDATED,
            intent_core::events::DRAFT_CHANGED,
            intent_core::events::AGENT_STATUS_CHANGED,
            intent_core::events::AGENT_IDLE,
            intent_core::events::AGENT_SUBSCRIPTIONS_CHANGED,
            intent_core::events::SETTINGS_CHANGED,
            intent_core::events::WORKSPACE_TOKEN_USAGE_CHANGED,
            intent_core::events::AGENT_QUEUE_UPDATED,
        ] {
            removed += self.delete_exact_type_before(event_type, cutoff).await?;
        }
        Ok(removed)
    }

    /// Retention sweep for `agent:tool:call` events (87% of live data on the
    /// dev seat): delete rows whose `timestamp` is strictly older than `cutoff`
    /// and return the number removed. Kept separate from
    /// [`Store::delete_ephemeral_events_before`] because tool calls carry a
    /// longer TTL (24h) than the ephemeral stream families. No consumer reads
    /// persisted `agent:tool:call` rows beyond bounded recent windows —
    /// conversation replay uses `agent_message`, and live streaming synthesizes
    /// tool blocks from the in-memory bus.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if a chunked delete fails.
    pub async fn delete_tool_call_events_before(&self, cutoff: &str) -> Result<u64> {
        self.delete_exact_type_before(intent_core::events::AGENT_TOOL_CALL, cutoff)
            .await
    }

    /// Chunked delete of one exact `event_type` older than `cutoff`, driven by
    /// `idx_event_type_time` (equality on `event_type`, range on `timestamp`).
    async fn delete_exact_type_before(&self, event_type: &str, cutoff: &str) -> Result<u64> {
        self.delete_events_by_type_range_before(event_type, None, cutoff)
            .await
    }

    /// Chunked delete of an `event_type` prefix family older than `cutoff`.
    /// Uses a half-open range (`event_type >= prefix AND event_type < upper`)
    /// instead of `LIKE` so the BINARY-collated `idx_event_type_time` index
    /// serves the predicate (`SQLite`'s default case-insensitive LIKE cannot use
    /// a BINARY index).
    async fn delete_type_prefix_before(&self, prefix: &str, cutoff: &str) -> Result<u64> {
        let upper = prefix_upper_bound(prefix).expect("ascii retention prefix");
        self.delete_events_by_type_range_before(prefix, Some(&upper), cutoff)
            .await
    }

    /// Core retention delete: remove events matching an `event_type` bound
    /// (`= lower` when `upper` is `None`, else `>= lower AND < upper`) with
    /// `timestamp < cutoff`, in chunks of [`RETENTION_DELETE_CHUNK`] rows.
    /// Each chunk is a single small statement (implicitly transactional) whose
    /// inner SELECT is an `idx_event_type_time` range scan and whose outer
    /// DELETE resolves rowids directly, so the write pool is held only for
    /// milliseconds at a time instead of one long full-table-scan transaction.
    /// Loops until a chunk deletes fewer rows than the limit.
    async fn delete_events_by_type_range_before(
        &self,
        lower: &str,
        upper: Option<&str>,
        cutoff: &str,
    ) -> Result<u64> {
        let mut removed: u64 = 0;
        loop {
            let result = match upper {
                Some(upper) => {
                    sqlx::query(
                        "DELETE FROM event WHERE rowid IN (
                            SELECT rowid FROM event
                            WHERE event_type >= ? AND event_type < ? AND timestamp < ?
                            LIMIT ?
                        )",
                    )
                    .bind(lower)
                    .bind(upper)
                    .bind(cutoff)
                    .bind(RETENTION_DELETE_CHUNK)
                    .execute(self.write_pool())
                    .await
                }
                None => {
                    sqlx::query(
                        "DELETE FROM event WHERE rowid IN (
                            SELECT rowid FROM event
                            WHERE event_type = ? AND timestamp < ?
                            LIMIT ?
                        )",
                    )
                    .bind(lower)
                    .bind(cutoff)
                    .bind(RETENTION_DELETE_CHUNK)
                    .execute(self.write_pool())
                    .await
                }
            }
            .map_err(|e| Error::Internal(format!("event retention sweep failed: {e}")))?;
            let affected = result.rows_affected();
            removed += affected;
            if affected < RETENTION_DELETE_CHUNK as u64 {
                return Ok(removed);
            }
        }
    }

    /// Legacy alias for `delete_ephemeral_events_before`. Preserved for
    /// backward compatibility during the transition; new callers should use
    /// `delete_ephemeral_events_before` directly.
    #[cfg(test)]
    pub(crate) async fn delete_stream_events_before(&self, cutoff: &str) -> Result<u64> {
        self.delete_ephemeral_events_before(cutoff).await
    }

    /// Most-recent events for a workspace regardless of type (newest first).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the query fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the query fails.
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
}

/// Smallest string strictly greater than every string starting with `prefix`,
/// for half-open `[prefix, upper)` index ranges over `event_type`. Increments
/// the final byte. Returns `None` when no valid bound exists — empty or
/// non-ASCII prefixes, or a final byte of 0x7F (incrementing would leave
/// ASCII) — so callers with arbitrary input can fall back safely.
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    if prefix.is_empty() || !prefix.is_ascii() {
        return None;
    }
    let mut bytes = prefix.as_bytes().to_vec();
    let last = bytes.last_mut().expect("non-empty prefix");
    if *last >= 0x7F {
        return None;
    }
    *last += 1;
    Some(String::from_utf8(bytes).expect("ascii prefix"))
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

    /// monorepo#1538: `event_type_prefix` compiles to a case-sensitive
    /// half-open range scan over `event_type` (not LIKE), so it matches
    /// subscribe's `starts_with` semantics exactly: `NOTE:updated` is NOT
    /// matched by prefix `note:`, and `%`/`_` are plain literal bytes
    /// (`no_e:` must not match `note:`, `no%e:` must not match all).
    #[tokio::test]
    async fn query_events_event_type_prefix_matches_category() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        for t in [
            "note:updated",
            "note:deleted",
            "NOTE:updated",
            "workspace:updated",
            "no_e:x",
            "no%e:y",
            "émoji:added",
        ] {
            store
                .insert_event(&new_event(t, json!({})))
                .await
                .expect("insert");
        }

        let types_for = |prefix: &str| {
            let store = store.clone();
            let prefix = prefix.to_string();
            async move {
                let mut types: Vec<String> = store
                    .query_events(&EventQuery {
                        workspace_id: Some(WorkspaceId::from("ws-test")),
                        event_type_prefix: Some(prefix),
                        ..Default::default()
                    })
                    .await
                    .expect("query")
                    .into_iter()
                    .map(|e| e.event_type)
                    .collect();
                types.sort();
                types
            }
        };

        // `note:` matches only the note-category events — case-sensitively:
        // the stored `NOTE:updated` must NOT match (LIKE would have matched
        // it; subscribe's `starts_with` does not).
        assert_eq!(
            types_for("note:").await,
            vec!["note:deleted", "note:updated"]
        );
        // The uppercase prefix matches only the uppercase event.
        assert_eq!(types_for("NOTE:").await, vec!["NOTE:updated"]);
        // `_` in the prefix is a literal byte, not a single-char wildcard.
        assert_eq!(types_for("no_e:").await, vec!["no_e:x"]);
        // `%` in the prefix is a literal byte, not a multi-char wildcard.
        assert_eq!(types_for("no%e:").await, vec!["no%e:y"]);
        // Non-ASCII prefix (no computable upper bound) takes the substr
        // fallback and still matches correctly.
        assert_eq!(types_for("émoji:").await, vec!["émoji:added"]);
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
