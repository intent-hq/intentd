//! Event log and persisted event subscriptions (Postgres). Mirrors
//! `event_repo.rs` and `event_subscription_repo.rs`.
//!
//! JSON filters keep `SQLite`'s `json_extract` semantics: a string field
//! matches a bound string only when it is a JSON string (`jsonb` equality
//! with `to_jsonb`), and escaped NULs are dropped first (`NUL_FREE`). `LIKE`
//! is ASCII case-insensitive in `SQLite`, so both sides are ASCII-lowered;
//! `GLOB` is case-sensitive like Postgres `LIKE`. Text order and ranges use
//! `COLLATE "C"`; `created_at` ties fall back to insertion order (`row_seq`).
//! A negative `LIMIT` means no limit and a negative `OFFSET` means none, as
//! in `SQLite`.

use intent_core::{AgentId, Error, Event, EventActor, Result, WorkspaceId};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use super::agent_messages::nul_free;
use super::{db, PgStore};
use crate::enum_to_db;
use crate::event_repo::{
    escape_like, persisted_event, prefix_upper_bound, seq_from_db, seq_to_db, EventQuery, EventSeq,
    NewEvent, EPHEMERAL_EVENT_TYPES, EPHEMERAL_TYPE_PREFIXES, EVENT_COLUMNS,
    RETENTION_DELETE_CHUNK,
};
use crate::event_subscription_repo::PersistedEventSubscription;

/// `SQLite`'s ASCII-only case folding for `LIKE`.
fn ascii_lower(expr: &str) -> String {
    format!("translate({expr}, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')")
}

/// `json_extract(col, '$.key') = <param>` for a text parameter.
fn json_text_eq(col: &str, key: &str, param: &str) -> String {
    nul_free(&format!(
        "intent_json(NUL_FREE({col})) -> '{key}' = to_jsonb({param}::text)"
    ))
}

/// `json_extract(col, '$.key')` as `SQLite` returns it, as text.
fn json_extract(col: &str, key: &str) -> String {
    nul_free(&format!("sqlite_json_extract(NUL_FREE({col}), '$.{key}')"))
}

enum Arg {
    Text(String),
    Texts(Vec<String>),
    Int(i64),
}

/// Filters appended to a statement whose `$1` is the tenant.
#[derive(Default)]
struct Filters {
    sql: String,
    args: Vec<Arg>,
}

impl Filters {
    /// Adds `arg` and returns its placeholder.
    fn arg(&mut self, arg: Arg) -> String {
        self.args.push(arg);
        format!("${}", self.args.len() + 1)
    }

    fn text(&mut self, value: impl Into<String>) -> String {
        self.arg(Arg::Text(value.into()))
    }

    fn push(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }
}

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn map_event_row(row: &PgRow) -> Result<Event> {
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

/// A new event's minted id and encoded JSON columns.
struct Encoded {
    id: String,
    actor: String,
    data: String,
    metadata: Option<String>,
}

fn encode(ev: &NewEvent) -> Result<Encoded> {
    Ok(Encoded {
        id: Uuid::now_v7().to_string(),
        actor: serde_json::to_string(&ev.actor)
            .map_err(|e| Error::Internal(format!("encode actor failed: {e}")))?,
        data: serde_json::to_string(&ev.data)
            .map_err(|e| Error::Internal(format!("encode data_json failed: {e}")))?,
        metadata: ev
            .metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| Error::Internal(format!("encode metadata_json failed: {e}")))?,
    })
}

fn decode_subscription_row(row: &PgRow) -> Result<PersistedEventSubscription> {
    let event_types_raw: String = row
        .try_get("event_types")
        .map_err(|e| Error::Internal(format!("decode event_types: {e}")))?;
    let event_types: Vec<String> = serde_json::from_str(&event_types_raw)
        .map_err(|e| Error::Internal(format!("parse event_types: {e}")))?;
    let text = |name: &str| {
        row.try_get::<String, _>(name)
            .map_err(|e| Error::Internal(format!("decode {name}: {e}")))
    };
    Ok(PersistedEventSubscription {
        id: text("id")?,
        workspace_id: WorkspaceId::from(text("workspace_id")?.as_str()),
        subscriber_agent_id: AgentId::from(text("subscriber_agent_id")?.as_str()),
        event_types,
        exclude_self: row
            .try_get::<i64, _>("exclude_self")
            .map_err(|e| Error::Internal(format!("decode exclude_self: {e}")))?
            != 0,
        batch_window_ms: row
            .try_get("batch_window_ms")
            .map_err(|e| Error::Internal(format!("decode batch_window_ms: {e}")))?,
        created_at: text("created_at")?,
    })
}

impl PgStore {
    pub async fn insert_event(&self, ev: &NewEvent) -> Result<Event> {
        Ok(self.insert_event_seq(ev).await?.0)
    }

    pub async fn insert_event_seq(&self, ev: &NewEvent) -> Result<(Event, EventSeq)> {
        let enc = encode(ev)?;
        let sql = format!(
            "INSERT INTO event (tenant_id, {EVENT_COLUMNS}) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING row_seq"
        );
        let mut tx = self.begin_write().await?;
        let row_seq: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&enc.id)
            .bind(&ev.workspace_id.0)
            .bind(&ev.timestamp)
            .bind(&ev.event_type)
            .bind(&enc.actor)
            .bind(&ev.session_id)
            .bind(&ev.correlation_id)
            .bind(&ev.parent_event_id)
            .bind(&enc.metadata)
            .bind(&enc.data)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("insert event failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok((persisted_event(ev, enc.id), seq_from_db(row_seq)?))
    }

    pub async fn insert_events(&self, events: &[NewEvent]) -> Result<Vec<Event>> {
        Ok(self
            .insert_events_seq(events)
            .await?
            .into_iter()
            .map(|(ev, _)| ev)
            .collect())
    }

    /// The `row_seq` trigger numbers the rows in `ORDER BY u.ord` order, so
    /// seqs follow the input; `RETURNING` order is unspecified, so they are
    /// paired back up by id.
    pub async fn insert_events_seq(&self, events: &[NewEvent]) -> Result<Vec<(Event, EventSeq)>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let encoded = events.iter().map(encode).collect::<Result<Vec<_>>>()?;
        let texts =
            |f: fn(&NewEvent) -> &String| events.iter().map(|e| f(e).as_str()).collect::<Vec<_>>();
        let optional = |f: fn(&NewEvent) -> &Option<String>| {
            events.iter().map(|e| f(e).as_deref()).collect::<Vec<_>>()
        };
        let mut tx = self.begin_write().await?;
        let rows = sqlx::query(
            "INSERT INTO event (tenant_id, id, workspace_id, timestamp, event_type, actor, \
                 session_id, correlation_id, parent_event_id, metadata_json, data_json) \
             SELECT $1, u.id, u.workspace_id, u.timestamp, u.event_type, u.actor, \
                 u.session_id, u.correlation_id, u.parent_event_id, u.metadata_json, u.data_json \
             FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::text[], \
                 $7::text[], $8::text[], $9::text[], $10::text[], $11::text[]) WITH ORDINALITY \
                 AS u(id, workspace_id, timestamp, event_type, actor, session_id, \
                     correlation_id, parent_event_id, metadata_json, data_json, ord) \
             ORDER BY u.ord \
             RETURNING id, row_seq",
        )
        .bind(self.tenant())
        .bind(encoded.iter().map(|e| e.id.as_str()).collect::<Vec<_>>())
        .bind(
            events
                .iter()
                .map(|e| e.workspace_id.0.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(texts(|e| &e.timestamp))
        .bind(texts(|e| &e.event_type))
        .bind(encoded.iter().map(|e| e.actor.as_str()).collect::<Vec<_>>())
        .bind(optional(|e| &e.session_id))
        .bind(optional(|e| &e.correlation_id))
        .bind(optional(|e| &e.parent_event_id))
        .bind(
            encoded
                .iter()
                .map(|e| e.metadata.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(encoded.iter().map(|e| e.data.as_str()).collect::<Vec<_>>())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("insert events failed: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("commit failed: {e}")))?;
        let mut seqs = std::collections::HashMap::with_capacity(rows.len());
        for row in &rows {
            seqs.insert(
                col::<String>(row, "id")?,
                seq_from_db(col::<i64>(row, "row_seq")?)?,
            );
        }
        events
            .iter()
            .zip(encoded)
            .map(|(ev, enc)| {
                let seq = seqs.get(&enc.id).copied().ok_or_else(|| {
                    Error::Internal(format!("insert events: no row_seq returned for {}", enc.id))
                })?;
                Ok((persisted_event(ev, enc.id), seq))
            })
            .collect()
    }

    /// Served by `idx_event_row_seq`.
    pub async fn events_after(
        &self,
        after: EventSeq,
        limit: i64,
    ) -> Result<Vec<(Event, EventSeq)>> {
        let sql = format!(
            "SELECT row_seq, {EVENT_COLUMNS} FROM event WHERE tenant_id = $1 AND row_seq > $2 \
             ORDER BY row_seq LIMIT CASE WHEN $3 < 0 THEN NULL ELSE $3 END"
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(seq_to_db(after))
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("events after seq failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok((
                    map_event_row(row)?,
                    seq_from_db(col::<i64>(row, "row_seq")?)?,
                ))
            })
            .collect()
    }

    pub async fn max_event_seq(&self) -> Result<EventSeq> {
        let mut tx = self.begin().await?;
        let max: Option<i64> =
            sqlx::query_scalar("SELECT max(row_seq) FROM event WHERE tenant_id = $1")
                .bind(self.tenant())
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("max event seq failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        max.map_or(Ok(0), seq_from_db)
    }

    pub async fn query_events(&self, q: &EventQuery) -> Result<Vec<Event>> {
        let mut f = Filters::default();
        // Desktop event visibility is connection/principal bound; see the
        // `SQLite` query.
        f.push(&format!(
            " AND ({} NOT LIKE 'desktop:%'",
            ascii_lower("event_type")
        ));
        if let Some(intent_core::Caller::Wire { principal_id, .. }) = intent_core::current_caller()
        {
            let principal = f.text(principal_id.0);
            f.push(&format!(
                " OR ({} AND ({} IS NULL",
                json_text_eq("metadata_json", "desktopPrincipalId", &principal),
                json_extract("metadata_json", "desktopConnectionEpoch")
            ));
            if let Some(connection) = intent_core::desktop::current_connection() {
                let epoch = f.text(connection.connection_epoch);
                f.push(&format!(
                    " OR {}",
                    json_text_eq("metadata_json", "desktopConnectionEpoch", &epoch)
                ));
            }
            f.push(")))");
        } else {
            f.push(")");
        }
        if q.exclude_discussion_events {
            f.push(" AND event_type NOT LIKE 'discussion:%'");
        }
        if let Some(principal) = &q.client_principal_id {
            let kinds = f.arg(Arg::Texts(
                intent_core::events::CLIENT_EVENT_TYPES
                    .iter()
                    .map(|k| (*k).to_owned())
                    .collect(),
            ));
            let principal = f.text(principal.as_str());
            f.push(&format!(
                " AND (event_type <> ALL({kinds}::text[]) OR {})",
                json_text_eq("data_json", "principalId", &principal)
            ));
        }
        if let Some(principal) = &q.guest_permission_principal_id {
            let request = f.text(intent_core::events::AGENT_PERMISSION_REQUEST);
            let resolved = f.text(intent_core::events::AGENT_PERMISSION_RESOLVED);
            let principal = f.text(principal.as_str());
            f.push(&format!(
                " AND (event_type NOT IN ({request}, {resolved}) OR EXISTS (SELECT 1 \
                 FROM workspace_member m WHERE m.tenant_id = $1 \
                 AND m.workspace_id = event.workspace_id AND m.principal_id = {principal} \
                 AND m.role = 'owner'))"
            ));
        }
        if let Some(ws) = &q.workspace_id {
            let p = f.text(ws.0.as_str());
            f.push(&format!(" AND workspace_id = {p}"));
        }
        if !q.event_types.is_empty() {
            let p = f.arg(Arg::Texts(q.event_types.clone()));
            f.push(&format!(" AND event_type = ANY({p}::text[])"));
        }
        if let Some(prefix) = &q.event_type_prefix {
            let lower = f.text(prefix.as_str());
            if let Some(upper) = prefix_upper_bound(prefix) {
                let upper = f.text(upper);
                f.push(&format!(
                    " AND event_type COLLATE \"C\" >= {lower} AND event_type COLLATE \"C\" < {upper}"
                ));
            } else {
                let len = f.arg(Arg::Int(
                    i64::try_from(prefix.chars().count()).unwrap_or(i64::MAX),
                ));
                f.push(&format!(
                    " AND substr(event_type, 1, LEAST({len}, 2147483647)::int) = {lower}"
                ));
            }
        }
        if let Some(at) = &q.actor_type {
            let p = f.text(enum_to_db(at)?);
            f.push(&format!(" AND {}", json_text_eq("actor", "type", &p)));
        }
        if let Some(aid) = &q.actor_id {
            let p = f.text(aid.as_str());
            f.push(&format!(" AND {}", json_text_eq("actor", "id", &p)));
        }
        if let Some(s) = &q.session_id {
            let p = f.text(s.as_str());
            f.push(&format!(" AND session_id = {p}"));
        }
        if let Some(c) = &q.correlation_id {
            let p = f.text(c.as_str());
            f.push(&format!(" AND correlation_id = {p}"));
        }
        if let Some(since) = &q.since {
            let p = f.text(since.as_str());
            f.push(&format!(" AND timestamp COLLATE \"C\" >= {p}"));
        }
        if let Some(until) = &q.until {
            let p = f.text(until.as_str());
            f.push(&format!(" AND timestamp COLLATE \"C\" <= {p}"));
        }
        if let Some(prefix) = &q.path_prefix {
            let pat = f.text(format!("{}%", escape_like(prefix)));
            let pat = ascii_lower(&pat);
            f.push(&format!(
                " AND ({} LIKE {pat} ESCAPE '\\' OR {} LIKE {pat} ESCAPE '\\')",
                ascii_lower(&json_extract("data_json", "path")),
                ascii_lower(&json_extract("data_json", "relativePath")),
            ));
        }
        f.push(" ORDER BY timestamp COLLATE \"C\" DESC, id COLLATE \"C\" DESC");
        if let Some(limit) = q.limit {
            let p = f.arg(Arg::Int(limit));
            f.push(&format!(" LIMIT CASE WHEN {p} < 0 THEN NULL ELSE {p} END"));
            if let Some(offset) = q.offset {
                let p = f.arg(Arg::Int(offset));
                f.push(&format!(" OFFSET GREATEST({p}, 0)"));
            }
        }
        let sql = format!(
            "SELECT {EVENT_COLUMNS} FROM event WHERE tenant_id = $1{}",
            f.sql
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        for arg in f.args {
            query = match arg {
                Arg::Text(v) => query.bind(v),
                Arg::Texts(v) => query.bind(v),
                Arg::Int(v) => query.bind(v),
            };
        }
        let mut tx = self.begin().await?;
        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("query events failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_event_row).collect()
    }

    pub async fn delete_ephemeral_events_before(&self, cutoff: &str) -> Result<u64> {
        let mut removed = 0;
        for prefix in EPHEMERAL_TYPE_PREFIXES {
            let upper = prefix_upper_bound(prefix).expect("ascii retention prefix");
            removed += self
                .delete_events_by_type_range_before(prefix, Some(&upper), cutoff)
                .await?;
        }
        for event_type in EPHEMERAL_EVENT_TYPES {
            removed += self
                .delete_events_by_type_range_before(event_type, None, cutoff)
                .await?;
        }
        Ok(removed)
    }

    pub async fn delete_tool_call_events_before(&self, cutoff: &str) -> Result<u64> {
        self.delete_events_by_type_range_before(intent_core::events::AGENT_TOOL_CALL, None, cutoff)
            .await
    }

    /// The chunked retention delete: each chunk of at most
    /// [`RETENTION_DELETE_CHUNK`] rows is its own writer transaction, so
    /// the tenant's other writers run between chunks, and a fenced tenant's
    /// next chunk fails `NotOwner`.
    async fn delete_events_by_type_range_before(
        &self,
        lower: &str,
        upper: Option<&str>,
        cutoff: &str,
    ) -> Result<u64> {
        let limit = super::purge::probe::batch_rows(self.tenant(), RETENTION_DELETE_CHUNK);
        let mut removed: u64 = 0;
        loop {
            let mut tx = self.begin_write().await?;
            let result = match upper {
                Some(upper) => {
                    sqlx::query(
                        "DELETE FROM event WHERE tenant_id = $1 AND id IN ( \
                             SELECT id FROM event WHERE tenant_id = $1 \
                             AND event_type COLLATE \"C\" >= $2 AND event_type COLLATE \"C\" < $3 \
                             AND timestamp COLLATE \"C\" < $4 LIMIT $5)",
                    )
                    .bind(self.tenant())
                    .bind(lower)
                    .bind(upper)
                    .bind(cutoff)
                    .bind(limit)
                    .execute(&mut *tx)
                    .await
                }
                None => {
                    sqlx::query(
                        "DELETE FROM event WHERE tenant_id = $1 AND id IN ( \
                             SELECT id FROM event WHERE tenant_id = $1 \
                             AND event_type = $2 AND timestamp COLLATE \"C\" < $3 LIMIT $4)",
                    )
                    .bind(self.tenant())
                    .bind(lower)
                    .bind(cutoff)
                    .bind(limit)
                    .execute(&mut *tx)
                    .await
                }
            }
            .map_err(|e| Error::Internal(format!("event retention sweep failed: {e}")))?;
            tx.commit().await.map_err(db)?;
            let affected = result.rows_affected();
            removed += affected;
            if affected > 0 {
                super::purge::probe::after_table_batch(self.tenant(), "event", affected).await?;
            }
            if affected < limit.unsigned_abs() {
                return Ok(removed);
            }
            tokio::task::yield_now().await;
        }
    }

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

    pub async fn upsert_event_subscription(&self, s: &PersistedEventSubscription) -> Result<()> {
        let event_types = serde_json::to_string(&s.event_types)
            .map_err(|e| Error::Internal(format!("encode event_types: {e}")))?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO event_subscription (tenant_id, id, workspace_id, subscriber_agent_id, \
                 event_types, exclude_self, batch_window_ms, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (tenant_id, id) DO UPDATE SET \
                 event_types = excluded.event_types, \
                 exclude_self = excluded.exclude_self, \
                 batch_window_ms = excluded.batch_window_ms",
        )
        .bind(self.tenant())
        .bind(&s.id)
        .bind(&s.workspace_id.0)
        .bind(&s.subscriber_agent_id.0)
        .bind(event_types)
        .bind(i64::from(s.exclude_self))
        .bind(s.batch_window_ms)
        .bind(&s.created_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("upsert event_subscription failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_event_subscriptions(&self) -> Result<Vec<PersistedEventSubscription>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, workspace_id, subscriber_agent_id, event_types, \
                 exclude_self, batch_window_ms, created_at \
             FROM event_subscription WHERE tenant_id = $1 \
             ORDER BY created_at COLLATE \"C\", row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("list event subscriptions: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(decode_subscription_row).collect()
    }

    pub async fn delete_event_subscription(&self, id: &str) -> Result<()> {
        self.delete_event_subscriptions_by("id", id).await
    }

    pub async fn delete_event_subscriptions_for_agent(
        &self,
        subscriber_agent_id: &AgentId,
    ) -> Result<()> {
        self.delete_event_subscriptions_by("subscriber_agent_id", &subscriber_agent_id.0)
            .await
    }

    pub async fn delete_event_subscriptions_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<()> {
        self.delete_event_subscriptions_by("workspace_id", &workspace_id.0)
            .await
    }

    /// Deletes the tenant's event subscriptions whose `column` is `key`.
    async fn delete_event_subscriptions_by(&self, column: &str, key: &str) -> Result<()> {
        let what = match column {
            "id" => "delete event_subscription failed",
            "subscriber_agent_id" => "delete event_subscriptions for agent failed",
            _ => "delete event_subscriptions for workspace failed",
        };
        let sql = format!("DELETE FROM event_subscription WHERE tenant_id = $1 AND {column} = $2");
        let mut tx = self.begin_write().await?;
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(key)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("{what}: {e}")))?;
        tx.commit().await.map_err(db)
    }
}
