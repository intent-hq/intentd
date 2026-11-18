//! A current monitor wake's reserved message write, not an authorization token.
use super::{
    content_col_and_payload_rows, insert_payload_rows, AgentMessage, Error, PreparedContent,
    Result, Row, Store, UsageMessageOrigin, MESSAGE_INSERT_COLUMNS,
};
use crate::{MonitorTargetProvenance, PersistedMonitorTarget, QualifiedPrMonitor};
use intent_core::review_monitor::{ReviewMonitorBinding, ReviewMonitorWakeMetadata};
use serde_json::json;
use sqlx::{QueryBuilder, Sqlite, Transaction};

/// Owns a reserved writer and fully prepared text/metadata/payloads. Preparation
/// grants no authority and makes no message effect. The caller must hold its
/// original source, account and destination fences through `commit`.
pub struct PreparedMonitorMessage {
    inner: Inner,
}

enum Inner {
    Sqlite(Box<SqlitePrepared>),
    #[cfg(feature = "postgres")]
    Postgres(Box<crate::postgres::PreparedPgMonitorMessage>),
}

struct SqlitePrepared {
    transaction: Transaction<'static, Sqlite>,
    expected: QueryBuilder<Sqlite>,
    message: AgentMessage,
    draft: MonitorMessageDraft,
    usage_model: String,
    workspace_id: intent_core::WorkspaceId,
    envelope_sha256: [u8; 32],
}

/// The backend-independent part of a monitor wake message: checked identity
/// and the prepared content, metadata and binding.
pub(crate) struct MonitorMessageDraft {
    pub(crate) blocks: serde_json::Value,
    pub(crate) metadata: serde_json::Value,
    pub(crate) content: PreparedContent,
    pub(crate) metadata_json: String,
    pub(crate) binding_json: String,
}

impl MonitorMessageDraft {
    /// The message at `seq`, as both backends insert it.
    pub(crate) fn message(
        &self,
        id: &str,
        agent_id: &intent_core::AgentId,
        seq: i64,
        created_at: &str,
    ) -> AgentMessage {
        AgentMessage {
            id: id.into(),
            agent_id: agent_id.clone(),
            seq,
            role: "user".into(),
            content: self.blocks.clone(),
            metadata: Some(self.metadata.clone()),
            app_message_id: None,
            author: None,
            created_at: created_at.into(),
        }
    }
}

/// Check one text-only current monitor wake against its captured row and
/// prepare its content. No database access.
pub(crate) async fn draft_monitor_message(
    expected: &QualifiedPrMonitor,
    binding: &ReviewMonitorBinding,
    id: &str,
    text: &str,
    metadata: &ReviewMonitorWakeMetadata,
) -> Result<MonitorMessageDraft> {
    let monitor = expected.monitor();
    let PersistedMonitorTarget::Resolved {
        target,
        provenance: MonitorTargetProvenance::CapturedRequest,
    } = expected.target()
    else {
        return Err(Error::InvalidParams(
            "qualified monitor message requires captured target".into(),
        ));
    };
    if id.is_empty()
        || text.trim().is_empty()
        || metadata.monitor_id != monitor.monitor_id
        || metadata.pr_number != target.number
        || metadata.repo != target.repository.project_path
        || metadata.extension.resource.as_ref() != Some(target)
        || &binding.resource != target
    {
        return Err(Error::InvalidParams(
            "qualified monitor message identity mismatch".into(),
        ));
    }
    let stored_binding: ReviewMonitorBinding = monitor
        .last_snapshot
        .as_deref()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|value| serde_json::from_value(value["monitorBinding"].clone()).ok())
        .ok_or_else(|| Error::InvalidParams("original monitor binding unavailable".into()))?;
    if &stored_binding != binding {
        return Err(Error::InvalidParams(
            "original monitor binding mismatch".into(),
        ));
    }
    let binding_json = serde_json::to_string(binding)
        .map_err(|_| Error::Internal("monitor binding encoding failed".into()))?;
    let metadata = serde_json::to_value(metadata)
        .map_err(|e| Error::Internal(format!("encode monitor message metadata: {e}")))?;
    let blocks = json!([{"type":"text", "text":text, "messageMetadata":metadata}]);
    let content = content_col_and_payload_rows(&blocks).await?;
    let metadata_json = metadata.to_string();
    Ok(MonitorMessageDraft {
        blocks,
        metadata,
        content,
        metadata_json,
        binding_json,
    })
}

impl PreparedMonitorMessage {
    /// Finish on a blocking worker while the caller holds original authority.
    /// No pool reservation, payload compression or callback occurs here.
    /// A stale captured monitor returns `None` without inserting a message.
    ///
    /// # Errors
    /// Returns an SQL or commit failure. The whole transaction rolls back.
    /// # Panics
    /// Panics if invoked on an asynchronous runtime worker.
    pub fn commit(self) -> Result<Option<AgentMessage>> {
        tokio::runtime::Handle::current().block_on(self.finish())
    }

    #[cfg(feature = "postgres")]
    pub(crate) fn postgres(prepared: crate::postgres::PreparedPgMonitorMessage) -> Self {
        Self {
            inner: Inner::Postgres(Box::new(prepared)),
        }
    }

    async fn finish(self) -> Result<Option<AgentMessage>> {
        match self.inner {
            Inner::Sqlite(prepared) => prepared.finish().await,
            #[cfg(feature = "postgres")]
            Inner::Postgres(prepared) => prepared.finish().await,
        }
    }
}

impl SqlitePrepared {
    async fn finish(mut self) -> Result<Option<AgentMessage>> {
        if self
            .expected
            .build()
            .fetch_optional(&mut *self.transaction)
            .await
            .map_err(|error| database(&error))?
            .is_none()
        {
            self.transaction
                .rollback()
                .await
                .map_err(|error| database(&error))?;
            return Ok(None);
        }
        let m = &self.message;
        let sql = format!("INSERT INTO agent_message ({MESSAGE_INSERT_COLUMNS}, usage_model, usage_origin) VALUES (?,?,?,?,?,?,?,?,?)");
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&m.id)
            .bind(&m.agent_id.0)
            .bind(m.seq)
            .bind("user")
            .bind(&self.draft.content.content_json)
            .bind(&self.draft.metadata_json)
            .bind(&m.created_at)
            .bind(&self.usage_model)
            .bind(UsageMessageOrigin::Excluded.as_db())
            .execute(&mut *self.transaction)
            .await
            .map_err(|error| database(&error))?;
        insert_payload_rows(
            &mut self.transaction,
            &m.id,
            &m.agent_id.0,
            &self.draft.content.payload_rows,
        )
        .await?;
        sqlx::query("INSERT INTO pr_monitor_message_provenance(message_id,agent_id,workspace_id,binding_json,envelope_sha256,version) VALUES (?,?,?,?,?,1)")
            .bind(&m.id).bind(&m.agent_id.0).bind(&self.workspace_id.0)
            .bind(&self.draft.binding_json).bind(self.envelope_sha256.as_slice())
            .execute(&mut *self.transaction).await.map_err(|error| database(&error))?;
        // Shared session previews have no original recipient admission. Keep
        // their previous ordinary-message projection; never cache private text.
        sqlx::query("UPDATE agent_session SET updated_at = ? WHERE id = ?")
            .bind(&m.created_at)
            .bind(&m.agent_id.0)
            .execute(&mut *self.transaction)
            .await
            .map_err(|error| database(&error))?;
        self.transaction
            .commit()
            .await
            .map_err(|error| database(&error))?;
        Ok(Some(self.message))
    }
}

fn database(error: &sqlx::Error) -> Error {
    Error::Internal(format!("qualified monitor message write: {error}"))
}

impl Store {
    /// Prepare one text-only current monitor wake against its complete captured
    /// row. Retain the returned transaction until admitted commit or drop.
    /// This does not consume pending changes, accept delivery or grant access.
    ///
    /// # Errors
    /// Rejects inconsistent/unresolved identity, missing/retired destination,
    /// serialization failures or writer reservation failure.
    pub async fn prepare_monitor_message(
        &self,
        expected: &QualifiedPrMonitor,
        binding: &ReviewMonitorBinding,
        id: &str,
        text: &str,
        metadata: &ReviewMonitorWakeMetadata,
        created_at: &str,
    ) -> Result<PreparedMonitorMessage> {
        dispatch!(self, backend => backend.prepare_monitor_message(expected, binding, id, text, metadata, created_at).await)
    }
}

impl crate::SqliteStore {
    pub async fn prepare_monitor_message(
        &self,
        expected: &QualifiedPrMonitor,
        binding: &ReviewMonitorBinding,
        id: &str,
        text: &str,
        metadata: &ReviewMonitorWakeMetadata,
        created_at: &str,
    ) -> Result<PreparedMonitorMessage> {
        let draft = draft_monitor_message(expected, binding, id, text, metadata).await?;
        let monitor = expected.monitor();
        // Reserve SQLite before any application authority lock. Sequence and
        // destination row reads share this same serialized writer snapshot.
        let mut transaction = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| database(&error))?;
        let row = sqlx::query("SELECT COALESCE(NULLIF(resolved_model,''), NULLIF(model,''), 'unknown') AS model, (SELECT COALESCE(MAX(seq), -1) + 1 FROM agent_message WHERE agent_id = ?) AS next FROM agent_session WHERE id = ? AND workspace_id = ? AND retired_at IS NULL")
            .bind(&monitor.agent_id.0).bind(&monitor.agent_id.0).bind(&monitor.workspace_id.0)
            .fetch_optional(&mut *transaction).await.map_err(|error| database(&error))?
            .ok_or_else(|| Error::NotFound("original monitor destination unavailable".into()))?;
        let message = draft.message(id, &monitor.agent_id, row.get("next"), created_at);
        let envelope_sha256 = super::monitor_message_provenance::envelope_digest(
            &message,
            &monitor.workspace_id,
            binding,
        )?;
        Ok(PreparedMonitorMessage {
            inner: Inner::Sqlite(Box::new(SqlitePrepared {
                transaction,
                expected: crate::pr_monitor_repo::captured_monitor_exists(expected),
                message,
                draft,
                usage_model: row.get("model"),
                workspace_id: monitor.workspace_id.clone(),
                envelope_sha256,
            })),
        })
    }
}
