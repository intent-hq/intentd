//! Immutable local evidence. It constrains fresh admission; it grants no access.
use super::{AgentId, AgentMessage, Error, Result, Row, Store, WorkspaceId};
use intent_core::review_monitor::ReviewMonitorBinding;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// The exact unprojected message verified against its original local evidence.
/// Consumers must independently admit the original account and recipient, and
/// retain this envelope through final write rather than reloading by ID.
#[derive(Debug, Clone)]
pub struct MonitorMessageProvenance {
    pub message: AgentMessage,
    pub workspace_id: WorkspaceId,
    pub binding: ReviewMonitorBinding,
    pub envelope_sha256: [u8; 32],
}

pub(super) fn envelope_digest(
    message: &AgentMessage,
    workspace: &WorkspaceId,
    binding: &ReviewMonitorBinding,
) -> Result<[u8; 32]> {
    fn canonical(value: Value) -> Value {
        match value {
            Value::Object(object) => {
                let sorted = object
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>();
                Value::Object(
                    sorted
                        .into_iter()
                        .map(|(key, value)| (key, canonical(value)))
                        .collect(),
                )
            }
            Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
            value => value,
        }
    }
    let envelope = canonical(json!([
        "intent-monitor-message-v1",
        message.id,
        message.agent_id,
        workspace,
        message.seq,
        message.role,
        message.created_at,
        message.content,
        message.metadata,
        binding,
    ]));
    let bytes = serde_json::to_vec(&envelope)
        .map_err(|_| Error::Internal("monitor message envelope encoding failed".into()))?;
    Ok(Sha256::digest(bytes).into())
}

impl Store {
    /// Verify local evidence and full hydrated content in one read snapshot.
    /// Missing, legacy, replaced or malformed evidence returns `None`; no row
    /// is qualified from today's monitor state or public message metadata.
    ///
    /// # Errors
    /// Returns database or message decoding errors. Neither permits disclosure.
    pub async fn get_monitor_message_provenance(
        &self,
        agent: &AgentId,
        message_id: &str,
    ) -> Result<Option<MonitorMessageProvenance>> {
        let mut tx = self
            .read_pool()
            .begin()
            .await
            .map_err(|_| Error::Internal("monitor history read failed".into()))?;
        let row = sqlx::query("SELECT p.workspace_id,p.binding_json,p.envelope_sha256,p.version FROM pr_monitor_message_provenance p JOIN agent_session s ON s.id=p.agent_id AND s.workspace_id=p.workspace_id WHERE p.message_id=? AND p.agent_id=?")
            .bind(message_id).bind(&agent.0).fetch_optional(&mut *tx).await
            .map_err(|_| Error::Internal("monitor history read failed".into()))?;
        let Some(row) = row else { return Ok(None) };
        let binding: ReviewMonitorBinding =
            match serde_json::from_str(&row.get::<String, _>("binding_json")) {
                Ok(binding) => binding,
                Err(_) => return Ok(None),
            };
        let digest: Vec<u8> = row.get("envelope_sha256");
        if row.get::<i64, _>("version") != 1 || digest.len() != 32 {
            return Ok(None);
        }
        let workspace_id = WorkspaceId(row.get("workspace_id"));
        let Some(message) = Self::get_agent_message_by_id_in(&mut tx, agent, message_id).await?
        else {
            return Ok(None);
        };
        let actual = envelope_digest(&message, &workspace_id, &binding)?;
        if digest != actual {
            return Ok(None);
        }
        tx.commit()
            .await
            .map_err(|_| Error::Internal("monitor history read failed".into()))?;
        Ok(Some(MonitorMessageProvenance {
            message,
            workspace_id,
            binding,
            envelope_sha256: actual,
        }))
    }
}
