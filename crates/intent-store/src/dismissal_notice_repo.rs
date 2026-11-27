//! Questions-dismissed notice claims (`agent.dismissQuestions`, PROTOCOL
//! §5.5): one row per (agent, dismissed message id) whose notice was sent.
//! The insert is the claim, so concurrent or interleaved dismissals of one
//! id, from any number of processes, deliver one notice. Rows go with the
//! agent (`ON DELETE CASCADE`).

use intent_core::{now_iso, AgentId, Error, Result};

use crate::Store;

impl Store {
    /// Claim the questions-dismissed notice of `message_id` for `agent_id`:
    /// `true` when this call inserted the claim (send the notice), `false`
    /// when the notice was already claimed.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database write fails (for example,
    /// the agent does not exist).
    pub async fn claim_dismissal_notice(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<bool> {
        dispatch!(self, backend => backend.claim_dismissal_notice(agent_id, message_id).await)
    }

    /// Drop the claim of `message_id`'s notice, so a retried dismissal
    /// sends it (the dismissal failed before the notice went out).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database write fails.
    pub async fn release_dismissal_notice(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<()> {
        dispatch!(self, backend => backend.release_dismissal_notice(agent_id, message_id).await)
    }
}

impl crate::SqliteStore {
    pub async fn claim_dismissal_notice(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<bool> {
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO agent_dismissal_notice (agent_id, message_id, created_at) \
             VALUES (?, ?, ?)",
        )
        .bind(agent_id.as_str())
        .bind(message_id)
        .bind(now_iso())
        .execute(&self.write_pool)
        .await
        .map_err(|e| Error::Internal(format!("claim dismissal notice failed: {e}")))?
        .rows_affected();
        Ok(inserted > 0)
    }

    pub async fn release_dismissal_notice(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM agent_dismissal_notice WHERE agent_id = ? AND message_id = ?")
            .bind(agent_id.as_str())
            .bind(message_id)
            .execute(&self.write_pool)
            .await
            .map_err(|e| Error::Internal(format!("release dismissal notice failed: {e}")))?;
        Ok(())
    }
}
