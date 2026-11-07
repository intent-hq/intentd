//! Accepted user-message devices; private routing state, never wire metadata.
use intent_core::{AgentId, ClientId, Error, Result, WorkspaceId};

use crate::Store;

impl Store {
    /// Record a freshly accepted submission once, preserving retry provenance.
    ///
    /// # Errors
    /// Returns an error if the database write fails.
    pub async fn record_browser_sender(
        &self,
        agent_id: &AgentId,
        submission_id: &str,
        received_order: &str,
        client_id: Option<&ClientId>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO agent_browser_sender (agent_id, submission_id, received_order, client_id) VALUES (?, ?, ?, ?)",
        )
        .bind(agent_id.as_str())
        .bind(submission_id)
        .bind(received_order)
        .bind(client_id.map(ClientId::as_str))
        .execute(&self.write_pool)
        .await.map_err(|error| Error::Internal(format!("browser sender persistence: {error}")))?;
        Ok(())
    }

    /// Return the latest accepted human sender; unknown senders clear preference.
    ///
    /// # Errors
    /// Returns an error if the database read fails.
    pub async fn latest_browser_sender(&self, agent_id: &AgentId) -> Result<Option<ClientId>> {
        let client: Option<Option<String>> = sqlx::query_scalar(
            "SELECT client_id FROM agent_browser_sender WHERE agent_id = ? ORDER BY received_order DESC LIMIT 1",
        )
        .bind(agent_id.as_str())
        .fetch_optional(&self.read_pool)
        .await.map_err(|error| Error::Internal(format!("browser sender persistence: {error}")))?;
        Ok(client.flatten().map(ClientId::from))
    }

    /// Resolve the nearest human receipt on this conversation or its delegation
    /// parents. Unknown devices stop inheritance. Sponsor links are not parents.
    /// Reads at most 16 same-workspace rows, stopping on cycles or missing agents.
    ///
    /// # Errors
    /// Returns an error if the database read fails.
    pub async fn preferred_browser_sender(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<ClientId>> {
        let mut current = agent_id.as_str().to_owned();
        let mut visited = std::collections::HashSet::new();
        for _ in 0..16 {
            if !visited.insert(current.clone()) {
                break;
            }
            let row: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT a.parent_agent_id, r.submission_id, r.client_id
                 FROM agent_session a
                 LEFT JOIN agent_browser_sender r ON r.agent_id = a.id AND r.submission_id =
                   (SELECT submission_id FROM agent_browser_sender WHERE agent_id = a.id ORDER BY received_order DESC LIMIT 1)
                 WHERE a.id = ? AND a.workspace_id = ?",
            )
            .bind(&current)
            .bind(workspace_id.as_str())
            .fetch_optional(&self.read_pool)
            .await.map_err(|error| Error::Internal(format!("browser sender inheritance: {error}")))?;
            let Some((parent, submission, client)) = row else {
                break;
            };
            if submission.is_some() {
                return Ok(client.map(ClientId::from));
            }
            let Some(parent) = parent else {
                break;
            };
            current = parent;
        }
        Ok(None)
    }
}
