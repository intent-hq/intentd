//! Accepted browser sender receipts scoped to the store's tenant.

use intent_core::{AgentId, ClientId, Result, WorkspaceId};

use super::{db, PgStore};

impl PgStore {
    pub async fn record_browser_sender(
        &self,
        agent_id: &AgentId,
        submission_id: &str,
        received_order: &str,
        client_id: Option<&ClientId>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO agent_browser_sender (tenant_id, agent_id, submission_id, received_order, client_id)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tenant_id, agent_id, submission_id) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .bind(submission_id)
        .bind(received_order)
        .bind(client_id.map(ClientId::as_str))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn latest_browser_sender(&self, agent_id: &AgentId) -> Result<Option<ClientId>> {
        let mut tx = self.begin().await?;
        let client: Option<Option<String>> = sqlx::query_scalar(
            "SELECT client_id FROM agent_browser_sender WHERE tenant_id = $1 AND agent_id = $2
             ORDER BY received_order COLLATE \"C\" DESC LIMIT 1",
        )
        .bind(self.tenant())
        .bind(agent_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(client.flatten().map(ClientId::from))
    }

    pub async fn preferred_browser_sender(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<ClientId>> {
        let mut tx = self.begin().await?;
        let mut current = agent_id.as_str().to_owned();
        let mut visited = std::collections::HashSet::new();
        let mut preferred = None;
        for _ in 0..16 {
            if !visited.insert(current.clone()) {
                break;
            }
            let row: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT a.parent_agent_id, r.submission_id, r.client_id
                 FROM agent_session a
                 LEFT JOIN agent_browser_sender r ON r.tenant_id = $1 AND r.agent_id = a.id AND r.submission_id =
                   (SELECT submission_id FROM agent_browser_sender WHERE tenant_id = $1 AND agent_id = a.id
                    ORDER BY received_order COLLATE \"C\" DESC LIMIT 1)
                 WHERE a.tenant_id = $1 AND a.id = $2 AND a.workspace_id = $3",
            )
            .bind(self.tenant())
            .bind(&current)
            .bind(workspace_id.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
            let Some((parent, submission, client)) = row else {
                break;
            };
            if submission.is_some() {
                preferred = client.map(ClientId::from);
                break;
            }
            let Some(parent) = parent else {
                break;
            };
            current = parent;
        }
        tx.commit().await.map_err(db)?;
        Ok(preferred)
    }
}
