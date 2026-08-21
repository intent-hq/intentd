//! `services::drafts` — BE-persisted per-client chat drafts (§9.10, §15) and the
//! `client.hello` client-row upsert (§16).
//!
//! The connection→logical-`clientId` binding and the `server` capability block
//! are transport concerns (§16); this module owns only the persistence and the
//! `draft:changed` change event. That event deliberately carries `hasDraft` and
//! **never** the draft text (no leakage, PROTOCOL §5.16/§6.5).

use intent_core::events::DRAFT_CHANGED;
use intent_core::{now_iso, AgentId, ClientId, Draft, EventActor, Result, WorkspaceId};
use intent_store::NewEvent;

use crate::{publish_event, Services};

impl Services {
    /// `client.hello` persistence: upsert the logical `client` row, setting
    /// `first_seen` once and touching `last_seen`, persisting `name` /
    /// `capabilities` (PROTOCOL §5.17).
    pub(crate) async fn client_hello_upsert(
        &self,
        client_id: ClientId,
        name: Option<String>,
        capabilities: Option<serde_json::Value>,
    ) -> Result<()> {
        self.store
            .upsert_client(&client_id, name.as_deref(), capabilities.as_ref())
            .await
    }

    /// `drafts.get`: the calling client's draft for `(workspace, agent)`, or
    /// `None` (PROTOCOL §5.16).
    pub(crate) async fn drafts_get(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> Result<Option<Draft>> {
        self.store
            .get_draft(&workspace_id, &agent_id, &client_id)
            .await
    }

    /// `drafts.set`: upsert (or, for empty `text` with no attachments, clear)
    /// the calling client's draft and emit `draft:changed`. `attachments` is an
    /// opaque JSON array stored verbatim; an empty array is normalized to none.
    /// Returns `Some(updatedAt)` on store, or `None` when cleared (PROTOCOL
    /// §5.16/§6.5).
    pub(crate) async fn drafts_set(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
        text: String,
        attachments: Option<serde_json::Value>,
    ) -> Result<Option<String>> {
        let attachments = attachments.filter(|a| a.as_array().is_some_and(|arr| !arr.is_empty()));
        let updated = if text.is_empty() && attachments.is_none() {
            self.store
                .delete_draft(&workspace_id, &agent_id, &client_id)
                .await?;
            None
        } else {
            Some(
                self.store
                    .upsert_draft(
                        &workspace_id,
                        &agent_id,
                        &client_id,
                        &text,
                        attachments.as_ref(),
                    )
                    .await?,
            )
        };
        publish_event(
            &self.event_bus,
            draft_changed_event(&workspace_id, &agent_id, &client_id, updated.is_some()),
        )
        .await;
        Ok(updated)
    }

    /// `drafts.clear`: delete the calling client's draft (idempotent) and emit
    /// `draft:changed` with `hasDraft: false` (PROTOCOL §5.16/§6.5).
    pub(crate) async fn drafts_clear(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> Result<()> {
        self.store
            .delete_draft(&workspace_id, &agent_id, &client_id)
            .await?;
        publish_event(
            &self.event_bus,
            draft_changed_event(&workspace_id, &agent_id, &client_id, false),
        )
        .await;
        Ok(())
    }
}

/// Build a `draft:changed` change event with the self-sufficient payload
/// `{ workspaceId, agentId, clientId, hasDraft }` (PROTOCOL §6.5). The payload
/// deliberately omits the draft text. The actor is the default `user` (a draft
/// is user-authored input), matching the PROTOCOL §5.16 example.
fn draft_changed_event(
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    client_id: &ClientId,
    has_draft: bool,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: DRAFT_CHANGED.to_string(),
        actor: EventActor::default(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "agentId": agent_id.as_str(),
            "clientId": client_id.as_str(),
            "hasDraft": has_draft,
        }),
    }
}
