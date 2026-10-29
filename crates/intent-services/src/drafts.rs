//! `services::drafts` — BE-persisted per-client chat drafts (§9.10, §15) and the
//! `client.hello` client-row upsert (§16).
//!
//! The connection→logical-`clientId` binding and the `server` capability block
//! are transport concerns (§16); this module owns only the persistence and the
//! `draft:changed` change event. That event deliberately carries `hasDraft` and
//! **never** the draft text (no leakage, PROTOCOL §5.16/§6.5).

use intent_core::events::{BROWSER_TAB_UPDATED, DRAFT_CHANGED};
use intent_core::{
    now_iso, ActorType, AgentId, ClientHostInfo, ClientId, Draft, EventActor, Result, WorkspaceId,
};
use intent_store::NewEvent;

use crate::{publish_event, Services};

impl Services {
    /// `client.hello` persistence: upsert the logical `client` row, setting
    /// `first_seen` once and touching `last_seen`, persisting `name` /
    /// `capabilities` / the client's host identification (PROTOCOL §5.17).
    pub(crate) async fn client_hello_upsert(
        &self,
        client_id: ClientId,
        name: Option<String>,
        capabilities: Option<serde_json::Value>,
        host: ClientHostInfo,
    ) -> Result<()> {
        self.store
            .upsert_client(&client_id, name.as_deref(), capabilities.as_ref(), &host)
            .await
    }

    /// `client.hello` follow-up (intent-hq/intent#8045): move the open browser
    /// tabs hosted by `superseded` — earlier ids of the hello'd device, which
    /// the transport has already limited to ids this caller may act as — to
    /// `client_id`, publishing one `browser:tab-updated { changes: {
    /// hostClientId, displayed?: null } }` per moved tab so every window
    /// re-acknowledges the host. A superseded id that has a live hello'd
    /// connection right now is skipped: it is still a separate, active host.
    ///
    /// A move must never widen who can act as a tab's host. Besides the
    /// administrator, only principal `P` may hello as an id `P:…`, so when
    /// the head segment of `client_id` names a principal of this daemon, only
    /// superseded ids inside that same `P:` namespace are rehomed: an
    /// administrator-only id never moves to an id a collaborator can claim.
    /// Prefixes left by other backends' principals are unknown here and add
    /// no one. Returns how many tabs moved.
    pub(crate) async fn client_rehome_superseded_tabs(
        &self,
        client_id: ClientId,
        superseded: Vec<ClientId>,
    ) -> Result<usize> {
        if superseded.is_empty() {
            return Ok(0);
        }
        let live: std::collections::HashSet<ClientId> = self
            .reverse_dispatch
            .as_ref()
            .map(|d| d.live_clients().into_iter().map(|c| c.client_id).collect())
            .unwrap_or_default();
        let mut candidates: Vec<ClientId> = superseded
            .into_iter()
            .filter(|id| *id != client_id && !live.contains(id))
            .collect();
        if let Some((head, _)) = client_id.as_str().split_once(':') {
            let head_is_principal = !self
                .store
                .get_principals(&[intent_core::PrincipalId::from_string(head)])
                .await?
                .is_empty();
            if head_is_principal {
                let namespace = format!("{head}:");
                candidates.retain(|id| id.as_str().starts_with(&namespace));
            }
        }
        if candidates.is_empty() {
            return Ok(0);
        }
        let _gate = self.browser_tab_gate.lock().await;
        let moved = self
            .store
            .reassign_browser_tabs_from_hosts(&client_id, &candidates)
            .await?;
        for (tab, changes) in &moved {
            self.desktop.assignment_changed(&tab.workspace_id);
            publish_event(
                self.event_bus.as_ref(),
                NewEvent {
                    workspace_id: tab.workspace_id.clone(),
                    timestamp: now_iso(),
                    event_type: BROWSER_TAB_UPDATED.to_string(),
                    actor: EventActor {
                        actor_type: ActorType::User,
                        id: Some(client_id.0.clone()),
                        ..EventActor::default()
                    },
                    session_id: None,
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data: serde_json::json!({ "tab": tab, "changes": changes }),
                },
            )
            .await;
        }
        if !moved.is_empty() {
            self.desktop.changed.notify_waiters();
        }
        Ok(moved.len())
    }

    /// Anonymous-connection draft placeholder: ensure the `client` row exists
    /// for a connection-minted id without recording a hello (PROTOCOL §5.16).
    pub(crate) async fn client_ensure(&self, client_id: ClientId) -> Result<()> {
        self.store.ensure_client(&client_id).await
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
            self.event_bus.as_ref(),
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
            self.event_bus.as_ref(),
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
