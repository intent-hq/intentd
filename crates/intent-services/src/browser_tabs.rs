//! `services::browser_tabs` — the daemon-owned browser tab registry (REV-2
//! Model 2 & 6, intent-hq/intent#461). Persistence lives in
//! `intent-store::browser_tab_repo`; this module owns the host-reporting
//! semantics' event emission: `browser:tab-opened` / `browser:tab-updated
//! { changes }` / `browser:tab-closed`, workspace-scoped with the
//! self-sufficient `{ tab, changes? }` payload, attributed to the reporting
//! host (`actor.id` = its `clientId`). The connection→host binding (which
//! `clientId` is reporting) is a transport concern.
//!
//! Every mutation holds `Services::browser_tab_gate` from before its store
//! transaction until its last event is published, so the event stream is a
//! serialization of committed outcomes: a subscriber can never see an event
//! for a row state the database has already moved past (e.g. `tab-closed`
//! for an id followed by a stale `tab-opened` from an earlier snapshot).
//! Host reports are low-frequency, so one process-wide gate suffices.

use intent_core::events::{BROWSER_TAB_CLOSED, BROWSER_TAB_OPENED, BROWSER_TAB_UPDATED};
use intent_core::{
    now_iso, ActorType, BrowserTab, BrowserTabInput, BrowserTabUpsertOutcome, ClientId, EventActor,
    Result, WorkspaceId,
};
use intent_store::NewEvent;

use crate::{publish_event, Services};

impl Services {
    /// `browser.listTabs`: open tabs of the workspace, oldest first.
    pub(crate) async fn browser_tabs_list(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<BrowserTab>> {
        self.store.list_browser_tabs(&workspace_id).await
    }

    /// `browser.upsertTab`: host-reported open / navigation / state change.
    /// Emits `browser:tab-opened` for a new row, `browser:tab-updated` with
    /// the field-wise `changes` when something differed, nothing otherwise.
    /// A report naming another `workspaceId` for a known tab is rejected by
    /// the store (`-32602`): tabs do not move between workspaces.
    pub(crate) async fn browser_tab_upsert(
        &self,
        host: ClientId,
        tab: BrowserTabInput,
    ) -> Result<BrowserTab> {
        let _gate = self.browser_tab_gate.lock().await;
        let outcome = self.store.upsert_browser_tab(&host, tab).await?;
        match &outcome {
            BrowserTabUpsertOutcome::Opened(tab) => {
                publish_event(
                    self.event_bus.as_ref(),
                    tab_event(BROWSER_TAB_OPENED, &host, tab, None),
                )
                .await;
            }
            BrowserTabUpsertOutcome::Updated { tab, changes } => {
                publish_event(
                    self.event_bus.as_ref(),
                    tab_event(BROWSER_TAB_UPDATED, &host, tab, Some(changes)),
                )
                .await;
            }
            BrowserTabUpsertOutcome::Unchanged(_) => {}
        }
        Ok(outcome.tab().clone())
    }

    /// `browser.removeTab`: host-reported close. Emits `browser:tab-closed`
    /// when an open row was deleted; unknown ids are a silent no-op.
    pub(crate) async fn browser_tab_remove(&self, host: ClientId, tab_id: String) -> Result<()> {
        let _gate = self.browser_tab_gate.lock().await;
        if let Some(tab) = self.store.remove_browser_tab(&host, &tab_id).await? {
            publish_event(
                self.event_bus.as_ref(),
                tab_event(BROWSER_TAB_CLOSED, &host, &tab, None),
            )
            .await;
        }
        Ok(())
    }

    /// `browser.syncTabs`: reconcile the host's full snapshot (REV-2 Model 6),
    /// emitting one event per created / changed / closed row, and return the
    /// ids the host must drop. A snapshot row naming another `workspaceId`
    /// for a known tab rejects the whole snapshot (`-32602`, nothing written).
    pub(crate) async fn browser_tabs_sync(
        &self,
        host: ClientId,
        tabs: Vec<BrowserTabInput>,
    ) -> Result<Vec<String>> {
        let _gate = self.browser_tab_gate.lock().await;
        let result = self.store.sync_browser_tabs(&host, tabs).await?;
        let bus = self.event_bus.as_ref();
        for tab in &result.opened {
            publish_event(bus, tab_event(BROWSER_TAB_OPENED, &host, tab, None)).await;
        }
        for (tab, changes) in &result.updated {
            publish_event(
                bus,
                tab_event(BROWSER_TAB_UPDATED, &host, tab, Some(changes)),
            )
            .await;
        }
        for tab in &result.closed {
            publish_event(bus, tab_event(BROWSER_TAB_CLOSED, &host, tab, None)).await;
        }
        Ok(result.drop)
    }
}

/// Build a workspace-scoped `browser:tab-*` change event with the
/// self-sufficient `{ tab, changes? }` payload. The actor is the reporting
/// host — `{ type: "user", id: <clientId> }` — since the host client reports
/// on behalf of whoever drives the tab and is the origin every consumer needs
/// to distinguish hosts by.
fn tab_event(
    event_type: &str,
    host: &ClientId,
    tab: &BrowserTab,
    changes: Option<&serde_json::Value>,
) -> NewEvent {
    let mut data = serde_json::json!({ "tab": tab });
    if let Some(changes) = changes {
        data["changes"] = changes.clone();
    }
    NewEvent {
        workspace_id: tab.workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::User,
            id: Some(host.0.clone()),
            ..EventActor::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

#[cfg(test)]
mod tests;
