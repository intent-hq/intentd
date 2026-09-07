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

use std::collections::HashMap;

use intent_core::events::{BROWSER_TAB_CLOSED, BROWSER_TAB_OPENED, BROWSER_TAB_UPDATED};
use intent_core::{
    now_iso, ActorType, AgentId, BrowserTab, BrowserTabInput, BrowserTabUpsertOutcome, ClientId,
    Error, EventActor, Result, ReverseDispatchError, ReverseTarget, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};

use crate::{publish_event, Services};

/// The `browser.exec` action the daemon answers itself from the registry.
pub(crate) const LIST_TABS_ACTION: &str = "listTabs";

impl Services {
    /// The [`ReverseTarget`] every agent `browser.exec` (and every routed
    /// claimed-tab request) for `workspace_id` dispatches to — the
    /// workspace's **driving client** (REV-2 Model 10): the explicit pin
    /// (`Pinned`), else the host of the workspace's existing claimed tabs
    /// (`Client`), else the first-connected eligible client (`Default`).
    /// Chief and workspaces the store does not know are unpinned.
    pub(crate) async fn driving_client_target(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<ReverseTarget> {
        if workspace_id.is_chief() {
            return Ok(ReverseTarget::Default);
        }
        match self.store.workspace_browser_client(workspace_id).await {
            Ok(Some(client_id)) => return Ok(ReverseTarget::Pinned(client_id)),
            Ok(None) | Err(Error::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let claimed_host = self
            .store
            .list_browser_tabs(workspace_id)
            .await?
            .into_iter()
            .find(|tab| tab.owner_agent_id.is_some())
            .map(|tab| tab.host_client_id);
        Ok(claimed_host.map_or(ReverseTarget::Default, ReverseTarget::Client))
    }

    /// Where a request about one registered tab goes (REV-2 Model 3 & 5): a
    /// claimed tab lives on its workspace's driving client, an unclaimed one
    /// on its physical host.
    pub(crate) async fn browser_tab_route_target(&self, tab: &BrowserTab) -> Result<ReverseTarget> {
        if tab.owner_agent_id.is_some() {
            self.driving_client_target(&tab.workspace_id).await
        } else {
            Ok(ReverseTarget::Client(tab.host_client_id.clone()))
        }
    }

    /// The open registry row for `tab_id`, or the `-32602` the tab-addressed
    /// `browser.*` methods answer for an unknown / tombstoned id.
    pub(crate) async fn browser_tab_required(
        &self,
        method: &str,
        tab_id: &str,
    ) -> Result<BrowserTab> {
        self.store
            .get_browser_tab(tab_id)
            .await?
            .ok_or_else(|| Error::InvalidParams(format!("{method}: tab not found: {tab_id}")))
    }

    /// Dispatch one `browser.exec` action about `tab` to its routing target
    /// ([`Self::browser_tab_route_target`]) and return the FE's raw envelope.
    /// The attribution `workspaceId` / `tabId` are threaded so the host sees
    /// the client-triggered envelope shape; an offline target is
    /// `Error::Internal` worded by [`Self::browser_dispatch_error_message`].
    pub(crate) async fn browser_tab_dispatch(
        &self,
        method: &str,
        tab: &BrowserTab,
        action: Value,
    ) -> Result<Value> {
        let Some(dispatch) = self.reverse_dispatch.clone() else {
            return Err(Error::Internal(format!("{method}: no client connected")));
        };
        let target = self.browser_tab_route_target(tab).await?;
        let params = json!({
            "workspaceId": tab.workspace_id.0,
            "tabId": tab.tab_id,
            "actions": [action],
        });
        match dispatch.dispatch("browser.exec", params, target).await {
            Ok(response) => Ok(response),
            Err(ReverseDispatchError::NoClient) => {
                Err(Error::Internal(format!("{method}: no client connected")))
            }
            Err(err) => {
                let message = self.browser_dispatch_error_message(err).await;
                Err(Error::Internal(format!("{method}: {message}")))
            }
        }
    }

    /// Word a [`ReverseDispatchError`] for the `browser.*` surfaces (REV-2
    /// Model 5): an offline driving client / host reads `browser client
    /// "<name>" (<clientId>) for this workspace is not connected` whether the
    /// client was pinned or resolved — the name filled from the persisted
    /// `client` row when the registry no longer knows it (a fully offline
    /// client).
    pub(crate) async fn browser_dispatch_error_message(&self, err: ReverseDispatchError) -> String {
        match err {
            ReverseDispatchError::ClientOffline {
                client_id, name, ..
            } => {
                let name = match name {
                    Some(name) => Some(name),
                    None => self
                        .store
                        .get_client(&client_id)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|c| c.name),
                };
                match name {
                    Some(name) => format!(
                        "browser client \"{name}\" ({}) for this workspace is not connected",
                        client_id.as_str()
                    ),
                    None => format!(
                        "browser client {} for this workspace is not connected",
                        client_id.as_str()
                    ),
                }
            }
            other => other.to_string(),
        }
    }

    /// The agent-facing `listTabs` action answered from the registry (REV-2
    /// Model 5): every open tab of `workspace_id` across hosts, filtered by
    /// `scope` (`"mine"` / `"unclaimed"` / `"all"`, default `all`), shaped
    /// with today's FE field names plus `hostClientId` / `hostName` /
    /// `hostConnected`. Returns the per-action result envelope; `mine`
    /// without an agent caller is the FE's structured action error.
    pub(crate) async fn browser_tabs_list_for_agent(
        &self,
        workspace_id: &WorkspaceId,
        scope: Option<&str>,
        agent_id: Option<&AgentId>,
    ) -> Result<Value> {
        let scope = scope.unwrap_or("all");
        if !matches!(scope, "mine" | "unclaimed" | "all") {
            return Err(Error::InvalidParams(format!(
                "browser.exec: listTabs scope must be \"mine\", \"unclaimed\" or \"all\", got \"{scope}\""
            )));
        }
        if scope == "mine" && agent_id.is_none() {
            return Ok(json!({
                "action": LIST_TABS_ACTION,
                "success": false,
                "error": "listTabs scope \"mine\" requires an agent caller (agentId), but this call carries none — user calls have no owned tabs. Use scope \"all\" or \"unclaimed\" instead.",
            }));
        }
        let tabs = self.store.list_browser_tabs(workspace_id).await?;
        let presence: HashMap<ClientId, Option<String>> = self
            .reverse_dispatch
            .as_ref()
            .map(|d| {
                d.live_clients()
                    .into_iter()
                    .map(|c| (c.client_id, c.name))
                    .collect()
            })
            .unwrap_or_default();
        let result: Vec<Value> = tabs
            .iter()
            .filter(|tab| match scope {
                "mine" => tab.owner_agent_id.as_ref() == agent_id,
                "unclaimed" => tab.owner_agent_id.is_none(),
                _ => true,
            })
            .map(|tab| registry_tab_entry(tab, &presence))
            .collect();
        Ok(json!({ "action": LIST_TABS_ACTION, "success": true, "result": result }))
    }

    /// Claim migration (REV-2 Model 5): after the driving client `new_host`
    /// successfully executed an agent's `claimTab` on `tab_id`, re-home the
    /// row there with `agent_id` as owner and publish `browser:tab-updated {
    /// changes: { hostClientId, ownerAgentId } }`. A tab already hosted by
    /// the driving client is left to the host's own report (no event).
    pub(crate) async fn browser_tab_claim_migrate(
        &self,
        tab_id: &str,
        new_host: &ClientId,
        agent_id: Option<&AgentId>,
    ) -> Result<()> {
        let _gate = self.browser_tab_gate.lock().await;
        let Some(current) = self.store.get_browser_tab(tab_id).await? else {
            return Ok(());
        };
        if current.host_client_id == *new_host {
            return Ok(());
        }
        if let Some((tab, changes)) = self
            .store
            .reassign_browser_tab_host(tab_id, new_host, agent_id)
            .await?
        {
            publish_event(
                self.event_bus.as_ref(),
                tab_event(BROWSER_TAB_UPDATED, new_host, &tab, Some(&changes)),
            )
            .await;
        }
        Ok(())
    }

    /// Driving-client switch (REV-2 Model 10): move every claimed tab of
    /// `workspace_id` to `new_host`, publishing one `browser:tab-updated {
    /// changes: { hostClientId } }` per moved tab.
    pub(crate) async fn browser_tabs_migrate_claimed(
        &self,
        workspace_id: &WorkspaceId,
        new_host: &ClientId,
    ) -> Result<()> {
        let _gate = self.browser_tab_gate.lock().await;
        let moved = self
            .store
            .reassign_claimed_browser_tabs(workspace_id, new_host)
            .await?;
        for (tab, changes) in &moved {
            publish_event(
                self.event_bus.as_ref(),
                tab_event(BROWSER_TAB_UPDATED, new_host, tab, Some(changes)),
            )
            .await;
        }
        Ok(())
    }

    /// Daemon-side close (REV-2 Model 6, `browser.closeTab` with `force` or
    /// an offline host): tombstone the row and publish `browser:tab-closed`;
    /// the host is told to drop the id on its next `browser.syncTabs`.
    /// Returns the closed row, `None` when there was no open row.
    pub(crate) async fn browser_tab_force_close(&self, tab_id: &str) -> Result<Option<BrowserTab>> {
        let _gate = self.browser_tab_gate.lock().await;
        let closed = self.store.close_browser_tab(tab_id).await?;
        if let Some(tab) = &closed {
            publish_event(
                self.event_bus.as_ref(),
                tab_event(BROWSER_TAB_CLOSED, &tab.host_client_id, tab, None),
            )
            .await;
        }
        Ok(closed)
    }
}

/// One `listTabs` entry from a registry row: the FE's field names
/// (`tabId` / `workspaceId` / `url` / `requestedUrl?` / `title?` /
/// `ownerAgentId` (`null` when unowned) / `ownerAgentName?` / `mode` +
/// `width` / `height` when emulated / `visibility`) plus the registry's
/// `hostClientId` / `hostName?` / `hostConnected`.
fn registry_tab_entry(tab: &BrowserTab, presence: &HashMap<ClientId, Option<String>>) -> Value {
    let mut entry = serde_json::Map::new();
    entry.insert("tabId".into(), tab.tab_id.clone().into());
    entry.insert("workspaceId".into(), tab.workspace_id.0.clone().into());
    entry.insert("url".into(), tab.url.clone().into());
    if let Some(requested) = &tab.requested_url {
        entry.insert("requestedUrl".into(), requested.clone().into());
    }
    if let Some(title) = &tab.title {
        entry.insert("title".into(), title.clone().into());
    }
    entry.insert("ownerAgentId".into(), json!(tab.owner_agent_id));
    if let Some(name) = &tab.owner_agent_name {
        entry.insert("ownerAgentName".into(), name.clone().into());
    }
    match tab.emulated_size {
        Some(size) => {
            entry.insert("mode".into(), "emulated".into());
            entry.insert("width".into(), size.width.into());
            entry.insert("height".into(), size.height.into());
        }
        None => {
            entry.insert("mode".into(), "native".into());
        }
    }
    entry.insert("visibility".into(), json!(tab.visibility));
    entry.insert("hostClientId".into(), tab.host_client_id.0.clone().into());
    let host = presence.get(&tab.host_client_id);
    entry.insert("hostConnected".into(), Value::Bool(host.is_some()));
    if let Some(name) = host.and_then(Option::as_deref) {
        entry.insert("hostName".into(), name.into());
    }
    Value::Object(entry)
}

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
