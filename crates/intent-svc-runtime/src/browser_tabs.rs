//! `services::browser_tabs` — the daemon-owned browser tab registry (REV-2
//! Model 2 & 6, intent-hq/intent#461). Persistence lives in
//! `intent-store::browser_tab_repo`; this module owns the host-reporting
//! semantics' event emission: `browser:tab-opened` / `browser:tab-updated
//! { changes }` / `browser:tab-closed`, workspace-scoped with the
//! self-sufficient `{ tab, changes? }` payload, attributed to the reporting
//! host (`actor.id` = its `clientId`) unless the report arrives on a bound
//! wire principal's connection, in which case the event bus re-stamps the
//! actor with that principal (multiplayer w4) and the host stays
//! identifiable through `tab.hostClientId`. The connection→host binding
//! (which `clientId` is reporting) is a transport concern.
//!
//! Every mutation holds `Services::browser_tab_gate` from before its store
//! transaction until its last event is published, so the event stream is a
//! serialization of committed outcomes: a subscriber can never see an event
//! for a row state the database has already moved past (e.g. `tab-closed`
//! for an id followed by a stale `tab-opened` from an earlier snapshot).
//! Host reports are low-frequency, so one process-wide gate suffices.

use std::collections::{HashMap, HashSet};

use intent_core::events::{BROWSER_TAB_CLOSED, BROWSER_TAB_OPENED, BROWSER_TAB_UPDATED};
use intent_core::{
    now_iso, ActorType, AgentId, BrowserTab, BrowserTabInput, BrowserTabUpsertOutcome, ClientId,
    Error, EventActor, PrincipalId, Result, ReverseDispatchError, ReverseTarget, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};

use intent_services::feature_api::{publish_event, Services};

/// The `browser.exec` action the daemon answers itself from the registry.
pub(crate) const LIST_TABS_ACTION: &str = "listTabs";

/// Browser-tab routing and agent-side tab operations, on [`Services`].
pub(crate) trait BrowserTabRoutingExt {
    async fn browser_tab_route_target(&self, tab: &BrowserTab) -> Result<ReverseTarget>;
    async fn browser_tab_required(&self, method: &str, tab_id: &str) -> Result<BrowserTab>;
    async fn require_browser_workspace(&self, workspace: &WorkspaceId) -> Result<()>;
    async fn browser_tab_dispatch(
        &self,
        method: &str,
        tab: &BrowserTab,
        action: Value,
    ) -> Result<Value>;
    async fn browser_dispatch_error_message(&self, err: ReverseDispatchError) -> String;
    async fn browser_tabs_list_for_agent(
        &self,
        workspace_id: &WorkspaceId,
        scope: Option<&str>,
        agent_id: Option<&AgentId>,
    ) -> Result<Value>;
    async fn browser_tab_claim_migrate(
        &self,
        tab_id: &str,
        executed_on: &ClientId,
        agent_id: Option<&AgentId>,
    ) -> Result<()>;
    async fn claim_commit_host(
        &self,
        workspace_id: &WorkspaceId,
        executed_on: &ClientId,
    ) -> Result<ClientId>;
    async fn browser_tabs_migrate_claimed(
        &self,
        workspace_id: &WorkspaceId,
        new_host: &ClientId,
    ) -> Result<()>;
    async fn browser_tab_force_close(&self, tab_id: &str) -> Result<Option<BrowserTab>>;
    async fn browser_tabs_rehome_superseded(
        &self,
        client_id: &ClientId,
        superseded: Vec<ClientId>,
    ) -> Result<usize>;
}

impl BrowserTabRoutingExt for Services {
    /// Where a request about one registered tab goes (REV-2 Model 3 & 5): a
    /// claimed tab lives on its workspace's driving client, an unclaimed one
    /// on its physical host.
    async fn browser_tab_route_target(&self, tab: &BrowserTab) -> Result<ReverseTarget> {
        if tab.owner_agent_id.is_some() {
            self.driving_client_target(&tab.workspace_id).await
        } else {
            Ok(ReverseTarget::Client(tab.host_client_id.clone()))
        }
    }

    /// The open registry row for `tab_id`, or the `-32602` the tab-addressed
    /// `browser.*` methods answer for an unknown / tombstoned id.
    async fn browser_tab_required(&self, method: &str, tab_id: &str) -> Result<BrowserTab> {
        let tab =
            self.store().get_browser_tab(tab_id).await?.ok_or_else(|| {
                Error::InvalidParams(format!("{method}: tab not found: {tab_id}"))
            })?;
        self.require_browser_workspace(&tab.workspace_id).await?;
        Ok(tab)
    }

    /// Browser transport used to be administrator-only. Retain internal callers
    /// while checking the actual resource for every non-administrator wire call.
    async fn require_browser_workspace(&self, workspace: &WorkspaceId) -> Result<()> {
        if intent_services::feature_api::event_ops::is_collaborator_caller() {
            self.require_workspace_manager(workspace, "browser").await?;
        }
        Ok(())
    }

    /// Dispatch one `browser.exec` action about `tab` to its routing target
    /// ([`Self::browser_tab_route_target`]) and return the FE's raw envelope.
    /// The attribution `workspaceId` / `tabId` are threaded so the host sees
    /// the client-triggered envelope shape; an offline target is
    /// `Error::Internal` worded by [`Self::browser_dispatch_error_message`].
    async fn browser_tab_dispatch(
        &self,
        method: &str,
        tab: &BrowserTab,
        action: Value,
    ) -> Result<Value> {
        let Some(dispatch) = self.feature_reverse_dispatch().cloned() else {
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
    async fn browser_dispatch_error_message(&self, err: ReverseDispatchError) -> String {
        match err {
            ReverseDispatchError::ClientOffline {
                client_id, name, ..
            } => {
                let name = match name {
                    Some(name) => Some(name),
                    None => self
                        .store()
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
    async fn browser_tabs_list_for_agent(
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
        let tabs = self.store().list_browser_tabs(workspace_id).await?;
        let presence: HashMap<ClientId, Option<String>> = self
            .feature_reverse_dispatch()
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

    /// Claim migration (REV-2 Model 5): after `executed_on` — the driving
    /// client when the dispatch started — successfully executed an agent's
    /// `claimTab` on `tab_id`, re-home the row to the workspace's driving
    /// client with `agent_id` as owner and publish `browser:tab-updated {
    /// changes: { hostClientId, ownerAgentId } }`. A tab already hosted
    /// there only records the owner (`changes: { ownerAgentId }`); a row
    /// already in the target state is a no-op with no event.
    ///
    /// The host is validated at **commit** time, under the tab gate: the
    /// driving client is re-resolved and a claim whose dispatch overlapped
    /// a `workspace.setBrowserClient` lands on the *new* driving client
    /// (one host per workspace, Model 3 & 10) rather than on the client
    /// that happened to execute it. Either interleaving with the pin
    /// setter's own claimed-tab migration converges on the new pin.
    async fn browser_tab_claim_migrate(
        &self,
        tab_id: &str,
        executed_on: &ClientId,
        agent_id: Option<&AgentId>,
    ) -> Result<()> {
        let _gate = self.feature_browser_tab_gate().lock().await;
        let Some(current) = self.store().get_browser_tab(tab_id).await? else {
            return Ok(());
        };
        let new_host = self
            .claim_commit_host(&current.workspace_id, executed_on)
            .await?;
        if let Some((tab, changes)) = self
            .store()
            .reassign_browser_tab_host(tab_id, &new_host, agent_id)
            .await?
        {
            self.feature_desktop().assignment_changed(&tab.workspace_id);
            publish_event(
                self.feature_event_bus(),
                tab_event(BROWSER_TAB_UPDATED, &new_host, &tab, Some(&changes)),
            )
            .await;
        }
        self.feature_desktop().changed.notify_waiters();
        Ok(())
    }

    /// The client a confirmed claim commits as host: the workspace's driving
    /// client as of now — a pin or claimed-tab host even while offline (the
    /// pin setter re-homes to an offline pin the same way), the resolved
    /// default otherwise — falling back to the client that executed the
    /// claim only when nothing else is known.
    async fn claim_commit_host(
        &self,
        workspace_id: &WorkspaceId,
        executed_on: &ClientId,
    ) -> Result<ClientId> {
        let target = self.driving_client_target(workspace_id).await?;
        let resolved = self
            .feature_reverse_dispatch()
            .and_then(|d| d.resolve(&target).ok())
            .map(|r| r.client_id);
        Ok(match (target, resolved) {
            (_, Some(client_id)) => client_id,
            (ReverseTarget::Pinned(client_id) | ReverseTarget::Client(client_id), None) => {
                client_id
            }
            (ReverseTarget::Default, None) => executed_on.clone(),
        })
    }

    /// Driving-client switch (REV-2 Model 10): move every claimed tab of
    /// `workspace_id` to `new_host`, publishing one `browser:tab-updated {
    /// changes: { hostClientId } }` per moved tab.
    async fn browser_tabs_migrate_claimed(
        &self,
        workspace_id: &WorkspaceId,
        new_host: &ClientId,
    ) -> Result<()> {
        let _gate = self.feature_browser_tab_gate().lock().await;
        let moved = self
            .store()
            .reassign_claimed_browser_tabs(workspace_id, new_host)
            .await?;
        for (tab, changes) in &moved {
            self.feature_desktop().assignment_changed(&tab.workspace_id);
            publish_event(
                self.feature_event_bus(),
                tab_event(BROWSER_TAB_UPDATED, new_host, tab, Some(changes)),
            )
            .await;
        }
        self.feature_desktop().changed.notify_waiters();
        Ok(())
    }

    /// Daemon-side close (REV-2 Model 6, `browser.closeTab` with `force` or
    /// an offline host): tombstone the row and publish `browser:tab-closed`;
    /// the host is told to drop the id on its next `browser.syncTabs`.
    /// Returns the closed row, `None` when there was no open row.
    async fn browser_tab_force_close(&self, tab_id: &str) -> Result<Option<BrowserTab>> {
        let _gate = self.feature_browser_tab_gate().lock().await;
        let closed = self.store().close_browser_tab(tab_id).await?;
        if let Some(tab) = &closed {
            if tab.owner_agent_id.is_some() {
                self.feature_desktop().assignment_changed(&tab.workspace_id);
            }
            publish_event(
                self.feature_event_bus(),
                tab_event(BROWSER_TAB_CLOSED, &tab.host_client_id, tab, None),
            )
            .await;
        }
        Ok(closed)
    }

    /// `client.hello` follow-up (intent-hq/intent#8045): move the open tabs
    /// still hosted by `superseded` — earlier ids of the hello'd device, which
    /// the transport has already limited to ids the caller may act as — onto
    /// `client_id`, publishing one `browser:tab-updated { changes: {
    /// hostClientId, displayed?: null } }` per moved tab. Returns how many
    /// tabs moved.
    ///
    /// Guards, each failing closed:
    /// - **Liveness.** A superseded id with a live hello'd connection keeps
    ///   its tabs — it is still an active host. Liveness is read while holding
    ///   the tab gate, both before the move and again after it commits (rows
    ///   of an old host that came online in between go back before anything
    ///   is published); without a reverse registry it is unknown, so nothing
    ///   moves.
    /// - **Workspace access.** A collaborator caller moves a host only when it
    ///   may manage the browser in every workspace that host has open tabs in
    ///   (the transport has already required current host execution
    ///   authority).
    /// - **No change of who may act as the host.** Besides the
    ///   administrator, only principal `P` may hello as `P:…`. An id's
    ///   *namespace owner* is its head segment when that names a principal of
    ///   this daemon, else none (prefixes left by other backends' principals
    ///   add no one). A superseded id moves only when its namespace owner
    ///   equals `client_id`'s, so a move neither hands an administrator-only
    ///   host to a collaborator nor takes a collaborator's host away.
    async fn browser_tabs_rehome_superseded(
        &self,
        client_id: &ClientId,
        superseded: Vec<ClientId>,
    ) -> Result<usize> {
        if superseded.is_empty() {
            return Ok(0);
        }
        let Some(dispatch) = self.feature_reverse_dispatch() else {
            return Ok(0);
        };
        let mut candidates = superseded;
        let head = |id: &ClientId| id.as_str().split_once(':').map(|(h, _)| h.to_string());
        let heads: Vec<PrincipalId> = std::iter::once(client_id)
            .chain(candidates.iter())
            .filter_map(head)
            .map(PrincipalId::from_string)
            .collect();
        let principals: HashSet<String> = self
            .store()
            .get_principals(&heads)
            .await?
            .into_iter()
            .map(|p| p.id.0)
            .collect();
        let owner = |id: &ClientId| head(id).filter(|h| principals.contains(h));
        let target_owner = owner(client_id);
        candidates.retain(|id| owner(id) == target_owner);
        if candidates.is_empty() {
            return Ok(0);
        }
        let _gate = self.feature_browser_tab_gate().lock().await;
        if intent_services::feature_api::event_ops::is_collaborator_caller() {
            let mut allowed = Vec::with_capacity(candidates.len());
            'host: for host in candidates {
                for tab in self.store().list_browser_tabs_by_host(&host).await? {
                    if self
                        .require_browser_workspace(&tab.workspace_id)
                        .await
                        .is_err()
                    {
                        continue 'host;
                    }
                }
                allowed.push(host);
            }
            candidates = allowed;
        }
        // Liveness is read under the gate every host report takes, so an old
        // host that reconnected while this request waited keeps its tabs.
        let live_now = || -> HashSet<ClientId> {
            dispatch
                .live_clients()
                .into_iter()
                .map(|c| c.client_id)
                .collect()
        };
        let live = live_now();
        candidates.retain(|id| !live.contains(id));
        if candidates.is_empty() {
            return Ok(0);
        }
        let mut origin: HashMap<String, ClientId> = HashMap::new();
        for host in &candidates {
            for tab in self.store().list_browser_tabs_by_host(host).await? {
                origin.insert(tab.tab_id, host.clone());
            }
        }
        let mut moved = self
            .store()
            .reassign_browser_tabs_from_hosts(client_id, &candidates)
            .await?;
        // Binding a connection does not take the gate, so an old host can come
        // online between the snapshot above and the commit. Re-read liveness
        // before anything is published and hand such a host its rows back;
        // its own reports are still queued behind the gate, so it never sees
        // the moved state. A bind after this point is ordered after the rehome.
        let live = live_now();
        if candidates.iter().any(|id| live.contains(id)) {
            let mut kept = Vec::with_capacity(moved.len());
            for (tab, changes) in moved {
                match origin.get(&tab.tab_id).filter(|old| live.contains(*old)) {
                    Some(old) => {
                        self.store()
                            .reassign_browser_tab_host(&tab.tab_id, old, None)
                            .await?;
                    }
                    None => kept.push((tab, changes)),
                }
            }
            moved = kept;
        }
        let mut claimed_workspaces: Vec<&WorkspaceId> = moved
            .iter()
            .filter(|(tab, _)| tab.owner_agent_id.is_some())
            .map(|(tab, _)| &tab.workspace_id)
            .collect();
        claimed_workspaces.sort();
        claimed_workspaces.dedup();
        for workspace in &claimed_workspaces {
            self.feature_desktop().assignment_changed(workspace);
        }
        for (tab, changes) in &moved {
            publish_event(
                self.feature_event_bus(),
                tab_event(BROWSER_TAB_UPDATED, client_id, tab, Some(changes)),
            )
            .await;
        }
        if !claimed_workspaces.is_empty() {
            self.feature_desktop().changed.notify_waiters();
        }
        Ok(moved.len())
    }
}

/// One `listTabs` entry from a registry row: the FE's field names
/// (`tabId` / `workspaceId` / `url` / `requestedUrl?` / `title?` /
/// `ownerAgentId` (`null` when unowned) / `ownerAgentName?` / `mode` +
/// `width` / `height` when emulated / `visibility` / `displayed` — omitted
/// only while the host has never reported it) plus the registry's
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
    if let Some(displayed) = tab.displayed {
        entry.insert("displayed".into(), Value::Bool(displayed));
    }
    entry.insert("hostClientId".into(), tab.host_client_id.0.clone().into());
    let host = presence.get(&tab.host_client_id);
    entry.insert("hostConnected".into(), Value::Bool(host.is_some()));
    if let Some(name) = host.and_then(Option::as_deref) {
        entry.insert("hostName".into(), name.into());
    }
    Value::Object(entry)
}

/// The client-facing browser-tab registry operations, on [`Services`].
pub(crate) trait BrowserTabOpsExt {
    async fn browser_tabs_list(&self, workspace_id: WorkspaceId) -> Result<Vec<BrowserTab>>;
    async fn browser_tab_upsert(&self, host: ClientId, tab: BrowserTabInput) -> Result<BrowserTab>;
    async fn browser_tab_remove(&self, host: ClientId, tab_id: String) -> Result<()>;
    async fn browser_tabs_sync(
        &self,
        host: ClientId,
        tabs: Vec<BrowserTabInput>,
    ) -> Result<Vec<String>>;
}

impl BrowserTabOpsExt for Services {
    /// `browser.listTabs`: open tabs of the workspace, oldest first.
    async fn browser_tabs_list(&self, workspace_id: WorkspaceId) -> Result<Vec<BrowserTab>> {
        self.store().list_browser_tabs(&workspace_id).await
    }

    /// `browser.upsertTab`: host-reported open / navigation / state change.
    /// Emits `browser:tab-opened` for a new row, `browser:tab-updated` with
    /// the field-wise `changes` when something differed, nothing otherwise.
    /// A report naming another `workspaceId` for a known tab is rejected by
    /// the store (`-32602`): tabs do not move between workspaces.
    async fn browser_tab_upsert(&self, host: ClientId, tab: BrowserTabInput) -> Result<BrowserTab> {
        self.require_browser_workspace(&tab.workspace_id).await?;
        let _mutation = self
            .feature_workspace_mutations()
            .enter(&tab.workspace_id)?;
        let _gate = self.feature_browser_tab_gate().lock().await;
        if let Some(existing) = self.store().get_browser_tab(&tab.tab_id).await? {
            self.require_browser_workspace(&existing.workspace_id)
                .await?;
        }
        let outcome = self.store().upsert_browser_tab(&host, tab).await?;
        match &outcome {
            BrowserTabUpsertOutcome::Opened(tab) => {
                if tab.owner_agent_id.is_some() {
                    self.feature_desktop().assignment_changed(&tab.workspace_id);
                }
                publish_event(
                    self.feature_event_bus(),
                    tab_event(BROWSER_TAB_OPENED, &host, tab, None),
                )
                .await;
            }
            BrowserTabUpsertOutcome::Updated { tab, changes } => {
                if changes.get("ownerAgentId").is_some() || changes.get("hostClientId").is_some() {
                    self.feature_desktop().assignment_changed(&tab.workspace_id);
                }
                publish_event(
                    self.feature_event_bus(),
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
    async fn browser_tab_remove(&self, host: ClientId, tab_id: String) -> Result<()> {
        if let Some(tab) = self.store().get_browser_tab(&tab_id).await? {
            self.require_browser_workspace(&tab.workspace_id).await?;
        }
        let _gate = self.feature_browser_tab_gate().lock().await;
        if let Some(tab) = self.store().remove_browser_tab(&host, &tab_id).await? {
            if tab.owner_agent_id.is_some() {
                self.feature_desktop().assignment_changed(&tab.workspace_id);
            }
            publish_event(
                self.feature_event_bus(),
                tab_event(BROWSER_TAB_CLOSED, &host, &tab, None),
            )
            .await;
        }
        self.feature_desktop().changed.notify_waiters();
        Ok(())
    }

    /// `browser.syncTabs`: reconcile the host's full snapshot (REV-2 Model 6),
    /// emitting one event per created / changed / closed row, and return the
    /// ids the host must drop. A snapshot row naming another `workspaceId`
    /// for a known tab rejects the whole snapshot (`-32602`, nothing written).
    async fn browser_tabs_sync(
        &self,
        host: ClientId,
        tabs: Vec<BrowserTabInput>,
    ) -> Result<Vec<String>> {
        for tab in &tabs {
            self.require_browser_workspace(&tab.workspace_id).await?;
        }
        let _mutations = tabs
            .iter()
            .map(|tab| self.feature_workspace_mutations().enter(&tab.workspace_id))
            .collect::<Result<Vec<_>>>()?;
        let _gate = self.feature_browser_tab_gate().lock().await;
        for tab in &tabs {
            if let Some(existing) = self.store().get_browser_tab(&tab.tab_id).await? {
                self.require_browser_workspace(&existing.workspace_id)
                    .await?;
            }
        }
        let result = self.store().sync_browser_tabs(&host, tabs).await?;
        let bus = self.feature_event_bus();
        for tab in &result.opened {
            if tab.owner_agent_id.is_some() {
                self.feature_desktop().assignment_changed(&tab.workspace_id);
            }
            publish_event(bus, tab_event(BROWSER_TAB_OPENED, &host, tab, None)).await;
        }
        for (tab, changes) in &result.updated {
            if changes.get("ownerAgentId").is_some() || changes.get("hostClientId").is_some() {
                self.feature_desktop().assignment_changed(&tab.workspace_id);
            }
            publish_event(
                bus,
                tab_event(BROWSER_TAB_UPDATED, &host, tab, Some(changes)),
            )
            .await;
        }
        for tab in &result.closed {
            if tab.owner_agent_id.is_some() {
                self.feature_desktop().assignment_changed(&tab.workspace_id);
            }
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

#[cfg(test)]
mod browser_exec_reverse_tests;

#[cfg(test)]
mod browser_client_pin_tests;

#[cfg(test)]
mod browser_routing_tests;

#[cfg(test)]
mod browser_routing_fixture_tests;

#[cfg(test)]
mod browser_clients_tests;
