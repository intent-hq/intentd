//! Backend side of daemon-to-daemon remote agents (monorepo
//! `docs/protocol/methods/nodes.md`, "Backend methods: exposed machines"):
//! the owner gate in front of `remoteMachines.*`, and the seams the remote
//! feature uses for them (the executor token in the secret store, the
//! experimental switch, this backend's routes and the
//! `remoteMachine:changed` event).
//!
//! The methods are on neither the collaborator nor the member allowlist, so
//! the transport refuses members and guests first; the owner is rechecked
//! here before the remote feature's port runs the method.

use intent_core::machines::MachineRoutes;
use intent_core::remote_launch::{setup_retry, RemotePlacementView, RemoteSetupState};
use intent_core::{now_iso, AgentId, Caller, Result, WorkspaceId};
use serde_json::Value;

use crate::Services;

/// The secret-store account of an exposed machine's executor token.
#[must_use]
/// `-32602` `homes-unsupported`: a backend without `workspaceHomes`
/// (nodes.md "Moving the home").
pub fn homes_unsupported() -> intent_core::Error {
    intent_core::Error::remote_machine(
        false,
        "This backend doesn't keep workspace homes.",
        serde_json::json!({ "code": "homes-unsupported" }),
    )
}

/// `-32602` `setup-retry-not-remote`: `agent.retrySetup` for an agent that
/// is not a remote agent with a prepared checkout.
#[must_use]
pub fn setup_retry_not_remote() -> intent_core::Error {
    setup_retry_refusal(
        setup_retry::NOT_REMOTE,
        "Retry setup is for a remote agent's checkout on its machine; this agent has no setup there.",
    )
}

/// A `-32602` refusal of `agent.retrySetup` with `data.code` `code`.
#[must_use]
pub fn setup_retry_refusal(code: &str, message: impl Into<String>) -> intent_core::Error {
    intent_core::Error::remote_machine(false, message, serde_json::json!({ "code": code }))
}

pub fn remote_machine_token_account(machine_id: &str) -> String {
    format!("remoteMachines.{machine_id}.token")
}

/// The secret-store account of cloud machine provider `provider`'s API key
/// (`cloudMachines.e2b.apiKey`).
#[must_use]
pub fn cloud_machine_key_account(provider: &str) -> String {
    format!("cloudMachines.{provider}.apiKey")
}

/// A new workspace's initial agent that waits for a new sandbox of its
/// cloud home's entry, placed with no checkout, asks to work in the home
/// checkout (nodes.md "The home checkout" 1): its start keeps that only
/// when its sandbox becomes the home.
pub(crate) fn share_new_cloud_home(
    home: &intent_core::WorkspaceHome,
    plan: &mut crate::agent_ops::AgentCreatePlan,
) {
    let explicit = plan
        .placement
        .as_ref()
        .is_some_and(|placement| placement.checkout.is_some());
    let Some(view) = plan.remote.as_mut() else {
        return;
    };
    if !explicit
        && home.node_id.is_none()
        && view.is_cloud()
        && view.is_unlinked()
        && home.cloud_machine_id.is_some()
        && view.placement.cloud_machine_id == home.cloud_machine_id
    {
        view.home_checkout = Some(home.home_id.clone());
    }
}

impl Services {
    /// `agent.retrySetup` (nodes.md "Retrying a remote agent's setup"): the
    /// agent's own checks, then the machine runs the script again. An agent
    /// caller (its `ws.workspace.retrySetup()`) may retry only its own
    /// setup, during its own turn.
    pub(crate) async fn agent_retry_setup_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<Value> {
        let by_agent = match intent_core::current_caller() {
            Some(Caller::Agent { agent_id: caller }) if caller == *agent_id => true,
            Some(Caller::Agent { .. }) => {
                return Err(intent_core::Error::Forbidden(
                    "an agent can retry only its own setup".into(),
                ))
            }
            _ => false,
        };
        let session = self
            .store
            .get_agent_session_without_messages(agent_id)
            .await?;
        if session.workspace_id != *workspace_id {
            return Err(intent_core::Error::NotFound(format!(
                "agent {agent_id} not found"
            )));
        }
        let view = RemotePlacementView::from_metadata(session.metadata.as_ref())
            .ok_or_else(setup_retry_not_remote)?;
        if view.shared_checkout_of.is_some() {
            return Err(setup_retry_refusal(
                setup_retry::SHARED_CHECKOUT,
                "This agent works in another agent's checkout; retry setup on the agent that owns it.",
            ));
        }
        if view.home_checkout.is_some() {
            return Err(setup_retry_refusal(
                setup_retry::SHARED_CHECKOUT,
                "This agent works in the workspace home, whose setup is the home's; it cannot be retried here.",
            ));
        }
        if view.setup.is_none() {
            return Err(setup_retry_not_remote());
        }
        let state = view.setup.as_ref().map(|setup| setup.state);
        if !matches!(
            state,
            Some(RemoteSetupState::Failed | RemoteSetupState::Running)
        ) {
            return Err(setup_retry_refusal(
                setup_retry::NOT_FAILED,
                "Setup has not failed on this agent's machine, so there is nothing to retry.",
            ));
        }
        if !by_agent && session.status.is_running_turn() {
            return Err(setup_retry_refusal(
                setup_retry::AGENT_BUSY,
                "The agent is working; retry setup when its turn ends.",
            ));
        }
        let status = self
            .feature_remote_launch_port()
            .retry_setup(self, agent_id, &view)
            .await?;
        Ok(serde_json::json!({ "agentId": agent_id, "setup": status }))
    }

    /// `remoteMachines.<method>`: see
    /// [`intent_core::WorkspaceApi::remote_machines`].
    pub(crate) async fn remote_machines_op(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<Value> {
        let what = format!("remoteMachines.{method}");
        match method {
            // The ordered list carries names only: members read it too.
            "order" => self.require_host_execution(&what).await?,
            _ => self.require_machine_owner(&what).await?,
        }
        self.feature_remote_launch_port()
            .remote_machines(self, method, params)
            .await
    }

    /// `cloudMachines.<method>`: `configure` and `remove` are owner-only,
    /// `list` is also open to shared-host members (the port filters the
    /// machines to the workspaces they can manage), and `pause` and `kill`
    /// to the member who started the machine (the port checks that).
    pub(crate) async fn cloud_machines_op(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<Value> {
        let what = format!("cloudMachines.{method}");
        match method {
            "list" | "pause" | "kill" => self.require_host_execution(&what).await?,
            _ => self.require_machine_owner(&what).await?,
        }
        self.feature_remote_launch_port()
            .cloud_machines(self, method, params)
            .await
    }

    /// The saved API key of cloud machine provider `provider`.
    ///
    /// # Errors
    ///
    /// When the secret store cannot be read.
    pub async fn load_cloud_machine_key(&self, provider: &str) -> Result<Option<String>> {
        self.secrets
            .load(&cloud_machine_key_account(provider))
            .await
    }

    /// Save the API key of cloud machine provider `provider`.
    ///
    /// # Errors
    ///
    /// When the secret store cannot be written.
    pub async fn store_cloud_machine_key(&self, provider: &str, key: &str) -> Result<()> {
        self.secrets
            .store(&cloud_machine_key_account(provider), key)
            .await
    }

    /// Forget the API key of cloud machine provider `provider`.
    ///
    /// # Errors
    ///
    /// When the secret store cannot be written.
    pub async fn delete_cloud_machine_key(&self, provider: &str) -> Result<()> {
        self.secrets
            .delete(&cloud_machine_key_account(provider))
            .await
    }

    /// Publish `cloudMachine:changed` with `data`. Provider changes are
    /// global (`workspace` `None`) and reach the owner and every shared-host
    /// member; a machine change carries the workspace it was started for,
    /// so members receive it only where they can manage that workspace.
    /// Never delivered to guests.
    pub async fn publish_cloud_machine_changed(&self, data: Value, workspace: Option<&str>) {
        let event = intent_store::NewEvent {
            workspace_id: WorkspaceId::from_string(workspace.unwrap_or_default().to_owned()),
            timestamp: now_iso(),
            event_type: intent_core::events::CLOUD_MACHINE_CHANGED.to_owned(),
            actor: crate::system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        crate::publish_event(self.event_bus.as_ref(), event).await;
    }

    /// This daemon's own routes (its display name and WSS fingerprint), for
    /// `remoteMachines.backendInfo`; `None` without a listener.
    pub async fn own_machine_routes(&self) -> Option<MachineRoutes> {
        self.machine_routes.get()?.machine_routes().await.ok()
    }

    /// The executor token saved for exposed machine `machine_id`.
    ///
    /// # Errors
    ///
    /// When the secret store cannot be read.
    pub async fn load_remote_machine_token(&self, machine_id: &str) -> Result<Option<String>> {
        self.secrets
            .load(&remote_machine_token_account(machine_id))
            .await
    }

    /// Save the executor token of exposed machine `machine_id`.
    ///
    /// # Errors
    ///
    /// When the secret store cannot be written.
    pub async fn store_remote_machine_token(&self, machine_id: &str, token: &str) -> Result<()> {
        self.secrets
            .store(&remote_machine_token_account(machine_id), token)
            .await
    }

    /// Forget the executor token of exposed machine `machine_id`; absence is
    /// not an error.
    ///
    /// # Errors
    ///
    /// When the secret store cannot be written.
    pub async fn delete_remote_machine_token(&self, machine_id: &str) -> Result<()> {
        self.secrets
            .delete(&remote_machine_token_account(machine_id))
            .await
    }

    /// Publish `remoteMachine:changed` with `data` (`{ machine }` or
    /// `{ machineId, removed: true }`). Global and owner-only: it is not on
    /// the collaborator event list, so members and guests never receive it.
    pub async fn publish_remote_machine_changed(&self, data: Value) {
        let event = intent_store::NewEvent {
            workspace_id: WorkspaceId::from_string(String::new()),
            timestamp: now_iso(),
            event_type: intent_core::events::REMOTE_MACHINE_CHANGED.to_owned(),
            actor: crate::system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        crate::publish_event(self.event_bus.as_ref(), event).await;
    }

    /// The setup script a remote agent of `workspace_id` placed with `view`
    /// runs on its machine, resolved at every launch with the local
    /// `workspace.create` rule: the explicit create parameter kept with the
    /// initial agent's placement, else `setupScript` in the workspace
    /// worktree's `.intent/config.json`, else the workspace's stored script.
    /// A workspace without a worktree (`skipWorktree`, a plain folder) has
    /// none.
    ///
    /// # Errors
    ///
    /// When the workspace cannot be read.
    pub async fn remote_setup_command(
        &self,
        workspace_id: &WorkspaceId,
        view: &intent_core::remote_launch::RemotePlacementView,
    ) -> Result<Option<intent_core::node_link::SetupCommand>> {
        self.setup_command_with(workspace_id, view.setup_script.as_deref())
            .await
    }

    /// The workspace's setup script for its home checkout (nodes.md
    /// "Workspace homes"): the repository's, else the workspace's.
    ///
    /// # Errors
    ///
    /// The workspace read's error.
    pub async fn home_setup_command(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<intent_core::node_link::SetupCommand>> {
        self.setup_command_with(workspace_id, None).await
    }

    async fn setup_command_with(
        &self,
        workspace_id: &WorkspaceId,
        explicit: Option<&str>,
    ) -> Result<Option<intent_core::node_link::SetupCommand>> {
        let ws = self.store.get_workspace(workspace_id).await?;
        if ws.skip_worktree || ws.plain_folder {
            return Ok(None);
        }
        let Some(worktree) = crate::git_ops::worktree_path(&ws) else {
            return Ok(None);
        };
        let nonblank = |script: &str| {
            let script = script.trim();
            (!script.is_empty()).then(|| script.to_owned())
        };
        let script = match explicit.and_then(nonblank) {
            Some(explicit) => Some(explicit),
            None => crate::repo_config::read_repo_config(&worktree)
                .await
                .setup_script
                .as_deref()
                .and_then(nonblank)
                .or_else(|| {
                    ws.setup_script
                        .as_ref()
                        .and_then(|stored| nonblank(&stored.script))
                }),
        };
        Ok(script.map(|script| intent_core::node_link::SetupCommand {
            script,
            branch_name: ws.branch.clone(),
            source_branch: ws.base_ref.clone().unwrap_or_default(),
            timeout_secs: None,
        }))
    }

    /// Publish `agent:updated` for a remote agent whose projected placement
    /// changed on its node (checkout isolated, or a start error), so the
    /// `agent` channel pushes its list row with the new `effectiveIsolation`
    /// / `placementError` instead of leaving the list at `pending` until
    /// the agent is opened.
    pub async fn publish_agent_placement_changed(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &intent_core::AgentId,
    ) {
        self.publish_agent_mutation_event(
            workspace_id,
            agent_id,
            intent_core::events::AGENT_UPDATED,
            serde_json::json!({ "agentId": agent_id.0 }),
        )
        .await;
    }

    /// `workspace.setHome` (nodes.md "Moving the home"): the workspace's
    /// owner and managers only.
    ///
    /// # Errors
    ///
    /// `-32003` for any other caller, `homes-unsupported` on a backend
    /// without workspace homes, and the move's own refusals.
    pub async fn set_workspace_home_op(
        &self,
        workspace_id: WorkspaceId,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.require_workspace_manager(&workspace_id, "workspace.setHome")
            .await?;
        if !self.workspace_homes() {
            return Err(crate::homes_unsupported());
        }
        self.ports
            .remote_launch()
            .set_home(self, &workspace_id, params)
            .await
    }

    /// The home of a workspace being created (nodes.md "Workspace homes" →
    /// "Choosing the home"): only on an orchestrator serving homes, and
    /// never for a plain folder. Nothing is written.
    ///
    /// # Errors
    ///
    /// The remote feature's refusal when no machine can be a home.
    pub(crate) async fn choose_workspace_home(
        &self,
        plain_folder: bool,
        initial: Option<&intent_core::remote_launch::RemotePlacementView>,
    ) -> Result<Option<Box<intent_core::WorkspaceHome>>> {
        if !self.workspace_homes() || plain_folder {
            return Ok(None);
        }
        let home = self
            .ports
            .remote_launch()
            .choose_home(self, initial)
            .await?;
        Ok(Some(Box::new(home)))
    }

    /// Record the new workspace's home (when it has one), announce it and open it on its
    /// machine in the background (it opens again at the machine's next
    /// bind when this fails).
    ///
    /// # Errors
    ///
    /// The store's error.
    pub(crate) async fn record_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
        home: Option<Box<intent_core::WorkspaceHome>>,
    ) -> Result<()> {
        let Some(home) = home else {
            return Ok(());
        };
        self.store.set_workspace_home(workspace_id, &home).await?;
        // A machine's home reports its open as `setup` (nodes.md "Opening
        // the home"); a cloud home's start is its `starting` state.
        if home.node_id.is_some() {
            self.home_setups.opening(workspace_id, &home.home_id);
        }
        self.publish_workspace_home_changed(workspace_id).await;
        let opener = self.clone();
        let workspace_id = workspace_id.clone();
        // caller-binding: allow — opens the new workspace's home on its machine
        tokio::spawn(async move {
            if let Err(error) = opener
                .ports
                .remote_launch()
                .open_home(&opener, &workspace_id)
                .await
            {
                tracing::warn!(workspace = %workspace_id, %error, "could not open the workspace's home; it opens at the machine's next bind");
            }
        });
        Ok(())
    }

    /// How far each workspace home's opening has got (nodes.md "Opening the
    /// home"). A change the caller makes is announced with
    /// [`Self::publish_workspace_home_changed`].
    #[must_use]
    pub fn home_setups(&self) -> &crate::home_setup::HomeSetups {
        &self.home_setups
    }

    /// Show `notice` (nodes.md "Moving the home" step 4: the agent now works
    /// in a checkout of its own) in agent `agent_id`'s transcript as a system
    /// row. Best-effort.
    pub async fn add_home_moved_transcript_row(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &intent_core::AgentId,
        notice: &str,
    ) {
        let content = serde_json::json!([{ "type": "text", "text": notice }]);
        match self
            .store
            .append_agent_message(agent_id, "system", &content, &now_iso())
            .await
        {
            Ok(message) => {
                self.invalidate_agent_list_cache(workspace_id);
                self.publish_agent_message_events(workspace_id, agent_id, &message, None)
                    .await;
            }
            Err(error) => {
                tracing::warn!(agent = %agent_id, %error, "could not add the home-moved notice to the transcript");
            }
        }
    }

    /// The workspace's notice that a moved home's leftover work was pushed
    /// to `refs` (nodes.md "Moving the home" step 5): a note naming them, so
    /// nothing is merged automatically and the user can pick them up.
    ///
    /// # Errors
    ///
    /// The store's error.
    pub async fn post_home_orphaned_notice(
        &self,
        workspace_id: &WorkspaceId,
        home_name: Option<&str>,
        refs: &[String],
    ) -> Result<intent_core::NoteId> {
        let from = home_name.map_or_else(
            || "the old home".to_owned(),
            |name| format!("the old home on {name}"),
        );
        let mut content = format!(
            "The workspace's home moved. Work left on {from} after its last backup was saved to:\n\n"
        );
        for name in refs {
            content.push_str("- `");
            content.push_str(name);
            content.push_str("`\n");
        }
        content.push_str(
            "\nNothing was merged. To bring it back, fetch the ref and merge or cherry-pick it in the home checkout.\n",
        );
        self.post_home_notice(
            workspace_id,
            intent_core::NoteId::new(),
            "Work left on the old home",
            content,
        )
        .await
    }

    /// The workspace's notice that its cloud home was rebuilt (nodes.md
    /// "Cloud homes" 5): the sandbox that held it on `home_name` was
    /// deleted, and a new one holds it from the backup of `last_backup_at`.
    /// The note's id is the rebuilt home's `home_id` (a fresh UUID), so a
    /// retry after a failure or a restart finds it instead of posting again.
    ///
    /// # Errors
    ///
    /// The store's error.
    pub async fn post_home_rebuilt_notice(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &intent_core::HomeId,
        home_name: &str,
        last_backup_at: Option<&str>,
    ) -> Result<intent_core::NoteId> {
        let backup = last_backup_at.map_or_else(
            || "the workspace's last saved work".to_owned(),
            |at| format!("its last backup ({at})"),
        );
        let content = format!(
            "The cloud sandbox that held this workspace's home on {home_name} was deleted. A new sandbox now holds the home, restored from {backup}.\n\nChanges made after that backup are not on the new home.\n"
        );
        let id = intent_core::NoteId::from_string(home_id.as_str());
        if self.store.get_note(workspace_id, &id).await.is_ok() {
            return Ok(id);
        }
        self.post_home_notice(workspace_id, id, "The home was rebuilt", content)
            .await
    }

    /// A workspace note from Intent about its home.
    async fn post_home_notice(
        &self,
        workspace_id: &WorkspaceId,
        id: intent_core::NoteId,
        title: &str,
        content: String,
    ) -> Result<intent_core::NoteId> {
        use intent_core::{ContentType, Note, NoteMetadata, NoteVersionAuthor, NoteVisibility};
        let now = now_iso();
        let note = Note {
            id,
            workspace_id: workspace_id.clone(),
            title: title.to_owned(),
            content,
            content_type: ContentType::Markdown,
            tags: vec!["home".to_owned()],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: now.clone(),
            rev: 0,
            updated_at: now,
        };
        let author = NoteVersionAuthor {
            id: "system".to_owned(),
            name: "Intent".to_owned(),
            author_type: "system".to_owned(),
        };
        crate::note_write::persist_new_note(&self.store, &note, &author).await?;
        crate::publish_event(
            self.event_bus.as_ref(),
            crate::note_change_event(
                workspace_id,
                &note.id,
                &note.title,
                intent_core::events::NOTE_CREATED,
                "create",
            ),
        )
        .await;
        Ok(note.id)
    }

    /// Announce the workspace's home (nodes.md "Workspace homes") as the
    /// existing `workspace:updated` with `changes: { home }`.
    pub async fn publish_workspace_home_changed(&self, workspace_id: &WorkspaceId) {
        let home = match self.store.get_shown_workspace_home(workspace_id).await {
            Ok(mut home) => {
                if let Some(home) = home.as_mut() {
                    self.home_setups.attach(workspace_id, home, false);
                }
                home
            }
            Err(error) => {
                tracing::warn!(workspace = %workspace_id, %error, "could not read the workspace's home to announce it");
                return;
            }
        };
        crate::publish_event(
            self.event_bus.as_ref(),
            crate::workspace_updated_event(workspace_id, &serde_json::json!({ "home": home })),
        )
        .await;
    }
}

#[cfg(test)]
mod tests;
