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
use intent_core::{now_iso, Result, WorkspaceId};
use serde_json::Value;

use crate::Services;

/// The secret-store account of an exposed machine's executor token.
#[must_use]
pub fn remote_machine_token_account(machine_id: &str) -> String {
    format!("remoteMachines.{machine_id}.token")
}

/// The secret-store account of cloud machine provider `provider`'s API key
/// (`cloudMachines.e2b.apiKey`).
#[must_use]
pub fn cloud_machine_key_account(provider: &str) -> String {
    format!("cloudMachines.{provider}.apiKey")
}

impl Services {
    /// `remoteMachines.<method>`: see
    /// [`intent_core::WorkspaceApi::remote_machines`].
    pub(crate) async fn remote_machines_op(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<Value> {
        self.require_machine_owner(&format!("remoteMachines.{method}"))
            .await?;
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
        let script = match view.setup_script.as_deref().and_then(nonblank) {
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
}

#[cfg(test)]
mod tests;
