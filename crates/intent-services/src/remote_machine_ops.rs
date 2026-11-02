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
}

#[cfg(test)]
mod tests;
