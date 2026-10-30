//! Exposed-machine vocabulary for daemon-to-daemon remote agents: the
//! `model-unavailable` and `machine-update-required` errors (monorepo
//! `docs/protocol/methods/nodes.md`). Types only; the backend raises them
//! when it places an agent or connects to a machine.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::node_link::{ProtocolRange, ProviderStatus};
use crate::placement::AgentPlatform;

/// `error.data.code` of [`ModelUnavailable`].
pub const MODEL_UNAVAILABLE_CODE: &str = "model-unavailable";

/// Most models listed per machine in `model-unavailable` data.
pub const MODEL_UNAVAILABLE_MAX_MODELS: usize = 50;

/// Most models per machine named in the `model-unavailable` message; the
/// data carries up to [`MODEL_UNAVAILABLE_MAX_MODELS`].
const MESSAGE_MAX_MODELS: usize = 10;

/// What one machine offers for the requested provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineModels {
    pub node_id: String,
    pub name: String,
    pub platform: AgentPlatform,
    /// The provider's status there; absent when the machine does not have
    /// the provider at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ProviderStatus>,
    /// The provider's models there, at most
    /// [`MODEL_UNAVAILABLE_MAX_MODELS`].
    pub models: Vec<String>,
    pub observed_at: String,
}

/// No exposed machine offers the requested model. Raised before any launch;
/// the message is written for the delegating model. Surfaces as JSON-RPC
/// `-32602` with [`ModelUnavailable::message`] and [`ModelUnavailable::data`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelUnavailable {
    pub provider: String,
    pub model: String,
    pub machines: Vec<MachineModels>,
    pub retryable: bool,
}

impl ModelUnavailable {
    /// Build the error, capping each machine's model list to
    /// [`MODEL_UNAVAILABLE_MAX_MODELS`].
    #[must_use]
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        machines: impl IntoIterator<Item = MachineModels>,
        retryable: bool,
    ) -> Self {
        let machines = machines
            .into_iter()
            .map(|mut machine| {
                machine.models.truncate(MODEL_UNAVAILABLE_MAX_MODELS);
                machine
            })
            .collect();
        Self {
            provider: provider.into(),
            model: model.into(),
            machines,
            retryable,
        }
    }

    /// The message, for example `Model "opus" (claude) isn't available on
    /// mac-mini (macos/aarch64); it has: sonnet, haiku.`
    #[must_use]
    pub fn message(&self) -> String {
        let lead = format!("Model \"{}\" ({})", self.model, self.provider);
        if self.machines.is_empty() {
            return format!("{lead} isn't available: no machine is exposed to this backend.");
        }
        let names = self
            .machines
            .iter()
            .map(|machine| machine.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let offers = self
            .machines
            .iter()
            .map(|machine| self.offer(machine))
            .collect::<Vec<_>>()
            .join("; ");
        format!("{lead} isn't available on {names}. {offers}.")
    }

    fn offer(&self, machine: &MachineModels) -> String {
        let at = format!("{} ({})", machine.name, machine.platform);
        let Some(status) = machine.status else {
            return format!("{at} doesn't have {}", self.provider);
        };
        let status = match status {
            ProviderStatus::SignedIn | ProviderStatus::Unknown => String::new(),
            other => format!(" [{}: {}]", self.provider, status_name(other)),
        };
        if machine.models.is_empty() {
            return format!("{at}{status} has no models listed");
        }
        let shown = machine.models.len().min(MESSAGE_MAX_MODELS);
        let mut list = machine.models[..shown].join(", ");
        if machine.models.len() > shown {
            list.push_str(&format!(" (+{} more)", machine.models.len() - shown));
        }
        format!("{at}{status} has: {list}")
    }

    /// The `error.data` object.
    #[must_use]
    pub fn data(&self) -> serde_json::Value {
        serde_json::json!({
            "code": MODEL_UNAVAILABLE_CODE,
            "provider": self.provider,
            "model": self.model,
            "machines": self.machines,
            "retryable": self.retryable,
        })
    }
}

fn status_name(status: ProviderStatus) -> &'static str {
    match status {
        ProviderStatus::SignedIn => "signed-in",
        ProviderStatus::SignedOut => "signed-out",
        ProviderStatus::AuthFailed => "auth-failed",
        ProviderStatus::CliMissing => "cli-missing",
        ProviderStatus::PrerequisiteMissing => "prerequisite-missing",
        ProviderStatus::Unknown => "unknown",
    }
}

impl fmt::Display for ModelUnavailable {
    /// MCP renders errors as text, so the text keeps the typed data too.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.message(), self.data())
    }
}

/// `error.data.code` of [`MachineUpdateRequired`].
pub const MACHINE_UPDATE_REQUIRED_CODE: &str = "machine-update-required";

/// Which side must update for a backend and a machine to share a protocol
/// version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UpdateSide {
    Machine,
    Backend,
}

impl UpdateSide {
    /// The side to update when `backend` and `machine` do not overlap: the
    /// machine when its newest version is older than the backend's oldest,
    /// the backend otherwise.
    #[must_use]
    pub fn for_ranges(backend: ProtocolRange, machine: ProtocolRange) -> Self {
        if machine.max < backend.min {
            Self::Machine
        } else {
            Self::Backend
        }
    }
}

/// A backend and a machine share no node-link protocol version. Versions
/// are intentd versions; `machine_name` names the machine in the message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineUpdateRequired {
    pub machine_name: String,
    pub machine_version: String,
    pub backend_version: String,
    pub update: UpdateSide,
}

impl MachineUpdateRequired {
    /// The message, for example `Update Intent on mac-mini (it runs 0.11.0;
    /// this backend needs 0.12.0 or newer).`
    #[must_use]
    pub fn message(&self) -> String {
        match self.update {
            UpdateSide::Machine => format!(
                "Update Intent on {} (it runs {}; this backend needs {} or newer).",
                self.machine_name, self.machine_version, self.backend_version
            ),
            UpdateSide::Backend => format!(
                "Update Intent on this backend (it runs {}; {} needs {} or newer).",
                self.backend_version, self.machine_name, self.machine_version
            ),
        }
    }

    /// The `error.data` object.
    #[must_use]
    pub fn data(&self) -> serde_json::Value {
        serde_json::json!({
            "code": MACHINE_UPDATE_REQUIRED_CODE,
            "machineVersion": self.machine_version,
            "backendVersion": self.backend_version,
            "update": self.update,
        })
    }
}

impl fmt::Display for MachineUpdateRequired {
    /// MCP renders errors as text, so the text keeps the typed data too.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.message(), self.data())
    }
}

#[cfg(test)]
mod tests;
