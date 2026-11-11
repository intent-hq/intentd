//! Exposed-machine vocabulary for daemon-to-daemon remote agents: the
//! `model-unavailable` and `machine-update-required` errors (monorepo
//! `docs/protocol/methods/nodes.md`). Types only; the backend raises them
//! when it places an agent or connects to a machine.

use std::fmt::{self, Write as _};
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::ids::ExecutorCredentialId;
use crate::node_link::{MachineCapabilities, ProtocolRange, ProviderStatus};
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
    /// The provider's status there (`cli-missing` when it isn't installed,
    /// `disabled` when it is switched off in the machine's own settings).
    pub status: ProviderStatus,
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
    /// A sentence appended to the message (never to the data).
    pub hint: Option<String>,
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
            hint: None,
        }
    }

    /// The message, written for the delegating model, for example
    /// `Model "opus" isn't available on mac-mini (macos/aarch64, claude:
    /// signed-in); it has: sonnet, haiku.` with one sentence per machine.
    #[must_use]
    pub fn message(&self) -> String {
        let message = self.bare_message();
        match &self.hint {
            Some(hint) => format!("{message} {hint}"),
            None => message,
        }
    }

    fn bare_message(&self) -> String {
        if self.model.is_empty() {
            return self.provider_message();
        }
        if self.machines.is_empty() {
            return format!(
                "Model \"{}\" ({}) isn't available: no machine is exposed to this backend.",
                self.model, self.provider
            );
        }
        self.machines
            .iter()
            .map(|machine| self.sentence(machine))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The message when no model was requested: the provider itself is not
    /// usable on any candidate.
    fn provider_message(&self) -> String {
        if self.machines.is_empty() {
            return format!(
                "Provider \"{}\" isn't available: no machine is exposed to this backend.",
                self.provider
            );
        }
        self.machines
            .iter()
            .map(|machine| {
                if machine.status == ProviderStatus::Disabled {
                    return format!(
                        "Provider \"{}\" can't start: it is {}.",
                        self.provider,
                        disabled_on(machine)
                    );
                }
                format!(
                    "Provider \"{}\" can't start on {} ({}, status: {}).",
                    self.provider,
                    machine.name,
                    machine.platform,
                    status_name(machine.status),
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn sentence(&self, machine: &MachineModels) -> String {
        if machine.status == ProviderStatus::Disabled {
            return format!(
                "Model \"{}\" isn't available: {} is {}.",
                self.model,
                self.provider,
                disabled_on(machine)
            );
        }
        let shown = machine.models.len().min(MESSAGE_MAX_MODELS);
        let mut has = if shown == 0 {
            "no models listed".to_string()
        } else {
            machine.models[..shown].join(", ")
        };
        if machine.models.len() > shown {
            let _ = write!(has, " (+{} more)", machine.models.len() - shown);
        }
        format!(
            "Model \"{}\" isn't available on {} ({}, {}: {}); it has: {has}.",
            self.model,
            machine.name,
            machine.platform,
            self.provider,
            status_name(machine.status),
        )
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

/// "disabled on <machine> (<platform>); …": the provider is switched off
/// in that machine's own settings, so it is switched on there, not
/// installed.
fn disabled_on(machine: &MachineModels) -> String {
    format!(
        "disabled on {} ({}); switch it on in that machine's Settings > Providers",
        machine.name, machine.platform
    )
}

/// The wire name of a provider status.
pub(crate) fn status_name(status: ProviderStatus) -> &'static str {
    match status {
        ProviderStatus::SignedIn => "signed-in",
        ProviderStatus::SignedOut => "signed-out",
        ProviderStatus::AuthFailed => "auth-failed",
        ProviderStatus::CliMissing => "cli-missing",
        ProviderStatus::PrerequisiteMissing => "prerequisite-missing",
        ProviderStatus::Disabled => "disabled",
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
    /// A sentence appended to the message (never to the data).
    pub hint: Option<String>,
    /// Why placement needs the update, when the versions do overlap:
    /// [`PREPARED_CHECKOUT_REASON`].
    pub reason: Option<String>,
}

/// `machine-update-required`'s `reason` when the machine links below
/// nodeProtocol 10 and the workspace needs a prepared checkout.
pub const PREPARED_CHECKOUT_REASON: &str = "prepared-checkout";

impl MachineUpdateRequired {
    /// Placement's refusal of a machine that cannot prepare the workspace's
    /// checkout (submodules or a setup script).
    #[must_use]
    pub fn prepared_checkout(machine_name: String, machine_version: String) -> Self {
        Self {
            machine_name,
            machine_version,
            backend_version: env!("CARGO_PKG_VERSION").to_owned(),
            update: UpdateSide::Machine,
            hint: None,
            reason: Some(PREPARED_CHECKOUT_REASON.to_owned()),
        }
    }

    /// The message, for example `Update Intent on mac-mini (it runs 0.11.0;
    /// this backend needs 0.12.0 or newer).`
    #[must_use]
    pub fn message(&self) -> String {
        let message = self.bare_message();
        match &self.hint {
            Some(hint) => format!("{message} {hint}"),
            None => message,
        }
    }

    fn bare_message(&self) -> String {
        if self.reason.as_deref() == Some(PREPARED_CHECKOUT_REASON) {
            return format!(
                "Update Intent on {} to run agents for this workspace: it has submodules or a setup script.",
                self.machine_name
            );
        }
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
        let mut data = serde_json::json!({
            "code": MACHINE_UPDATE_REQUIRED_CODE,
            "machineVersion": self.machine_version,
            "backendVersion": self.backend_version,
            "update": self.update,
        });
        if let Some(reason) = &self.reason {
            data["reason"] = serde_json::json!(reason);
        }
        data
    }
}

impl fmt::Display for MachineUpdateRequired {
    /// MCP renders errors as text, so the text keeps the typed data too.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.message(), self.data())
    }
}

/// A machine-side executor credential: what the machine keeps about a
/// credential it minted for one backend. The token itself is never stored,
/// only its SHA-256 hash, which this record does not carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorCredential {
    pub id: ExecutorCredentialId,
    /// The backend's persisted head id; a bind must name it as `headId`.
    pub backend_id: String,
    pub backend_name: String,
    pub backend_fingerprint: Option<String>,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub revoked_at: Option<String>,
}

impl ExecutorCredential {
    /// Whether the credential still admits its backend.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.revoked_at.is_none()
    }
}

/// `RemoteMachine.state` on the backend. `connected` describes a live link
/// and is never persisted; the backend keeps the other, resting states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteMachineState {
    Connected,
    Disconnected,
    Unreachable,
    Revoked,
    UpdateRequired,
}

impl RemoteMachineState {
    /// The wire name, also the persisted value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
            Self::Unreachable => "unreachable",
            Self::Revoked => "revoked",
            Self::UpdateRequired => "update-required",
        }
    }

    /// Parse a wire name.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "connected" => Self::Connected,
            "disconnected" => Self::Disconnected,
            "unreachable" => Self::Unreachable,
            "revoked" => Self::Revoked,
            "update-required" => Self::UpdateRequired,
            _ => return None,
        })
    }
}

/// A machine exposed to this backend: its routes, certificate pin, the
/// executor credential it minted for this backend (the token is in the
/// secret store) and its last capabilities report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExposedMachine {
    /// The machine's daemon id (`nodeId` in placement and the binding).
    pub id: String,
    pub name: String,
    pub hosts: Vec<String>,
    pub port: u16,
    pub tc_address: Option<String>,
    /// The machine's WSS certificate fingerprint, pinned on every connect.
    pub fingerprint: String,
    pub credential_id: ExecutorCredentialId,
    pub platform: Option<AgentPlatform>,
    /// The last capabilities report, kept while disconnected.
    pub capabilities: Option<MachineCapabilities>,
    /// When `capabilities` was reported.
    pub observed_at: Option<String>,
    /// The persisted resting state, never [`RemoteMachineState::Connected`].
    pub state: RemoteMachineState,
    pub added_at: String,
    pub last_connected_at: Option<String>,
    /// The node-link protocol the last bind negotiated (`RemoteMachine.nodeProtocol`),
    /// kept while disconnected; `None` before the first bind. Saved scripts
    /// need 8.
    pub node_protocol: Option<u32>,
    /// Set on a machine the backend started itself in a cloud sandbox
    /// (`RemoteMachine.cloud`). Such rows are hidden from
    /// `remoteMachines.*` and from placement without a `nodeId`.
    pub cloud: Option<CloudMachine>,
}

impl ExposedMachine {
    /// Whether this row is a cloud machine the backend started itself.
    #[must_use]
    pub fn is_cloud(&self) -> bool {
        self.cloud.is_some()
    }
}

/// The E2B cloud machine provider (`CloudProviderKind`).
pub const CLOUD_PROVIDER_E2B: &str = "e2b";
/// Its display name, also every cloud agent's `nodeName`.
pub const CLOUD_PROVIDER_E2B_NAME: &str = "E2B";
/// The Northflank cloud machine provider (`cloudMachineProviders: 1`).
pub const CLOUD_PROVIDER_NORTHFLANK: &str = "northflank";
pub const CLOUD_PROVIDER_NORTHFLANK_NAME: &str = "Northflank";
/// The providers this backend can configure, in the order clients offer
/// them (`cloudMachines.list` `providerKinds`).
pub const CLOUD_PROVIDER_KINDS: [&str; 2] = [CLOUD_PROVIDER_E2B, CLOUD_PROVIDER_NORTHFLANK];

/// The display name of cloud provider `kind`, for messages.
#[must_use]
pub fn cloud_provider_name(kind: &str) -> &'static str {
    match kind {
        CLOUD_PROVIDER_NORTHFLANK => CLOUD_PROVIDER_NORTHFLANK_NAME,
        _ => CLOUD_PROVIDER_E2B_NAME,
    }
}
/// The id of the E2B entry a single-provider setup migrates to (named
/// [`CLOUD_PROVIDER_E2B_NAME`]); also the entry of a cloud machine row or
/// placement saved before entries existed.
pub const CLOUD_E2B_LEGACY_ENTRY: &str = "e2b";

/// Longest `cloudMachineId`, in bytes.
pub const MAX_CLOUD_MACHINE_ID_BYTES: usize = 64;
/// Most named E2B entries a backend keeps.
pub const MAX_CLOUD_MACHINE_ENTRIES: usize = 16;

fn legacy_entry() -> String {
    CLOUD_E2B_LEGACY_ENTRY.to_owned()
}

/// A cloud sandbox size, `<vCPU>x<GiB>`; each size is its own template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CloudMachineSize {
    #[serde(rename = "2x4")]
    S2x4,
    #[serde(rename = "4x8")]
    S4x8,
    #[serde(rename = "8x16")]
    S8x16,
    #[serde(rename = "16x32")]
    S16x32,
}

impl CloudMachineSize {
    /// Every size, smallest first.
    pub const ALL: [Self; 4] = [Self::S2x4, Self::S4x8, Self::S8x16, Self::S16x32];
    /// The size new sandboxes use unless the owner picks another.
    pub const DEFAULT: Self = Self::S4x8;

    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::S2x4 => "2x4",
            Self::S4x8 => "4x8",
            Self::S8x16 => "8x16",
            Self::S16x32 => "16x32",
        }
    }

    #[must_use]
    pub fn parse(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|size| size.id() == id)
    }

    #[must_use]
    pub fn vcpu(self) -> u32 {
        match self {
            Self::S2x4 => 2,
            Self::S4x8 => 4,
            Self::S8x16 => 8,
            Self::S16x32 => 16,
        }
    }

    #[must_use]
    pub fn memory_mib(self) -> u32 {
        self.vcpu() * 2048
    }

    /// The lowest E2B plan that allows the size.
    #[must_use]
    pub fn plan(self) -> &'static str {
        if self.vcpu() <= 4 {
            "hobby"
        } else {
            "pro-raised-limits"
        }
    }

    /// The sizes a Northflank entry offers, smallest first.
    pub const NORTHFLANK: [Self; 3] = [Self::S2x4, Self::S4x8, Self::S8x16];

    /// The Northflank deployment plan a Northflank entry uses for the size
    /// unless the owner sets another; `None` for a size it does not offer.
    #[must_use]
    pub fn default_deployment_plan(self) -> Option<&'static str> {
        match self {
            Self::S2x4 => Some("nf-compute-200"),
            Self::S4x8 => Some("nf-compute-400"),
            Self::S8x16 => Some("nf-compute-800-16"),
            Self::S16x32 => None,
        }
    }

    /// `CloudMachineSizeInfo` of a Northflank entry: its deployment plan,
    /// no E2B `plan`.
    #[must_use]
    pub fn deployment_info_json(self, deployment_plan: &str) -> serde_json::Value {
        serde_json::json!({
            "id": self.id(),
            "vcpu": self.vcpu(),
            "memoryMiB": self.memory_mib(),
            "deploymentPlan": deployment_plan,
        })
    }

    /// `CloudMachineSizeInfo` on the wire.
    #[must_use]
    pub fn info_json(self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id(),
            "vcpu": self.vcpu(),
            "memoryMiB": self.memory_mib(),
            "plan": self.plan(),
        })
    }
}

/// `CloudMachineState.state`: where the sandbox is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CloudSandboxState {
    Running,
    Pausing,
    Paused,
    Resuming,
    Stopping,
}

/// Who started a cloud machine (`cloud.startedBy`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStartedBy {
    pub principal_id: String,
    pub display_name: Option<String>,
}

/// `RemoteMachine.cloud`: the sandbox behind a cloud machine row. Holds no
/// key or token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudMachine {
    pub provider: String,
    /// The named E2B entry whose key started the sandbox.
    #[serde(default = "legacy_entry")]
    pub cloud_machine_id: String,
    pub sandbox_id: String,
    /// Northflank only: the service's volume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<String>,
    pub state: CloudSandboxState,
    pub template: String,
    pub size: CloudMachineSize,
    pub started_by: CloudStartedBy,
    /// The top-level agent the sandbox was started for.
    pub agent_id: String,
    pub workspace_id: String,
    pub started_at: String,
    /// Seconds not paused since start, for cost estimates, up to the start
    /// of the current running stretch.
    pub running_seconds: u64,
    /// When the current running stretch began (the start or the last
    /// resume); `None` while not running. Kept off the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_since: Option<String>,
    /// When the sandbox was last paused; `None` unless paused. Kept off the
    /// wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<String>,
}

impl CloudMachine {
    /// Seconds not paused since start, the current stretch included.
    #[must_use]
    pub fn running_seconds_at(&self, now: time::OffsetDateTime) -> u64 {
        let stretch = self
            .running_since
            .as_deref()
            .and_then(parse_rfc3339)
            .map_or(0, |since| {
                u64::try_from((now - since).whole_seconds().max(0)).unwrap_or(0)
            });
        self.running_seconds.saturating_add(stretch)
    }

    /// `RemoteMachine.cloud` on the wire: no internal timestamps, and
    /// `runningSeconds` counted up to `now`.
    #[must_use]
    pub fn to_wire(&self, now: time::OffsetDateTime) -> serde_json::Value {
        let mut wire = serde_json::to_value(self).unwrap_or_default();
        if let Some(object) = wire.as_object_mut() {
            object.remove("runningSince");
            object.remove("pausedAt");
            object.insert(
                "runningSeconds".into(),
                serde_json::json!(self.running_seconds_at(now)),
            );
        }
        wire
    }
}

/// An RFC 3339 time, as `now_iso` writes it.
#[must_use]
pub fn parse_rfc3339(text: &str) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339).ok()
}

/// `AgentLite.cloudStart.step`: how far a new cloud machine got.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CloudStartStep {
    CreatingSandbox,
    StartingDaemon,
    Connecting,
}

/// `AgentLite.cloudStart`, present while an agent's cloud machine starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudStart {
    pub step: CloudStartStep,
}

/// `error.data.code` of a cloud machine that cannot start or be kept.
pub const CLOUD_MACHINE_UNAVAILABLE_CODE: &str = "cloud-machine-unavailable";
/// `error.data.code` when a cloud launch would send no profile environment.
pub const CLOUD_PROFILE_REQUIRED_CODE: &str = "cloud-profile-required";
/// `error.data.code` when E2B refused for a plan limit.
pub const CLOUD_QUOTA_CODE: &str = "cloud-quota";

/// `cloud-machine-unavailable`: the provider cannot start or keep a
/// sandbox. `not-configured`, `key-rejected`, `entry-required`,
/// `project-missing` and `permission-denied` are `-32602`; every other
/// reason is `-32603`. `provider` is E2B until [`with_cloud_provider`]
/// names another. Never carries the key.
#[must_use]
pub fn cloud_machine_unavailable(
    reason: &str,
    retryable: bool,
    status: Option<u16>,
    message: impl Into<String>,
) -> crate::Error {
    let internal = !matches!(
        reason,
        "not-configured"
            | "key-rejected"
            | "entry-required"
            | "project-missing"
            | "permission-denied"
    );
    let mut data = serde_json::json!({
        "code": CLOUD_MACHINE_UNAVAILABLE_CODE,
        "provider": CLOUD_PROVIDER_E2B,
        "reason": reason,
        "retryable": retryable,
    });
    if let Some(status) = status {
        data["status"] = serde_json::json!(status);
    }
    crate::Error::remote_machine(internal, message, data)
}

/// `error` with `data.cloudMachineId` set to the E2B entry `id`, when it is
/// one of the three cloud errors and does not name an entry yet.
#[must_use]
pub fn with_cloud_machine_id(error: crate::Error, id: &str) -> crate::Error {
    match error {
        crate::Error::RemoteMachine(mut failure)
            if !id.is_empty()
                && failure.data.get("cloudMachineId").is_none()
                && matches!(
                    failure.data["code"].as_str(),
                    Some(
                        CLOUD_MACHINE_UNAVAILABLE_CODE
                            | CLOUD_PROFILE_REQUIRED_CODE
                            | CLOUD_QUOTA_CODE
                    )
                ) =>
        {
            failure.data["cloudMachineId"] = serde_json::json!(id);
            crate::Error::RemoteMachine(failure)
        }
        other => other,
    }
}

/// `error` with `data.provider` set to cloud provider `kind`, when it is
/// one of the three cloud errors.
#[must_use]
pub fn with_cloud_provider(error: crate::Error, kind: &str) -> crate::Error {
    match error {
        crate::Error::RemoteMachine(mut failure)
            if matches!(
                failure.data["code"].as_str(),
                Some(
                    CLOUD_MACHINE_UNAVAILABLE_CODE | CLOUD_PROFILE_REQUIRED_CODE | CLOUD_QUOTA_CODE
                )
            ) =>
        {
            failure.data["provider"] = serde_json::json!(kind);
            crate::Error::RemoteMachine(failure)
        }
        other => other,
    }
}

/// `cloud-profile-required` (`-32602`): the launch would send no profile
/// environment, and a sandbox has no sign-in of its own.
#[must_use]
pub fn cloud_profile_required(provider_ref: &str) -> crate::Error {
    crate::Error::remote_machine(
        false,
        format!(
            "\"{provider_ref}\" uses this backend's own sign-in, which a cloud machine can't use; pick a profile with an API key or token."
        ),
        serde_json::json!({
            "code": CLOUD_PROFILE_REQUIRED_CODE,
            "provider": CLOUD_PROVIDER_E2B,
            "providerRef": provider_ref,
        }),
    )
}

/// `cloud-quota` (`-32602`): E2B refused for a plan limit (`concurrency`,
/// `rate` or `plan`). Only `plan` is not retryable.
#[must_use]
pub fn cloud_quota(
    limit: &str,
    retry_after_seconds: Option<u64>,
    message: impl Into<String>,
) -> crate::Error {
    let mut data = serde_json::json!({
        "code": CLOUD_QUOTA_CODE,
        "provider": CLOUD_PROVIDER_E2B,
        "limit": limit,
        "retryable": limit != "plan",
    });
    if let Some(seconds) = retry_after_seconds {
        data["retryAfterSeconds"] = serde_json::json!(seconds);
    }
    crate::Error::remote_machine(false, message, data)
}

/// Where this machine's executor link can be dialed, as `executor.mint`
/// reports it: its display name and the routes and certificate fingerprint of
/// its WSS listener (the same ones the pairing payload advertises).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MachineRoutes {
    pub name: String,
    pub port: u16,
    pub hosts: Vec<String>,
    pub tc_address: Option<String>,
    pub fingerprint: String,
}

/// Resolves [`MachineRoutes`]. The transport owns the listener, so it
/// implements this and the composition root attaches it to the services.
pub trait MachineRoutesSource: Send + Sync {
    /// The current routes; [`crate::Error::ListenerDown`] while the WSS
    /// listener is stopped.
    fn machine_routes(
        &self,
    ) -> Pin<Box<dyn Future<Output = crate::Result<MachineRoutes>> + Send + '_>>;
}

/// The live state of one executor credential on this machine, for
/// `executor.list`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutorLiveStatus {
    /// Whether the credential's backend has an executor link open.
    pub connected: bool,
    /// Remote agents the credential's backend runs here.
    pub running_agents: u32,
}

/// Reports [`ExecutorLiveStatus`]. The executor link implements it; without
/// one every credential reads as disconnected with no agents.
pub trait ExecutorLiveness: Send + Sync {
    fn status(&self, credential_id: &ExecutorCredentialId) -> ExecutorLiveStatus;
}

#[cfg(test)]
mod tests;

/// `data.code` of [`machine_not_signed_in`].
pub const MACHINE_NOT_SIGNED_IN_CODE: &str = "machine-not-signed-in";

/// One machine whose report says the provider is not signed in.
#[derive(Debug, Clone)]
pub struct SignedOutMachine {
    pub node_id: String,
    pub name: String,
    pub status: ProviderStatus,
    pub observed_at: String,
}

/// `machine-not-signed-in` (`-32602`): a machine sign-in launch (the
/// Default configuration sending nothing) would run on machines whose
/// report says `provider` is signed out there.
#[must_use]
pub fn machine_not_signed_in(
    provider: &str,
    machines: &[SignedOutMachine],
    retryable: bool,
) -> crate::Error {
    let display = match provider {
        "claude-code" => "Claude Code",
        "codex" => "Codex",
        other => other,
    };
    let names: Vec<&str> = machines.iter().map(|m| m.name.as_str()).collect();
    let on = names.join(", ");
    crate::Error::remote_machine(
        retryable,
        format!(
            "{display} isn't signed in on {on}, and Default sends no sign-in of its own. Sign in to {display} on {on}, or pick a profile with an API key or token that is marked for remote machines."
        ),
        serde_json::json!({
            "code": MACHINE_NOT_SIGNED_IN_CODE,
            "providerRef": provider,
            "machines": machines.iter().map(|m| serde_json::json!({
                "nodeId": m.node_id,
                "name": m.name,
                "status": status_name(m.status),
                "observedAt": m.observed_at,
            })).collect::<Vec<_>>(),
            "retryable": retryable,
        }),
    )
}
