//! Private head-to-node link vocabulary for launching agents on one
//! configured execution node: the lease binding and its generation, the
//! frames exchanged on the control/journal/RPC connection, and the journal
//! records the node streams back.
//!
//! This is a private protocol between two intentd processes, not the public
//! client protocol. Wire claims grant nothing: head re-checks the binding's
//! generation when it dispatches a node request, and the node accepts a
//! bind only from its owning head with a newer generation.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::nodes::NodeCounter;
use crate::placement::{AgentPlatform, CapacityRefusal};
use crate::{AgentId, Error, Result, WorkspaceId};

/// Identifies one RPC request on the link.
pub type LinkId = uuid::Uuid;

/// Identifies one stop the backend asked for. The backend chooses it and
/// reuses it when it resends the same stop, on the same link or after a
/// reconnect; only a [`LinkFrame::StopAck`] carrying it confirms that stop.
pub type StopId = uuid::Uuid;

/// What a machine reports when it has applied a stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StopOutcome {
    /// The agent was running for this backend; its process tree is gone and
    /// its shared slot is free. Its `exited` journal record follows on the
    /// journal lane, before or after the acknowledgement.
    Stopped,
    /// The machine runs no agent with that id for this backend: it already
    /// exited, was never launched, or is unknown.
    NotRunning,
}

/// The private node-link protocol versions this build speaks. The
/// daemon-to-daemon executor link started at 6; 7 adds the correlated stop
/// acknowledgement (`stopId` on [`LinkFrame::Stop`], [`LinkFrame::StopAck`]).
/// Neither 6 nor the single-node 5 ever shipped, and a protocol-6 peer
/// cannot confirm a stop, so this build speaks 7 only: a backend never
/// takes a stop as confirmed by a machine that cannot acknowledge it. The
/// prepared enrollment-based versions 2-4 in monorepo
/// `docs/protocol/node-link.md` are a different protocol. Within a version,
/// later additions are optional with defaults, so peers on different builds
/// interoperate inside their common range.
pub const NODE_LINK_PROTOCOLS: ProtocolRange = ProtocolRange { min: 7, max: 7 };

/// An inclusive range of node-link protocol versions a peer speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolRange {
    pub min: u32,
    pub max: u32,
}

impl ProtocolRange {
    /// Whether `version` is in the range.
    #[must_use]
    pub fn contains(self, version: u32) -> bool {
        self.min <= version && version <= self.max
    }

    /// The highest version both ranges contain, or `None` when they do not
    /// overlap (or either is empty).
    #[must_use]
    pub fn highest_common(self, peer: Self) -> Option<u32> {
        let version = self.max.min(peer.max);
        (self.contains(version) && peer.contains(version)).then_some(version)
    }
}

impl std::fmt::Display for ProtocolRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.min == self.max {
            write!(f, "{}", self.min)
        } else {
            write!(f, "{}-{}", self.min, self.max)
        }
    }
}

/// The link frame types a backend holding an executor credential may send
/// to the machine on `/node/link`, by their wire `type` tag: binding,
/// liveness, agent control, journal acks, the capabilities request and
/// responses to the machine's forwarded `workspace.call` requests. Nothing
/// else is admitted from an executor credential; the machine-to-backend
/// frames (`welcome`, `journal`, `replayComplete`, `request`,
/// `capabilitiesReply`, `stopAck`) and `cancelRequest` are refused. Git sessions use the
/// separate `/node/git` connection, admitted per binding. Sorted;
/// [`LinkFrame::admitted_from_executor`] is the typed form.
pub const EXECUTOR_LINK_REQUESTS: &[&str] = &[
    "ack",
    "cancel",
    "capabilities",
    "heartbeat",
    "heartbeatReply",
    "hello",
    "launch",
    "prompt",
    "response",
    "stop",
];

/// Largest encoded frame either side sends or accepts.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Who owns a node, and which link generation is current. The node persists
/// its current binding; head increments `generation` for every new link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LeaseBinding {
    pub head_id: String,
    pub node_id: String,
    pub lease_id: String,
    pub generation: NodeCounter,
}

/// The node's answer to a bind offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindDecision {
    /// Accept: persist `offered` and close any older connection.
    Accept,
    /// The offer is addressed to a different node.
    WrongNode,
    /// Another head owns this node. Moving a node to a new head is a manual
    /// reset of the node's binding file.
    OtherHead,
    /// The offer's generation is not newer than the current one.
    Stale { current: NodeCounter },
}

impl LeaseBinding {
    /// Decide whether `offered` replaces `current`, the node's persisted
    /// binding (`None` before its first bind). Only the owning head may bind,
    /// and only with a strictly higher generation, so a replayed or delayed
    /// bind can never take the node back from a newer link.
    #[must_use]
    pub fn decide(node_id: &str, current: Option<&Self>, offered: &Self) -> BindDecision {
        if offered.node_id != node_id {
            return BindDecision::WrongNode;
        }
        match current {
            Some(current) if current.head_id != offered.head_id => BindDecision::OtherHead,
            Some(current) if offered.generation <= current.generation => BindDecision::Stale {
                current: current.generation,
            },
            None | Some(_) => BindDecision::Accept,
        }
    }

    /// Head's dispatch check: a frame from `self` may act only while it is
    /// still the current binding.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParams`] `node-link-stale` when the binding was
    /// replaced (a newer generation or a different lease).
    pub fn ensure_current(&self, current: &Self) -> Result<()> {
        if self == current {
            Ok(())
        } else {
            Err(Error::InvalidParams("node-link-stale".into()))
        }
    }
}

/// First frame, head to node. Within a protocol version unknown members are
/// ignored, so a later optional addition does not break an older peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkHello {
    /// The versions head speaks; the node picks the highest common one.
    pub protocol: ProtocolRange,
    pub intentd_version: String,
    pub binding: LeaseBinding,
    /// Highest journal sequence head has ingested; the node replays
    /// everything after it.
    pub acked_seq: NodeCounter,
}

/// The node's reply to an accepted [`LinkHello`]: the negotiated protocol
/// version, the agents it is running for this binding (so head can stop
/// agents it does not claim), its capabilities report and its current
/// routes, which the backend stores so a changed address repairs itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkWelcome {
    /// The version the node chose: the highest both peers speak.
    pub protocol: u32,
    pub node_id: String,
    pub running_agents: Vec<AgentId>,
    pub capabilities: MachineCapabilities,
    /// Direct hosts the machine can be dialed on.
    pub hosts: Vec<String>,
    /// The machine's tailcat address, when tailcat is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tc_address: Option<String>,
}

/// What a machine can run, reported in the welcome and on request: its
/// platform and intentd version, its shared agent limits, and each
/// provider's sign-in status and models. A snapshot taken at `observed_at`.
/// The report is advisory, never a reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineCapabilities {
    pub platform: AgentPlatform,
    pub intentd_version: String,
    pub capacity: MachineCapacity,
    pub providers: Vec<ProviderCapability>,
    pub observed_at: String,
}

/// A machine's shared agent limits and their use, counting local and
/// remote agents together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MachineCapacity {
    /// Agents counted against the cap, local and remote.
    pub running_agents: u32,
    /// The effective `agents.maxConcurrent`.
    pub agent_cap: u32,
    /// Aggregate memory charged to running agents.
    pub charged_memory_bytes: u64,
    /// The effective `agents.memoryBudgetMb` in bytes; absent when
    /// unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_budget_bytes: Option<u64>,
}

impl MachineCapabilities {
    /// A report with no agents running, no memory charged or budgeted and
    /// no providers.
    #[must_use]
    pub fn new(
        platform: AgentPlatform,
        intentd_version: impl Into<String>,
        agent_cap: u32,
        observed_at: impl Into<String>,
    ) -> Self {
        Self {
            platform,
            intentd_version: intentd_version.into(),
            capacity: MachineCapacity {
                running_agents: 0,
                agent_cap,
                charged_memory_bytes: 0,
                memory_budget_bytes: None,
            },
            providers: Vec::new(),
            observed_at: observed_at.into(),
        }
    }
}

/// One provider on a machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCapability {
    pub id: String,
    /// Whether the provider is shown in the machine's own provider list.
    pub visible: bool,
    pub status: ProviderStatus,
    pub models: Vec<String>,
    /// The model list is a cached one the machine could not refresh.
    pub models_stale: bool,
}

/// A provider's readiness on a machine, for its default account only.
/// `signed-in` / `signed-out` come from the provider's sign-in probe,
/// `auth-failed` from a launch without a profile that failed to
/// authenticate, `cli-missing` / `prerequisite-missing` from the machine's
/// model listing. `unknown` when the provider has no probe or it failed,
/// and for any status this build does not know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderStatus {
    SignedIn,
    SignedOut,
    AuthFailed,
    CliMissing,
    PrerequisiteMissing,
    #[serde(other)]
    Unknown,
}

/// Check the protocol version a peer chose.
///
/// # Errors
///
/// [`Error::InvalidParams`] `node-link-version-mismatch` naming both sides.
pub fn ensure_protocol(peer: u32) -> Result<()> {
    if NODE_LINK_PROTOCOLS.contains(peer) {
        Ok(())
    } else {
        Err(Error::InvalidParams(format!(
            "node-link-version-mismatch: peer speaks {peer}, this daemon {NODE_LINK_PROTOCOLS}"
        )))
    }
}

/// Head asks the node to start an agent. Every member below is part of
/// protocol 6; later additions must be optional with defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LaunchCommand {
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The commit head recorded as the agent's `base` ref; the node fetches
    /// it over the git connection and checks it out for the agent.
    pub base_commit: String,
    /// Names the node's checkout cache for the workspace's repository, so
    /// agents of one repository share fetched history. Opaque to the node.
    pub repo_key: String,
    /// The session's captured `agentFeatures` snapshot
    /// (`AgentFeaturesSettings`): the node gates the agent's `workspace_api`
    /// with it, as a local agent's bridge is gated.
    pub agent_features: Value,
    /// Whether the agent is a sub-agent (delegated or background), for the
    /// same top-level-only gates a local bridge applies.
    pub is_sub_agent: bool,
    /// The selected provider profile's environment overlay, or absent when
    /// no profile is selected and the node uses its own provider login.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_env: Option<LaunchProfileEnv>,
}

/// The environment overlay of the provider profile a launch runs with:
/// `{ providerRef, fingerprint, env: [{ key, value | null }] }`, where a
/// `null` value unsets the variable. Account directories (`CLAUDE_CONFIG_DIR`,
/// `CODEX_HOME`) are sent as typed; the node expands a leading `~` against
/// its own home. Values are secrets: `Debug` never renders them, and the
/// node parses the overlay strictly before applying it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LaunchProfileEnv(pub Value);

impl std::fmt::Debug for LaunchProfileEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LaunchProfileEnv(<redacted>)")
    }
}

/// A request over the link: node to head for forwarded workspace API calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkRequest {
    pub request_id: LinkId,
    /// The agent the call is made for; head checks it is on this lease.
    pub agent_id: AgentId,
    pub method: String,
    pub params: Value,
    pub timeout_ms: u32,
}

/// The JSON-RPC shaped failure of a [`LinkRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkRpcError {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// The outcome of a [`LinkRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LinkOutcome {
    Ok(Value),
    Err(LinkRpcError),
}

/// Which logical lane a frame belongs to. Git traffic uses its own
/// connection and has no frames here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkChannel {
    Control,
    Journal,
    Rpc,
}

/// One frame on the control/journal/RPC connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LinkFrame {
    Hello(LinkHello),
    Welcome(LinkWelcome),
    #[serde(rename_all = "camelCase")]
    Heartbeat {
        nonce: u64,
    },
    #[serde(rename_all = "camelCase")]
    HeartbeatReply {
        nonce: u64,
    },
    Launch(LaunchCommand),
    /// Start a turn: `prompt` is the ACP content blocks.
    #[serde(rename_all = "camelCase")]
    Prompt {
        agent_id: AgentId,
        turn_id: String,
        prompt: Vec<Value>,
    },
    /// Cancel the agent's running turn.
    #[serde(rename_all = "camelCase")]
    Cancel {
        agent_id: AgentId,
    },
    /// Stop the agent's process. The machine answers with a
    /// [`LinkFrame::StopAck`] for `stop_id` once the stop is applied; a
    /// repeated `stop_id` is the same stop.
    #[serde(rename_all = "camelCase")]
    Stop {
        agent_id: AgentId,
        stop_id: StopId,
    },
    /// Machine to backend: the stop `stop_id` for `agent_id` is applied.
    /// May arrive in any order relative to other frames and acks.
    #[serde(rename_all = "camelCase")]
    StopAck {
        stop_id: StopId,
        agent_id: AgentId,
        outcome: StopOutcome,
    },
    Journal(JournalRecord),
    /// Head has ingested every record up to and including `seq`.
    #[serde(rename_all = "camelCase")]
    Ack {
        seq: NodeCounter,
    },
    /// The node has resent every record after the hello's `ackedSeq`.
    #[serde(rename_all = "camelCase")]
    ReplayComplete {
        through_seq: NodeCounter,
    },
    Request(LinkRequest),
    #[serde(rename_all = "camelCase")]
    Response {
        request_id: LinkId,
        outcome: LinkOutcome,
    },
    #[serde(rename_all = "camelCase")]
    CancelRequest {
        request_id: LinkId,
    },
    /// Head asks for a fresh capabilities report (empty body).
    Capabilities,
    /// The node's answer to [`LinkFrame::Capabilities`].
    #[serde(rename_all = "camelCase")]
    CapabilitiesReply {
        capabilities: MachineCapabilities,
    },
}

impl LinkFrame {
    /// The lane this frame travels on.
    #[must_use]
    pub fn channel(&self) -> LinkChannel {
        match self {
            Self::Hello(_)
            | Self::Welcome(_)
            | Self::Heartbeat { .. }
            | Self::HeartbeatReply { .. }
            | Self::Launch(_)
            | Self::Prompt { .. }
            | Self::Cancel { .. }
            | Self::Stop { .. }
            | Self::StopAck { .. }
            | Self::Capabilities
            | Self::CapabilitiesReply { .. } => LinkChannel::Control,
            Self::Journal(_) | Self::Ack { .. } | Self::ReplayComplete { .. } => {
                LinkChannel::Journal
            }
            Self::Request(_) | Self::Response { .. } | Self::CancelRequest { .. } => {
                LinkChannel::Rpc
            }
        }
    }

    /// Whether a backend holding an executor credential may send this frame
    /// to the machine; the typed form of [`EXECUTOR_LINK_REQUESTS`].
    #[must_use]
    pub fn admitted_from_executor(&self) -> bool {
        match self {
            Self::Hello(_)
            | Self::Heartbeat { .. }
            | Self::HeartbeatReply { .. }
            | Self::Launch(_)
            | Self::Prompt { .. }
            | Self::Cancel { .. }
            | Self::Stop { .. }
            | Self::Ack { .. }
            | Self::Response { .. }
            | Self::Capabilities => true,
            Self::Welcome(_)
            | Self::Journal(_)
            | Self::ReplayComplete { .. }
            | Self::Request(_)
            | Self::CancelRequest { .. }
            | Self::CapabilitiesReply { .. }
            | Self::StopAck { .. } => false,
        }
    }

    /// Encode as JSON.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParams`] `node-link-frame-too-large` past
    /// [`MAX_FRAME_BYTES`]; [`Error::Internal`] if serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)
            .map_err(|e| Error::Internal(format!("node link frame encode: {e}")))?;
        check_frame_size(bytes.len())?;
        Ok(bytes)
    }

    /// Decode a JSON frame.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParams`] for an oversized or malformed frame.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        check_frame_size(bytes.len())?;
        serde_json::from_slice(bytes)
            .map_err(|e| Error::InvalidParams(format!("node-link-frame-invalid: {e}")))
    }
}

fn check_frame_size(len: usize) -> Result<()> {
    if len > MAX_FRAME_BYTES {
        return Err(Error::InvalidParams(format!(
            "node-link-frame-too-large: {len} bytes (limit {MAX_FRAME_BYTES})"
        )));
    }
    Ok(())
}

/// One durable record the node journals for an agent and streams to head.
/// `seq` increases by one per record across the whole lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalRecord {
    pub seq: NodeCounter,
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub recorded_at: String,
    pub body: JournalBody,
}

/// What a [`JournalRecord`] reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum JournalBody {
    /// The agent's isolated checkout is ready at `base_commit` and its
    /// provider session is set up; prompts can run.
    #[serde(rename_all = "camelCase")]
    Started { base_commit: String },
    /// An ACP notification from the provider (`session/update` params).
    #[serde(rename_all = "camelCase")]
    Notification { notification: Value },
    /// A turn finished; `stop_reason` is the ACP stop reason.
    #[serde(rename_all = "camelCase")]
    TurnEnded {
        stop_reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// The agent's provider process exited. `error` stays
    /// `"<code>: <detail>"`; protocol 6 adds the structured `failure`.
    #[serde(rename_all = "camelCase")]
    Exited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<ExitFailure>,
    },
}

/// `exited.failure`: the structured form of an exit error. `data` is
/// present only for `node-capacity` and is closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExitFailure {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<CapacityRefusal>,
}

/// How head treats an incoming record given its highest ingested sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalOrder {
    /// The next record: ingest it, then ack its `seq`.
    Next,
    /// Already ingested (a replay after reconnect): skip it, ack again.
    Duplicate,
    /// Records are missing before this one: do not ingest; the link resyncs.
    Gap,
}

impl JournalOrder {
    /// Classify `seq` against `acked`, the highest contiguous sequence head
    /// has ingested.
    #[must_use]
    pub fn classify(acked: NodeCounter, seq: NodeCounter) -> Self {
        if seq <= acked {
            Self::Duplicate
        } else if acked.0.checked_add(1) == Some(seq.0) {
            Self::Next
        } else {
            Self::Gap
        }
    }
}

#[cfg(test)]
mod tests;
