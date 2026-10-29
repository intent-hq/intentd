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
use crate::placement::AgentPlatform;
use crate::{AgentId, Error, Result, WorkspaceId};

/// Identifies one RPC request on the link.
pub type LinkId = uuid::Uuid;

/// The private node-link protocol version of this single-node link. The
/// prepared enrollment-based versions 2-4 in monorepo
/// `docs/protocol/node-link.md` are a different protocol; peers must match
/// exactly.
pub const NODE_LINK_PROTOCOL: u32 = 5;

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

/// First frame, head to node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinkHello {
    pub protocol: u32,
    pub intentd_version: String,
    pub binding: LeaseBinding,
    /// Highest journal sequence head has ingested; the node replays
    /// everything after it.
    pub acked_seq: NodeCounter,
}

/// The node's reply to an accepted [`LinkHello`]: its platform, capacity and
/// the agents it is running, so head can rebuild its capacity count and stop
/// agents it does not claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinkWelcome {
    pub protocol: u32,
    pub intentd_version: String,
    pub node_id: String,
    pub platform: AgentPlatform,
    pub max_agents: u32,
    pub running_agents: Vec<AgentId>,
}

/// Check a peer's protocol version.
///
/// # Errors
///
/// [`Error::InvalidParams`] `node-link-version-mismatch` naming both versions.
pub fn ensure_protocol(peer: u32) -> Result<()> {
    if peer == NODE_LINK_PROTOCOL {
        Ok(())
    } else {
        Err(Error::InvalidParams(format!(
            "node-link-version-mismatch: peer speaks {peer}, this daemon {NODE_LINK_PROTOCOL}"
        )))
    }
}

/// Head asks the node to start an agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// Stop the agent's process.
    #[serde(rename_all = "camelCase")]
    Stop {
        agent_id: AgentId,
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
            | Self::Stop { .. } => LinkChannel::Control,
            Self::Journal(_) | Self::Ack { .. } | Self::ReplayComplete { .. } => {
                LinkChannel::Journal
            }
            Self::Request(_) | Self::Response { .. } | Self::CancelRequest { .. } => {
                LinkChannel::Rpc
            }
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    /// The agent's provider process exited.
    #[serde(rename_all = "camelCase")]
    Exited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
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
