//! The data the remote-launch feature port (`intent_services::RemoteLaunchPort`)
//! exchanges: where a remote agent runs, as recorded on head and projected
//! to clients.

use serde::{Deserialize, Serialize};

use crate::placement::AgentPlacement;
use crate::{AgentId, WorkspaceId};

/// Place the just-created agent `agent_id` on the node its admission
/// resolved (`view`): record it on the node's lease and point its `base`
/// ref at `view.base_commit`. Its provider process starts on its first turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLaunchRequest {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub view: RemotePlacementView,
}

/// One agent head runs on an execution node, under `lease_id`. Head claims
/// these agents when it binds a link; the node stops agents it runs that
/// head does not claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteAgentRecord {
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub node_id: String,
    pub lease_id: String,
    pub created_at: String,
}

/// Session `metadata` key recording where a remote agent runs: the agent
/// projection reads it, and an agent carrying it never runs locally.
/// Daemon-owned: it is written when the agent is created with a remote
/// placement and by the remote-launch writers only; whole-document metadata
/// writes and create-time caller metadata can neither set nor clear it. The
/// lease an agent runs under is the remote-agent record's.
pub const REMOTE_PLACEMENT_METADATA_KEY: &str = "remotePlacement";

/// `effectiveIsolation` of a remote agent: `pending` until the node has its
/// isolated checkout ready, then `isolated`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffectiveIsolation {
    Pending,
    Isolated,
}

/// `placementError`: why a remote agent could not be started on its node.
/// `code` is the node's error code (for example `node-capacity`), `detail`
/// its message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementError {
    pub code: String,
    pub detail: String,
}

impl PlacementError {
    /// Split a node error (`"<code>: <detail>"`) into its code and detail;
    /// an error without a code is `node-agent-start-failed`.
    #[must_use]
    pub fn from_node_error(error: &str) -> Self {
        match error.split_once(": ") {
            Some((code, detail))
                if !code.is_empty()
                    && code
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-') =>
            {
                Self {
                    code: code.to_owned(),
                    detail: detail.to_owned(),
                }
            }
            _ => Self {
                code: "node-agent-start-failed".to_owned(),
                detail: error.to_owned(),
            },
        }
    }
}

/// The [`REMOTE_PLACEMENT_METADATA_KEY`] value: the projection fields of an
/// agent placed on a node, computed when they change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemotePlacementView {
    pub node_id: String,
    pub lease_id: String,
    pub node_name: String,
    /// The resolved placement: the request with the node's platform and id.
    pub placement: AgentPlacement,
    pub effective_isolation: EffectiveIsolation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_error: Option<PlacementError>,
    /// The commit the agent starts from (its `base` ref).
    pub base_commit: String,
}

impl RemotePlacementView {
    /// Read the view from a session's `metadata`, if it carries one.
    #[must_use]
    pub fn from_metadata(metadata: Option<&serde_json::Value>) -> Option<Self> {
        let value = metadata?.get(REMOTE_PLACEMENT_METADATA_KEY)?;
        serde_json::from_value(value.clone()).ok()
    }
}

/// The message a remote launch reports to its caller: what the agent starts
/// from. Uncommitted changes in the workspace stay on head.
#[must_use]
pub fn launch_notice(node_name: &str, base_commit: &str) -> String {
    let short = base_commit.get(..12).unwrap_or(base_commit);
    format!(
        "The agent runs on execution node \"{node_name}\" and starts from commit {short} (the workspace's last commit). Uncommitted changes in the workspace are not carried to the remote agent; commit them first if it needs them."
    )
}
