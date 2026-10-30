//! The data the remote-launch feature port (`intent_services::RemoteLaunchPort`)
//! exchanges: where a remote agent runs, as recorded on head and projected
//! to clients.

use serde::{Deserialize, Serialize};

use crate::placement::{AgentPlacement, CapacityRefusal};
use crate::{AgentId, WorkspaceId};

pub mod workspace_api;

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
/// its message. A `node-capacity` refusal also carries the limit it hit
/// (`data`) and `retryable: true`; both are absent otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementError {
    pub code: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<CapacityRefusal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

/// Longest `placementError.detail` kept, in UTF-8 bytes: the field rides
/// every `agent.list` row, and node errors can quote command output.
pub const PLACEMENT_ERROR_DETAIL_MAX_BYTES: usize = 512;

/// Longest `placementError.code` kept, in bytes.
const PLACEMENT_ERROR_CODE_MAX_BYTES: usize = 64;

fn cap(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max.saturating_sub('…'.len_utf8());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

impl PlacementError {
    /// A placement error with `code` and `detail` capped to their list-row
    /// budgets.
    #[must_use]
    pub fn new(code: &str, detail: &str) -> Self {
        Self {
            code: cap(code, PLACEMENT_ERROR_CODE_MAX_BYTES),
            detail: cap(detail, PLACEMENT_ERROR_DETAIL_MAX_BYTES),
            data: None,
            retryable: None,
        }
    }

    /// A machine's capacity refusal: `node-capacity` with the limit it hit,
    /// retryable.
    #[must_use]
    pub fn capacity(detail: &str, refusal: CapacityRefusal) -> Self {
        Self {
            data: Some(refusal),
            retryable: Some(true),
            ..Self::new("node-capacity", detail)
        }
    }

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
                Self::new(code, detail)
            }
            _ => Self::new("node-agent-start-failed", error),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_errors_split_the_node_code_and_cap_the_detail() {
        let error =
            PlacementError::from_node_error("node-capacity: this node is running 2 of 2 agents");
        assert_eq!(error.code, "node-capacity");
        assert_eq!(error.detail, "this node is running 2 of 2 agents");
        let plain = PlacementError::from_node_error("Something: broke");
        assert_eq!(plain.code, "node-agent-start-failed");
        assert_eq!(plain.detail, "Something: broke");
        let long =
            PlacementError::from_node_error(&format!("node-checkout-failed: {}", "é".repeat(1000)));
        assert!(long.detail.len() <= PLACEMENT_ERROR_DETAIL_MAX_BYTES);
        assert!(long.detail.ends_with('…'));
        assert_eq!(
            serde_json::to_value(&plain).unwrap(),
            serde_json::json!({ "code": "node-agent-start-failed", "detail": "Something: broke" }),
            "data and retryable stay absent outside node-capacity"
        );
    }

    #[test]
    fn capacity_placement_errors_carry_the_limit_and_retry() {
        let error = PlacementError::capacity(
            "this machine is running 4 of 4 agents",
            CapacityRefusal::Count {
                running_agents: 4,
                agent_cap: 4,
            },
        );
        let wire = serde_json::to_value(&error).unwrap();
        assert_eq!(
            wire,
            serde_json::json!({
                "code": "node-capacity",
                "detail": "this machine is running 4 of 4 agents",
                "data": { "limit": "count", "runningAgents": 4, "agentCap": 4 },
                "retryable": true,
            })
        );
        assert_eq!(
            serde_json::from_value::<PlacementError>(wire).unwrap(),
            error
        );
        let legacy: PlacementError =
            serde_json::from_value(serde_json::json!({ "code": "x", "detail": "y" })).unwrap();
        assert_eq!(legacy, PlacementError::new("x", "y"));
    }
}
