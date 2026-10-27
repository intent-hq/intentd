//! The data the remote-launch feature port (`intent_services::RemoteLaunchPort`)
//! exchanges: a launch request for an agent with a remote placement, and
//! where it started.

use serde::{Deserialize, Serialize};

use crate::placement::{AgentPlacement, AgentPlatform};
use crate::{AgentId, WorkspaceId};

/// Launch `agent_id` on a node matching `placement`. The caller has already
/// created the agent record and checked spawn authority;
/// [`AgentPlacement::check_remote_supported`] has passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLaunchRequest {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub placement: AgentPlacement,
    pub provider: String,
    pub model: Option<String>,
}

/// Where a remote agent was launched. These become the agent projection's
/// `nodeId`, `leaseId`, `nodeName` and resolved placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteLaunchStarted {
    pub node_id: String,
    pub node_name: String,
    pub lease_id: String,
    pub platform: AgentPlatform,
}
