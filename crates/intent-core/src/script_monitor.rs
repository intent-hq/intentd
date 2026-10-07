//! Script-run monitor wire vocabulary. Output is opt-in and bounded to one line.
use crate::{AgentId, ScriptLastRun, ScriptMode, WorkspaceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptMonitor {
    pub monitor_id: String,
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub script_id: String,
    pub run_id: String,
    pub script_name: String,
    pub mode: ScriptMode,
    pub state: String,
    pub created_at: String,
    pub expires_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settled_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ScriptLastRun>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<ScriptMonitorTrigger>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptMonitorTrigger {
    pub observed_line_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_line: Option<String>,
}
