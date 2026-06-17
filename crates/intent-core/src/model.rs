//! Wire-facing domain structs (§9.1). Every struct uses
//! `#[serde(rename_all = "camelCase")]` so JSON matches the existing TS types
//! and PROTOCOL.md §5.1/§5.2. Enums serialize to their lowercase / snake_case
//! string forms, which are also their stored DB representations.

use serde::{Deserialize, Serialize};

use crate::ids::{NoteId, WorkspaceId};

/// Workspace lifecycle (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceStatus {
    #[default]
    Active,
    Archived,
    Deleted,
}

/// Derived in-flight agent state (green dot; read-only, not persisted) (§9.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceActivity {
    #[default]
    Idle,
    AgentRunning,
}

/// Dismissible attention flag (blue dot; server-owned) (§9.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAttention {
    #[default]
    None,
    Unread,
    ReviewRequired,
}

/// Note body content type (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentType {
    #[default]
    Markdown,
    PlainText,
    Json,
    Code,
}

/// Note visibility (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NoteVisibility {
    Private,
    #[default]
    Workspace,
    Shared,
    Public,
}

/// Workspace entity (§9.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Workspace {
    pub id: WorkspaceId,
    pub title: String,
    pub branch: String,
    pub base_ref: Option<String>,
    pub base_commit_sha: Option<String>,
    pub status: WorkspaceStatus,
    pub status_message: Option<String>,
    /// Derived, read-only; never persisted (§9.9).
    pub activity: WorkspaceActivity,
    pub attention: WorkspaceAttention,
    pub created_at: String,
    pub updated_at: String,
    pub last_activity: Option<String>,
    pub tags: Vec<String>,
    pub path: Option<String>,
    pub repository_owner: Option<String>,
    pub repository_name: Option<String>,
    pub worktree_path: Option<String>,
    pub scope: Option<String>,
    pub skip_worktree: bool,
    pub setup_script: Option<String>,
    pub is_remote: bool,
    pub default_model: Option<String>,
    pub pr_number: Option<u64>,
    pub pr_url: Option<String>,
    pub archived: bool,
    pub archived_at: Option<String>,
}

/// Note entity (§9.1). `task` carries serialized task metadata when the note is
/// a task; this slice treats it opaquely (stored as `task_json` TEXT).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Note {
    pub id: NoteId,
    pub workspace_id: WorkspaceId,
    pub title: String,
    pub content: String,
    pub content_type: ContentType,
    pub tags: Vec<String>,
    pub is_pinned: bool,
    pub is_archived: bool,
    pub is_default: bool,
    pub parent_id: Option<NoteId>,
    pub visibility: NoteVisibility,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
}
