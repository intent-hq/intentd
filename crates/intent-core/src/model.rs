//! Wire-facing domain structs (§9.1). Every struct uses
//! `#[serde(rename_all = "camelCase")]` so JSON matches the existing TS types
//! and PROTOCOL.md §5.1/§5.2. Enums serialize to their lowercase / snake_case
//! string forms, which are also their stored DB representations.

use serde::{Deserialize, Serialize};

use crate::ids::{AgentId, NoteId, WorkspaceId};

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

/// Note body content type (§9.1). `PlainText` serializes as `plain_text` to
/// match the TS `ContentType` enum (`src/shared/types.ts`); the others are their
/// lowercase names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentType {
    #[default]
    Markdown,
    #[serde(rename = "plain_text")]
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

/// Wire input for `workspace.create` (PROTOCOL §5.1). All fields are optional;
/// the service fills ids/timestamps/defaults. Unknown fields (e.g.
/// `initialAgent`) are ignored — initial-agent activation is fire-and-forget and
/// not part of this persistence slice.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkspaceCreate {
    pub title: Option<String>,
    pub status_message: Option<String>,
    pub branch: Option<String>,
    pub base_ref: Option<String>,
    pub base_commit_sha: Option<String>,
    pub tags: Option<Vec<String>>,
    pub path: Option<String>,
    pub repository_owner: Option<String>,
    pub repository_name: Option<String>,
    pub worktree_path: Option<String>,
    pub scope: Option<String>,
    pub skip_worktree: Option<bool>,
    pub setup_script: Option<String>,
    pub is_remote: Option<bool>,
    pub default_model: Option<String>,
}

/// Wire input for `workspace.update` (PROTOCOL §5.1). Every field is optional;
/// an absent field leaves the stored value unchanged (`workspaceId` is supplied
/// separately by the router).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkspaceUpdate {
    pub title: Option<String>,
    pub status_message: Option<String>,
    pub branch: Option<String>,
    pub base_ref: Option<String>,
    pub base_commit_sha: Option<String>,
    pub status: Option<WorkspaceStatus>,
    pub tags: Option<Vec<String>>,
    pub path: Option<String>,
    pub repository_owner: Option<String>,
    pub repository_name: Option<String>,
    pub worktree_path: Option<String>,
    pub scope: Option<String>,
    pub skip_worktree: Option<bool>,
    pub setup_script: Option<String>,
    pub is_remote: Option<bool>,
    pub default_model: Option<String>,
    pub pr_number: Option<u64>,
    pub pr_url: Option<String>,
    pub last_activity: Option<String>,
    pub attention: Option<WorkspaceAttention>,
    pub archived: Option<bool>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskMetadata>,
    pub created_at: String,
    pub updated_at: String,
}

/// Task workflow status (§9.1). Serializes to the `snake_case` strings the TS
/// app uses (`not_started`, `in_progress`, …); these are also the stored forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    #[default]
    NotStarted,
    Waiting,
    DiscussionNeeded,
    InProgress,
    ReviewRequired,
    Complete,
    Cancelled,
}

/// Task-note metadata (§9.1). Present iff a [`Note`] is a task; serialized into
/// the note's `task_json` column. Field names/optionality match the TS
/// `TaskMetadata` so the wire object is identical.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetadata {
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assigned_agent_ids: Vec<AgentId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acceptance_criteria: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_order: Option<i64>,
}

/// Comment discriminant (§9.1). Serializes to the TS wire form (e.g.
/// `change-request`) and is stored verbatim in the `comment.kind` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommentType {
    #[default]
    Comment,
    Suggestion,
    ChangeRequest,
    Question,
    Session,
}

/// Comment lifecycle status (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommentStatus {
    #[default]
    Open,
    Resolved,
    Accepted,
    Rejected,
    Pending,
}

/// Comment author kind (§9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthorType {
    #[default]
    User,
    Agent,
}

/// Anchor positioning kind for [`CommentAnchor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommentAnchorType {
    #[default]
    Range,
    Point,
}

/// Where a comment attaches in a note (§9.1). Matches the TS `CommentAnchor`
/// shape (`type` + optional `startId`/`endId`/`pointId`); stored as
/// `comment.anchor_json`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentAnchor {
    #[serde(rename = "type")]
    pub kind: CommentAnchorType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point_id: Option<String>,
}

/// Comment entity (§9.1; the TS `CommentV2` union flattened). The Rust field
/// `kind` serializes as `type` to match the TS wire (Rust reserves `type`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Comment {
    pub id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note_id: Option<NoteId>,
    #[serde(rename = "type")]
    pub kind: CommentType,
    pub content: String,
    pub author: String,
    pub author_type: AuthorType,
    pub status: CommentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub anchor: CommentAnchor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion_original: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion_proposed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub created_at: String,
    pub updated_at: String,
}

/// A comment thread: the comments sharing one `thread_id`, ordered by creation
/// time. Mirrors the TS `comment.getThread` result (`{ threadId, comments }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentThread {
    pub thread_id: String,
    pub comments: Vec<Comment>,
}

/// Wire input for `note.create` (PROTOCOL §5.2). `title` is required; the
/// service fills ids/timestamps/defaults. Built by the router from request
/// params.
#[derive(Debug, Clone, Default)]
pub struct NoteCreate {
    pub title: String,
    pub content: Option<String>,
    pub tags: Option<Vec<String>>,
    pub parent_id: Option<String>,
}

/// Wire input for the CRUD `note.update` path (PROTOCOL §5.2). `content`
/// present → raw full-content set; otherwise `title`/`tags` metadata update.
#[derive(Debug, Clone, Default)]
pub struct NoteUpdateInput {
    pub content: Option<String>,
    pub title: Option<String>,
    pub tags: Option<Vec<String>>,
}

/// Wire input for `note.add` (PROTOCOL §5.2).
#[derive(Debug, Clone, Default)]
pub struct NoteAddInput {
    pub content: String,
    pub heading: Option<String>,
    pub position: Option<String>,
}

/// Wire input for `note.edit` (PROTOCOL §5.2).
#[derive(Debug, Clone, Default)]
pub struct NoteEditInput {
    pub old: String,
    pub new: String,
}

/// Wire input for `note.editLines` (PROTOCOL §5.2); 1-based inclusive lines.
#[derive(Debug, Clone, Default)]
pub struct NoteEditLinesInput {
    pub start: i64,
    pub end: i64,
    pub content: String,
}

/// Result of `note.add` — mirrors the TS `ws.note.add` peer return shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteAddResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub added_length: usize,
    pub total_length: usize,
    pub position: String,
    pub old_content: String,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
}

/// Result of `note.edit` — first exact-match replacement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteEditResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub old_text_length: usize,
    pub new_text_length: usize,
    /// Scalar (char) offset of the match, or `-1` when the note was empty.
    pub match_position: i64,
    pub old_content: String,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
}

/// Result of `note.editLines` — 1-based inclusive replace/delete/insert.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteEditLinesResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub start_line: i64,
    pub end_line: i64,
    pub total_lines_before: usize,
    pub total_lines_after: usize,
    pub old_content: String,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
}

/// Result of `note.setContent` — full replace with the reduction guard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteSetContentResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_title: Option<String>,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_content: Option<String>,
    pub new_content: String,
    pub converted_count: i64,
    pub created_task_note_ids: Vec<String>,
}

/// Result of `note.updateMetadata`. Either a normal title/tags update or a
/// `skipped` response (spec title cannot be modified).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteUpdateMetadataResult {
    pub ok: bool,
    pub note_id: NoteId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Result of `note.delete`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteDeleteResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub deleted: bool,
}

/// One parsed checkbox row returned by `note.listTasks`. `taskNoteId` is
/// serialized as `null` when the row has no `intent://local/task/<id>` link.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteTaskRow {
    pub line_number: usize,
    pub text: String,
    pub status: String,
    pub task_note_id: Option<String>,
    pub linked_task_note_id: Option<String>,
}

/// Result of `note.readAsset` (PROTOCOL §5.2). `data` is base64; `sizeKb` is
/// rounded from the base64 string length to match the TS peer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadAssetResult {
    pub asset_id: String,
    pub mime_type: String,
    pub data: String,
    pub size_kb: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_wire_forms_match_ts() {
        // Mirrors `ContentType` in src/shared/types.ts: plain_text (not plaintext).
        for (variant, wire) in [
            (ContentType::Markdown, "\"markdown\""),
            (ContentType::PlainText, "\"plain_text\""),
            (ContentType::Json, "\"json\""),
            (ContentType::Code, "\"code\""),
        ] {
            assert_eq!(serde_json::to_string(&variant).unwrap(), wire);
            assert_eq!(serde_json::from_str::<ContentType>(wire).unwrap(), variant);
        }
    }
}
