//! Wire-facing domain structs (§9.1). Every struct uses
//! `#[serde(rename_all = "camelCase")]` so JSON matches the existing TS types
//! and PROTOCOL.md §5.1/§5.2. Enums serialize to their lowercase / snake_case
//! string forms, which are also their stored DB representations.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{AgentId, ClientId, NoteId, WorkspaceId};

/// Workspace lifecycle (§9.1; TS `WorkspaceStatus` in `src/shared/types.ts`).
/// Wire values are the PascalCase variant names (`Active`/`Inactive`/`Archived`/
/// `Deleted`), matching the TS string enum exactly; these are also the stored DB
/// words (the column DEFAULT is unused — inserts always bind explicitly).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum WorkspaceStatus {
    #[default]
    Active,
    Inactive,
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

/// Pull-request lifecycle status (§9.1; TS `PullRequestStatus` in
/// `src/shared/types.ts`). Wire values are the PascalCase variant names
/// (`Open`/`Closed`/`Merged`/`Draft`), matching the TS string enum exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PullRequestStatus {
    Open,
    Closed,
    Merged,
    Draft,
}

/// Pull-request metadata persisted on a [`Workspace`] as `activePullRequest`
/// (§7.6; TS `PullRequestInfo` in `src/shared/types.ts`). A focused subset of
/// the TS shape populated from the host-agnostic forge `PullRequest`; required
/// fields match the TS required set, and absent optionals are omitted from the
/// wire to mirror `JSON.stringify`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequestInfo {
    pub id: String,
    pub number: u64,
    pub url: String,
    pub title: String,
    pub status: PullRequestStatus,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mergeable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mergeable_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_draft: Option<bool>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_commit_sha: Option<String>,
    pub status: WorkspaceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// Derived, read-only; never persisted (§9.9).
    pub activity: WorkspaceActivity,
    pub attention: WorkspaceAttention,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub skip_worktree: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_script: Option<String>,
    pub is_remote: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_number: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    /// Persisted PR lifecycle status for the linked PR (§7.6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_status: Option<PullRequestStatus>,
    /// Persisted snapshot of the linked PR (§7.6); refreshed in the background.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_pull_request: Option<PullRequestInfo>,
    pub archived: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<String>,
    /// Card-aggregate rollups (§9.1). Populated only on the `workspace.list` /
    /// `workspace.get` emit paths (omitted elsewhere, e.g. `create`/`update`).
    /// The iOS coverflow cards read `taskStats`, `agentSummary`, and
    /// `diffSummary`; each is omitted (not `null`) when not computable so absent
    /// simply yields a sparser card.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_stats: Option<WorkspaceTaskStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_summary: Option<WorkspaceAgentSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_summary: Option<WorkspaceDiffSummary>,
}

/// `Workspace.taskStats` card aggregate (§9.1; TS `WorkspaceTaskStats`). Ports
/// the canonical `computeTaskStats` (`task-stats.ts`): `total` excludes
/// `cancelled`, `completed` counts `complete`, and `inProgress` counts
/// `in_progress` + `review_required`. The optional renderer-only per-task
/// `tasks` array is omitted (the server source-of-truth emits only the counts).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceTaskStats {
    pub total: usize,
    pub completed: usize,
    pub in_progress: usize,
}

/// One entry of [`WorkspaceAgentSummary::agents`] (§5.5 card; TS
/// `WorkspaceAgentInfo`). The live iOS `WorkspaceStore.parseWorkspace` decodes
/// `id`/`name`/`status`/`isStreaming`/`isResponding` as non-optional and
/// `specialist`/`lastActivity` as optional. `status` carries the same wire
/// strings as `agent.list`; `isStreaming`/`isResponding` are always `false`
/// (the headless backend has no live stream state — `status` carries liveness,
/// matching the `AgentLite` iOS wire-shape parity decision).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceAgentInfo {
    pub id: AgentId,
    pub name: String,
    pub status: AgentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specialist: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    pub is_streaming: bool,
    pub is_responding: bool,
}

/// `Workspace.agentSummary` card aggregate. The iOS coverflow reads the richer
/// `{ count, agents }` object (the live `WorkspaceStore.parseWorkspace`
/// consumer); `agentIds` is additionally emitted alongside it for forward-compat
/// with the TS `WorkspaceAgentIdSummary { agentIds }` (a future
/// desktop-on-intentd reads `agentSummary?.agentIds ?? []`). `agentIds` lists
/// the same agents used to build `agents` in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceAgentSummary {
    pub count: usize,
    pub agents: Vec<WorkspaceAgentInfo>,
    pub agent_ids: Vec<AgentId>,
}

/// One entry of [`WorkspaceDiffSummary::files`] (TS `WorkspaceDiffSummaryFile`).
/// The on-demand workspace card summary emits an empty `files` array (per-file
/// detail is fetched via the dedicated diff endpoints), but the type matches the
/// TS wire shape for completeness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDiffSummaryFile {
    pub path: String,
    /// `create` | `modify` | `delete` | `rename` (TS `DiffSummaryFileAction`).
    pub action: String,
    pub additions: usize,
    pub deletions: usize,
}

/// `Workspace.diffSummary` card aggregate (§9.1; TS `WorkspaceDiffSummary`).
/// Ports the on-demand `computeWorkspaceDiffSummary` (`workspace-summaries.ts`):
/// `totalFiles` counts changed-vs-`HEAD` (staged+unstaged) plus untracked files;
/// `totalAdditions`/`totalDeletions` sum line stats over the tracked changes.
/// iOS reads `totalFiles`; `files` mirrors the on-demand source (empty array).
/// Omitted from a workspace when there are no changes (or no git worktree).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDiffSummary {
    pub schema_version: u32,
    pub updated_at: String,
    pub total_files: usize,
    pub total_additions: usize,
    pub total_deletions: usize,
    pub files: Vec<WorkspaceDiffSummaryFile>,
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
    pub repository_path: Option<String>,
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
    pub repository_path: Option<String>,
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

// ---------------------------------------------------------------------------
// task.* result DTOs (PROTOCOL §5.4). Field names/optionality match the TS
// `ws.task.*` peer returns so the iOS client is unchanged.
// ---------------------------------------------------------------------------

/// Result of `task.updateStatus` (checkbox edit by task text).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateStatusResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub task_text: String,
    /// Checkbox status string: `todo` / `in-progress` / `done`.
    pub status: String,
}

/// Result of `task.updateNoteStatus` (task-note metadata status).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateNoteStatusResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub status: TaskStatus,
    pub note: Note,
}

/// Result of `task.update` (atomic single-line edit).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub line_number: i64,
    pub previous_text: String,
    pub new_text: String,
    /// Checkbox status string: `todo` / `in-progress` / `done`.
    pub status: String,
}

/// One subtask row in [`TaskGetMyTaskResult`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSubtask {
    pub id: NoteId,
    pub title: String,
    /// Child task status string, or `unknown` if the child lost its metadata.
    pub status: String,
}

/// Result of `task.getMyTask`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskGetMyTaskResult {
    pub note_id: NoteId,
    pub title: String,
    pub content: String,
    pub status: TaskStatus,
    pub task_metadata: TaskMetadata,
    pub parent_id: Option<NoteId>,
    pub subtasks: Vec<TaskSubtask>,
    pub assigned_agents: Vec<AgentId>,
}

/// Result of `task.markAsTask`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskMarkAsTaskResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub status: TaskStatus,
}

/// Result of `task.convertBlocks`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskConvertBlocksResult {
    pub ok: bool,
    pub converted_count: i64,
    pub created_note_ids: Vec<String>,
}

/// Result of `task.createPrerequisite`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCreatePrerequisiteResult {
    pub ok: bool,
    pub prerequisite_note_id: NoteId,
    pub dependent_note_id: NoteId,
    pub title: String,
}

/// Result of `task.assignAgent`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskAssignAgentResult {
    pub ok: bool,
    pub note_id: NoteId,
    pub agent_id: AgentId,
}

// ---------------------------------------------------------------------------
// comment.* wire DTOs (PROTOCOL §5.3). The stored [`Comment`] keeps anchor and
// suggestion fields flat; on the wire they nest into `anchorContext` /
// `suggestionDiff` to match the TS `CommentV2` shape the iOS client expects.
// ---------------------------------------------------------------------------

/// Nested `anchorContext` on the wire (`{ before, after }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorContext {
    pub before: String,
    pub after: String,
}

/// Nested `suggestionDiff` on the wire (`{ original, proposed }`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SuggestionDiff {
    pub original: String,
    pub proposed: String,
}

/// Wire-facing comment (the TS `CommentV2`). Built from the flat [`Comment`]
/// via [`CommentWire::from_comment`]; nests `anchorContext`/`suggestionDiff`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentWire {
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
    pub anchor_context: Option<AnchorContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion_diff: Option<SuggestionDiff>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    pub created_at: String,
    pub updated_at: String,
}

impl CommentWire {
    /// Map a stored [`Comment`] to its nested wire shape.
    pub fn from_comment(c: &Comment) -> Self {
        let anchor_context = match (&c.anchor_before, &c.anchor_after) {
            (None, None) => None,
            (before, after) => Some(AnchorContext {
                before: before.clone().unwrap_or_default(),
                after: after.clone().unwrap_or_default(),
            }),
        };
        let suggestion_diff = match (&c.suggestion_original, &c.suggestion_proposed) {
            (Some(original), Some(proposed)) => Some(SuggestionDiff {
                original: original.clone(),
                proposed: proposed.clone(),
            }),
            _ => None,
        };
        Self {
            id: c.id.clone(),
            thread_id: c.thread_id.clone(),
            note_id: c.note_id.clone(),
            kind: c.kind,
            content: c.content.clone(),
            author: c.author.clone(),
            author_type: c.author_type,
            status: c.status,
            parent_id: c.parent_id.clone(),
            anchor: c.anchor.clone(),
            anchor_text: c.anchor_text.clone(),
            anchor_context,
            suggestion_diff,
            agent_id: c.agent_id.clone(),
            created_at: c.created_at.clone(),
            updated_at: c.updated_at.clone(),
        }
    }
}

/// Anchor location echoed by `comment.add`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentLocation {
    pub line: usize,
    pub anchored_text: String,
}

/// Result of `comment.add`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentAddResult {
    pub success: bool,
    pub message: String,
    pub comment_id: String,
    pub anchored: bool,
    pub location: CommentLocation,
}

/// One thread summary in `comment.list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentThreadSummary {
    pub thread_id: String,
    pub note_id: NoteId,
    pub targeted_text: Option<String>,
    pub anchor_id: Option<String>,
    pub status: String,
    pub created_at: String,
    pub last_activity: String,
    pub latest_comment_author: String,
    pub latest_comment_author_type: AuthorType,
    pub latest_comment_at: String,
    pub comment_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comments: Option<Vec<CommentWire>>,
}

/// Result of `comment.list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentListResult {
    pub threads: Vec<CommentThreadSummary>,
    pub total_threads: usize,
    pub total_comments: usize,
}

/// Result of `comment.getThread`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentGetThreadResult {
    pub thread_id: String,
    pub note_id: NoteId,
    pub root_comment: CommentWire,
    pub replies: Vec<CommentWire>,
    pub total_comments: usize,
    pub status: String,
}

/// The `thread` summary echoed by `comment.respond`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentRespondThread {
    pub thread_id: String,
    pub total_comments: usize,
}

/// Result of `comment.respond`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentRespondResult {
    pub success: bool,
    pub message: String,
    pub comment: CommentWire,
    pub thread: CommentRespondThread,
}

/// Result of `comment.delete`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommentDeleteResult {
    pub success: bool,
    pub message: String,
}

/// Event actor kind (§9.1, `events/types.ts` `ActorType`). Serializes to its
/// lowercase string form, matching the TS wire values used by the iOS client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorType {
    #[default]
    User,
    Agent,
    System,
    External,
    Tool,
}

/// Who originated an event (§9.1; `events/types.ts` `EventActor`). The `type`
/// field is required; the rest are optional and omitted from the wire when
/// absent, matching the TS shape. Stored as the `event.actor` JSON column.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventActor {
    #[serde(rename = "type")]
    pub actor_type: ActorType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Append-only workspace event (§9.1 / §10; `events/types.ts`
/// `WorkspaceEventBase`). `event_type` serializes as `type` and the
/// type-specific payload lives in `data`, matching the TS/iOS wire shape.
/// Persisted to the insert-only `event` table; never updated or deleted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub timestamp: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: EventActor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub data: serde_json::Value,
}

/// One file-change activity row (§5.10; `agent-event-tools.ts` `FileActivity`).
/// Returned by `event.recentFiles` / `event.directoryChanges` and embedded in
/// `event.workspaceSummary`. `actor` is `"type:name"` for the workspace-wide
/// helpers and the bare actor name for the per-agent variant; absent optionals
/// are omitted from the wire to match the TS `JSON.stringify` shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileActivity {
    pub path: String,
    pub relative_path: String,
    pub action: String,
    pub timestamp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additions: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletions: Option<serde_json::Value>,
}

/// Aggregated per-agent activity (§5.10; `agent-event-tools.ts` `AgentActivity`).
/// Returned by `event.agentActivity` (no `agentId`) and embedded in
/// `event.workspaceSummary.activeAgents`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentActivity {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    pub event_count: i64,
    pub tool_calls: i64,
    pub files_modified: Vec<String>,
    pub last_active: String,
}

/// One entry of `event.workspaceSummary.topChangedFiles` (§5.10).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopChangedFile {
    pub path: String,
    pub change_count: i64,
}

/// `event.workspaceSummary` result (§5.10; `WorkspaceActivity` in
/// `agent-event-tools.ts`, renamed here to avoid colliding with the
/// lifecycle [`WorkspaceActivity`] enum).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceEventSummary {
    pub recent_files: Vec<FileActivity>,
    pub active_agents: Vec<AgentActivity>,
    pub event_rate: f64,
    pub top_changed_files: Vec<TopChangedFile>,
}

/// `event.subscribe` (deprecated alias) service result (§5.10 / §6). Mirrors the
/// `ws.event.subscribe` peer return `{ subscriptionId, eventTypes }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSubscribeResult {
    pub subscription_id: String,
    pub event_types: Vec<String>,
}

/// `event.unsubscribe` (deprecated alias) service result (§5.10 / §6). Mirrors
/// the `ws.event.unsubscribe` peer return `{ ok: true, subscriptionId }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventUnsubscribeResult {
    pub ok: bool,
    pub subscription_id: String,
}

/// Filter inputs for `event.query` (§5.10). Built by the transport router from
/// request params and consumed by the service layer; not serialized on the wire.
#[derive(Debug, Clone, Default)]
pub struct EventQueryParams {
    pub event_type: Option<String>,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub path: Option<String>,
    pub minutes_ago: Option<i64>,
    pub limit: Option<i64>,
    /// Opt-in pagination (TA-2 / §5.5): when set true (or when `page_token` is
    /// present), `event.query` returns the `{ items, nextToken }` envelope
    /// (newest→oldest, limit clamped to [1,200] default 50) instead of the
    /// legacy bare array. Absent/false preserves the legacy shape.
    pub paginate: Option<bool>,
    /// Opaque continuation token from a previous paginated `event.query`.
    pub page_token: Option<String>,
}

/// Agent runtime status (§9.1; `AgentStatus` in `agent.types.ts`). The modern
/// values are lowercase (`pending`/`active`/`idle`/`error`/`deleted`); the
/// capitalized variants are legacy values kept so persisted sessions round-trip
/// without rewriting. Mixed casing means each variant carries an explicit
/// `rename` rather than a blanket `rename_all`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AgentStatus {
    #[default]
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "active")]
    Active,
    /// Lowercase `idle` persisted by app-level runtime events (including Chief).
    #[serde(rename = "idle")]
    RuntimeIdle,
    #[serde(rename = "error")]
    Error,
    #[serde(rename = "deleted")]
    Deleted,
    /// Legacy capitalized values kept for backward-compatible round-trips.
    #[serde(rename = "Idle")]
    Idle,
    #[serde(rename = "Waiting")]
    Waiting,
    #[serde(rename = "Completed")]
    Completed,
    #[serde(rename = "Processing")]
    Processing,
}

/// Per-session credit/message/tool stats (§9.1 / §19.2). A derived snapshot
/// populated from `auggie session stats --json`; it is **not** persisted in the
/// `agent_session` table (the `stats` field is recomputed on demand). Field
/// names match the TS `SessionStats`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credits_used: Option<f64>,
    pub message_count: u64,
    pub tool_count: u64,
}

/// One row of the append-only agent conversation log (§9.2 `agent_message`).
/// `seq` is monotonic per agent (enforced by `UNIQUE(agent_id, seq)`); `content`
/// holds the message's JSON content blocks. Names use camelCase to match the
/// wire shape returned by `agent.getConversation` (PROTOCOL §5.5). The `content`
/// and `created_at` fields serialize as `contentBlocks`/`timestamp` to match the
/// TS `AgentMessage` (`src/shared/types/agent-message.ts`); the Rust identifiers
/// stay `content`/`created_at` so DB code and call sites are unchanged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentMessage {
    pub id: String,
    pub agent_id: AgentId,
    pub seq: i64,
    /// `user` | `assistant` | `tool` | `system`.
    pub role: String,
    #[serde(rename = "contentBlocks")]
    pub content: serde_json::Value,
    #[serde(rename = "timestamp")]
    pub created_at: String,
}

/// Agent runtime session (§9.1). Field names/casing match the TS `AgentSession`
/// (`agent-session.ts`): `backendSessionId`, `acpSessionId` (write-once after
/// the provider's `session:created`), `nameExplicitlySet`, `systemPrompt`, etc.
/// `messages` is the append-only conversation log; `stats` is a derived snapshot
/// (not persisted, §19.2). `provider` is immutable once set on first real use.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSession {
    pub id: AgentId,
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_session_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub name_explicitly_set: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Specialist id this agent was created with (`agent.create`'s `specialistId`),
    /// surfaced as `metadata.specialist` in the `AgentLite` projection. `None` for
    /// plain (non-specialist) agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specialist: Option<String>,
    pub status: AgentStatus,
    #[serde(default)]
    pub is_active: bool,
    #[serde(default)]
    pub messages: Vec<AgentMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SessionStats>,
    pub created_at: String,
    pub updated_at: String,
}

/// Nested `metadata` object on [`AgentLite`] (PROTOCOL §5.5). Mirrors the subset
/// of the TS `AgentMetadata` the iOS `AgentSession.parseAgent` reads:
/// `isBackground`, `specialist`, `createdByAgentId` (the parent/spawning agent),
/// and `taskNoteId`. `isBackground` is always emitted (iOS reads it with a
/// `false` default); the rest are omitted when absent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentMetadata {
    pub is_background: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specialist: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_note_id: Option<NoteId>,
}

/// Lightweight `agent.list` / `agent.get` projection (PROTOCOL §5.5). Mirrors
/// the TS `AgentLite`: the full [`AgentSession`] with `messages` and
/// `systemPrompt` stripped (clients fetch the transcript via
/// `agent.getConversation`), plus a derived `messageCount`, the
/// `lastAgentResponse` / `digest` / `lastUserMessage` computed from the
/// transcript, a nested `metadata` object, and the runtime activity flags the
/// iOS coverflow reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentLite {
    pub id: AgentId,
    pub workspace_id: WorkspaceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_session_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub name_explicitly_set: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub status: AgentStatus,
    #[serde(default)]
    pub is_active: bool,
    /// Runtime activity flags (iOS `isStreaming`/`isProcessing`/`isResponding`).
    /// The headless backend has no live stream state in this projection, so all
    /// three are `false`; `status` (+ `isActive`) carry the liveness signal.
    #[serde(default)]
    pub is_streaming: bool,
    #[serde(default)]
    pub is_processing: bool,
    #[serde(default)]
    pub is_responding: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SessionStats>,
    pub created_at: String,
    pub updated_at: String,
    /// Most-recent activity timestamp; derived from `updated_at` (iOS falls back
    /// to this after `updatedAt`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<String>,
    pub message_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_agent_response: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_user_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    pub metadata: AgentMetadata,
}

impl AgentLite {
    /// Project an [`AgentSession`] into its `agent.list`/`agent.get` form,
    /// stripping `messages`/`systemPrompt` and attaching the derived fields.
    pub fn from_session(
        session: AgentSession,
        message_count: u64,
        last_agent_response: Option<String>,
        last_user_message: Option<String>,
        digest: Option<String>,
    ) -> Self {
        let metadata = AgentMetadata {
            is_background: false,
            specialist: session.specialist,
            created_by_agent_id: session.parent_agent_id.clone(),
            task_note_id: None,
        };
        Self {
            id: session.id,
            workspace_id: session.workspace_id,
            parent_agent_id: session.parent_agent_id,
            backend_session_id: session.backend_session_id,
            acp_session_id: session.acp_session_id,
            name: session.name,
            name_explicitly_set: session.name_explicitly_set,
            model: session.model,
            provider: session.provider,
            status: session.status,
            is_active: session.is_active,
            is_streaming: false,
            is_processing: false,
            is_responding: false,
            stats: session.stats,
            last_activity: Some(session.updated_at.clone()),
            created_at: session.created_at,
            updated_at: session.updated_at,
            message_count,
            last_agent_response,
            last_user_message,
            digest,
            metadata,
        }
    }
}

/// Wire input for `agent.delegate` (PROTOCOL §5.5). `workspaceId` is passed
/// separately; these are the delegation options. Built by the router/MCP
/// surface; the runtime wiring lands in a later milestone.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AgentDelegateInput {
    pub task_note_id: Option<NoteId>,
    pub note_id: Option<NoteId>,
    pub task_text: Option<String>,
    pub agent_instructions: Option<String>,
    pub specialist: Option<String>,
    pub model: Option<String>,
    pub behavior_prompt: Option<String>,
    pub wait_mode: Option<String>,
    pub skip_auto_commit: Option<bool>,
}

/// A single file's status line, mirroring the TS `GitFileStatus` enum
/// (`src/shared/types.ts`). Serializes to the porcelain status character so the
/// wire shape matches `git.status` exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitFileStatus {
    #[serde(rename = "M")]
    Modified,
    #[serde(rename = "A")]
    Added,
    #[serde(rename = "D")]
    Deleted,
    #[serde(rename = "R")]
    Renamed,
    #[serde(rename = "C")]
    Copied,
    #[serde(rename = "?")]
    Untracked,
    #[serde(rename = "!")]
    Ignored,
}

/// One entry in [`GitStatus::files`] (`{ path, status, staged }`), mirroring the
/// TS `FileStatus`. A file with both staged and unstaged changes yields two
/// entries (matching the TS `parseStatusOutput`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileStatus {
    pub path: String,
    pub status: GitFileStatus,
    pub staged: bool,
}

/// `git.status` result (`GitStatus` in `src/shared/types.ts`). `diverged` is true
/// only when the branch is both ahead and behind its upstream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitStatus {
    pub branch: String,
    pub ahead: i64,
    pub behind: i64,
    pub diverged: bool,
    pub files: Vec<FileStatus>,
    pub has_uncommitted_changes: bool,
    pub has_untracked_files: bool,
}

/// `git.getBranches` result (`{ branches, remoteBranches, currentBranch,
/// defaultBranch }`), matching the TS `git.getBranches` handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitBranches {
    pub branches: Vec<String>,
    pub remote_branches: Vec<String>,
    pub current_branch: String,
    pub default_branch: String,
}

/// `git.commit` service result (the `ok` flag is added by the transport). Mirrors
/// the TS `ws.git.commit` payload `{ hash?, files? }`; on success both are
/// present (`hash` is the new commit SHA, `files` the files it changed).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitCommitResult {
    pub hash: String,
    pub files: Vec<String>,
}

/// `git.agentCommit` service result (the `ok` flag is added by the transport).
/// Mirrors the TS `ws.git.agentCommit` payload `{ hash, files, fileCount }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitAgentCommitResult {
    pub hash: String,
    pub files: Vec<String>,
    pub file_count: i64,
}

/// `git.checkMergeConflicts` result, mirroring the TS `ws.git.checkMergeConflicts`
/// payload `{ hasConflicts, conflictedFiles, cannotDetermine?, targetBranch,
/// currentBranch }`. `cannotDetermine` is omitted unless the merge base could not
/// be resolved (the TS legacy fallback's only producer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitMergeConflicts {
    pub has_conflicts: bool,
    pub conflicted_files: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cannot_determine: Option<bool>,
    pub target_branch: String,
    pub current_branch: String,
}

/// Script execution mode (`script.*`, PROTOCOL §5.8; ported from the TS
/// `ScriptMode`). `service` is a long-running, auto-restartable process (dev
/// server / watcher); `command` runs once to completion (build / test / lint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptMode {
    #[default]
    Service,
    Command,
}

/// Runtime status of a script process (ported from the TS `ScriptStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScriptStatus {
    #[default]
    Idle,
    Running,
    Exited,
}

/// In-memory runtime state of a script process — the `script.status` result and
/// the `runtime` field of a `script.list` entry (ported from the TS
/// `ScriptRuntimeState`). Not persisted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptRuntimeState {
    pub status: ScriptStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<String>,
    pub restart_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_url: Option<String>,
}

impl Default for ScriptRuntimeState {
    fn default() -> Self {
        Self {
            status: ScriptStatus::Idle,
            pid: None,
            exit_code: None,
            started_at: None,
            stopped_at: None,
            restart_count: 0,
            error: None,
            detected_url: None,
        }
    }
}

/// A workspace script definition — the `script.create` result and the base of a
/// `script.list` entry (ported from the TS `WorkspaceScript`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Script {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
    pub mode: ScriptMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_start: Option<bool>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// Wire input for `script.create` (PROTOCOL §5.8); built by the router from
/// request params. `workspaceId` is passed separately.
#[derive(Debug, Clone, Default)]
pub struct ScriptCreateParams {
    pub name: String,
    pub command: String,
    pub mode: ScriptMode,
    pub cwd: Option<String>,
    pub env: Option<BTreeMap<String, String>>,
    pub category: Option<String>,
    pub auto_start: Option<bool>,
    pub script_id: Option<String>,
}

/// Logical client record (§9.2, §16). The stable, client-supplied identity that
/// survives reconnects; persisted to the `client` table with `name`,
/// `capabilities`, `first_seen`, and `last_seen`. The ephemeral per-connection
/// id is transport-only and never stored here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    pub id: ClientId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub capabilities: serde_json::Value,
    pub first_seen: String,
    pub last_seen: String,
}

/// Per-client chat draft (§9.10, §15), keyed by `(workspaceId, agentId,
/// clientId)` so concurrent clients never clobber one another. Persisted to the
/// `draft` table; an empty draft is represented by the row's absence (an empty
/// `drafts.set` clears it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Draft {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub client_id: ClientId,
    pub text: String,
    pub updated_at: String,
}

/// Long-term agent memory row (§9.2, §9.12, §18.5). Persisted to the `memories`
/// table and surfaced to agents internally (no `memories.*` RPC in v1); a `null`
/// `workspaceId` denotes a global memory. `search.memories` (§5.15) reads this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Memory {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<WorkspaceId>,
    pub content: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// A persistently-registered repository (parity with the TS `KnownRepo`). Backs
/// the `repo.list` method that populates the Create-Workspace picker. Timestamps
/// are ISO-8601 strings; `owner` is omitted from the wire when absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownRepo {
    /// Absolute path to the repository.
    pub path: String,
    /// Repository name (typically the folder name).
    pub name: String,
    /// GitHub organization or user who owns this repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// ISO timestamp of when this repo was first added.
    pub added_at: String,
    /// ISO timestamp of when this repo was last used (workspace created).
    pub last_used_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn event_wire_shape_matches_ts() {
        // Mirrors `WorkspaceEventBase` + `EventActor` in events/types.ts: the
        // discriminant is `type`, ids/timestamps are camelCase, and absent
        // optionals are omitted.
        let event = Event {
            id: "01900000-0000-7000-8000-000000000000".to_string(),
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: "file:changed".to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some("agent-7".to_string()),
                model: Some("opus".to_string()),
                ..Default::default()
            },
            session_id: Some("sess-1".to_string()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "path": "src/a.rs", "action": "modify" }),
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({
                "id": "01900000-0000-7000-8000-000000000000",
                "workspaceId": "ws-1",
                "timestamp": "2026-01-01T00:00:00Z",
                "type": "file:changed",
                "actor": { "type": "agent", "id": "agent-7", "model": "opus" },
                "sessionId": "sess-1",
                "data": { "path": "src/a.rs", "action": "modify" }
            })
        );
        // Round-trips back to an equal value.
        let back: Event = serde_json::from_value(serde_json::to_value(&event).unwrap()).unwrap();
        assert_eq!(back, event);
    }

    #[test]
    fn event_metadata_serializes_camel_case() {
        // Mirrors `WorkspaceEventBase.metadata` in events/types.ts: an optional
        // free-form object emitted under the camelCase `metadata` key.
        let event = Event {
            id: "01900000-0000-7000-8000-000000000001".to_string(),
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: "agent:message".to_string(),
            actor: EventActor::default(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: Some(json!({ "source": "test", "retryCount": 2 })),
            data: json!({}),
        };
        let wire = serde_json::to_value(&event).unwrap();
        assert_eq!(
            wire["metadata"],
            json!({ "source": "test", "retryCount": 2 })
        );
        let back: Event = serde_json::from_value(wire).unwrap();
        assert_eq!(back, event);
    }

    #[test]
    fn known_repo_wire_shape_matches_ts() {
        // Mirrors `KnownRepo` in src/shared/types/known-repo.ts: camelCase
        // `addedAt`/`lastUsedAt`, and `owner` omitted when absent.
        let with_owner = KnownRepo {
            path: "/Users/me/src/intent".to_string(),
            name: "intent".to_string(),
            owner: Some("cloudlands-ai".to_string()),
            added_at: "2026-01-01T00:00:00Z".to_string(),
            last_used_at: "2026-01-02T00:00:00Z".to_string(),
        };
        assert_eq!(
            serde_json::to_value(&with_owner).unwrap(),
            json!({
                "path": "/Users/me/src/intent",
                "name": "intent",
                "owner": "cloudlands-ai",
                "addedAt": "2026-01-01T00:00:00Z",
                "lastUsedAt": "2026-01-02T00:00:00Z"
            })
        );
        // `owner: None` omits the key entirely (no `null`).
        let no_owner = KnownRepo {
            owner: None,
            ..with_owner.clone()
        };
        let wire = serde_json::to_value(&no_owner).unwrap();
        assert!(wire.get("owner").is_none());
        assert_eq!(serde_json::from_value::<KnownRepo>(wire).unwrap(), no_owner);
    }

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

    /// A regular comment with `anchorContext` and no suggestion fields must
    /// serialize to the camelCase `CommentV2` wire shape (`type`, `authorType`,
    /// `createdAt`, nested `anchorContext`) the iOS client expects (§5.3).
    #[test]
    fn comment_wire_regular_camel_case_parity() {
        let comment = Comment {
            id: "c1".to_string(),
            thread_id: "t1".to_string(),
            note_id: Some(NoteId::from("note-1")),
            kind: CommentType::Comment,
            content: "hello".to_string(),
            author: "Agent".to_string(),
            author_type: AuthorType::Agent,
            status: CommentStatus::Open,
            parent_id: None,
            anchor: CommentAnchor {
                kind: CommentAnchorType::Range,
                start_id: Some("c1".to_string()),
                end_id: Some("c1".to_string()),
                point_id: None,
            },
            anchor_text: Some("Seed".to_string()),
            anchor_before: Some("be".to_string()),
            anchor_after: Some("af".to_string()),
            suggestion_original: None,
            suggestion_proposed: None,
            agent_id: None,
            created_at: "t0".to_string(),
            updated_at: "t0".to_string(),
        };
        let value = serde_json::to_value(CommentWire::from_comment(&comment)).unwrap();
        assert_eq!(
            value,
            json!({
                "id": "c1",
                "threadId": "t1",
                "noteId": "note-1",
                "type": "comment",
                "content": "hello",
                "author": "Agent",
                "authorType": "agent",
                "status": "open",
                "anchor": { "type": "range", "startId": "c1", "endId": "c1" },
                "anchorText": "Seed",
                "anchorContext": { "before": "be", "after": "af" },
                "createdAt": "t0",
                "updatedAt": "t0"
            })
        );
    }

    /// A suggestion comment nests `suggestionDiff` and omits `anchorContext`
    /// when no anchor context is present (matches `comment-loader.ts`).
    #[test]
    fn comment_wire_suggestion_nests_suggestion_diff() {
        let comment = Comment {
            id: "c2".to_string(),
            thread_id: "t1".to_string(),
            note_id: Some(NoteId::from("note-1")),
            kind: CommentType::Suggestion,
            content: "try this".to_string(),
            author: "Agent".to_string(),
            author_type: AuthorType::Agent,
            status: CommentStatus::Open,
            parent_id: Some("c1".to_string()),
            anchor: CommentAnchor::default(),
            anchor_text: None,
            anchor_before: None,
            anchor_after: None,
            suggestion_original: Some("Seed".to_string()),
            suggestion_proposed: Some("Sprout".to_string()),
            agent_id: None,
            created_at: "t0".to_string(),
            updated_at: "t0".to_string(),
        };
        let value = serde_json::to_value(CommentWire::from_comment(&comment)).unwrap();
        assert_eq!(value["type"], json!("suggestion"));
        assert_eq!(value["parentId"], json!("c1"));
        assert_eq!(
            value["suggestionDiff"],
            json!({ "original": "Seed", "proposed": "Sprout" })
        );
        assert!(value.get("anchorContext").is_none());
        assert!(value.get("suggestion_original").is_none());
    }

    /// `comment.add` echoes a camelCase `commentId` + nested `location`
    /// (`anchoredText`), matching the TS `ws.comment.add` return (§5.3).
    #[test]
    fn comment_add_result_camel_case_parity() {
        let result = CommentAddResult {
            success: true,
            message: "Comment successfully anchored to \"Seed\"".to_string(),
            comment_id: "c1".to_string(),
            anchored: true,
            location: CommentLocation {
                line: 1,
                anchored_text: "Seed".to_string(),
            },
        };
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({
                "success": true,
                "message": "Comment successfully anchored to \"Seed\"",
                "commentId": "c1",
                "anchored": true,
                "location": { "line": 1, "anchoredText": "Seed" }
            })
        );
    }

    /// `task.update` returns `lineNumber` (camelCase) + a checkbox status word.
    #[test]
    fn task_update_result_camel_case_parity() {
        let result = TaskUpdateResult {
            ok: true,
            note_id: NoteId::from("task-1"),
            line_number: 3,
            previous_text: "old".to_string(),
            new_text: "new".to_string(),
            status: "done".to_string(),
        };
        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({
                "ok": true,
                "noteId": "task-1",
                "lineNumber": 3,
                "previousText": "old",
                "newText": "new",
                "status": "done"
            })
        );
    }

    /// `AgentStatus` keeps the modern lowercase values and the legacy
    /// capitalized ones distinct, so persisted sessions round-trip unchanged
    /// (`agent.types.ts`).
    #[test]
    fn agent_status_wire_forms_match_ts() {
        for (variant, wire) in [
            (AgentStatus::Pending, "\"pending\""),
            (AgentStatus::Active, "\"active\""),
            (AgentStatus::RuntimeIdle, "\"idle\""),
            (AgentStatus::Error, "\"error\""),
            (AgentStatus::Deleted, "\"deleted\""),
            (AgentStatus::Idle, "\"Idle\""),
            (AgentStatus::Waiting, "\"Waiting\""),
            (AgentStatus::Completed, "\"Completed\""),
            (AgentStatus::Processing, "\"Processing\""),
        ] {
            assert_eq!(serde_json::to_string(&variant).unwrap(), wire);
            assert_eq!(serde_json::from_str::<AgentStatus>(wire).unwrap(), variant);
        }
    }

    /// `WorkspaceStatus` serializes to the PascalCase TS `WorkspaceStatus` string
    /// enum (`src/shared/types.ts`): `Active`/`Inactive`/`Archived`/`Deleted`.
    #[test]
    fn workspace_status_wire_forms_match_ts() {
        for (variant, wire) in [
            (WorkspaceStatus::Active, "\"Active\""),
            (WorkspaceStatus::Inactive, "\"Inactive\""),
            (WorkspaceStatus::Archived, "\"Archived\""),
            (WorkspaceStatus::Deleted, "\"Deleted\""),
        ] {
            assert_eq!(serde_json::to_string(&variant).unwrap(), wire);
            assert_eq!(
                serde_json::from_str::<WorkspaceStatus>(wire).unwrap(),
                variant
            );
        }
    }

    /// `Workspace` emits PascalCase `status` and omits absent optionals
    /// (`skip_serializing_if`) so the iOS decoder sees the documented field set
    /// without nulls.
    #[test]
    fn workspace_status_pascal_and_optionals_absent() {
        let ts = "2026-01-01T00:00:00Z".to_string();
        let ws = Workspace {
            id: WorkspaceId::from("ws-1"),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
        };
        let v = serde_json::to_value(&ws).unwrap();
        assert_eq!(v["status"], "Active");
        // Absent optionals are omitted, not serialized as null.
        for key in [
            "statusMessage",
            "baseRef",
            "prNumber",
            "prStatus",
            "activePullRequest",
            "repositoryOwner",
            "lastActivity",
            "archivedAt",
        ] {
            assert!(v.get(key).is_none(), "expected `{key}` to be omitted");
        }
        // Round-trips back with optionals defaulted to None.
        let back: Workspace = serde_json::from_value(v).unwrap();
        assert_eq!(back, ws);
    }

    /// The card aggregates serialize with the exact nested field names + casing
    /// the iOS `WorkspaceStore.parseWorkspace` reads (`taskStats.{total,
    /// completed,inProgress}`, `agentSummary.{count,agents[]}` with
    /// `WorkspaceAgentInfo`, `diffSummary.{totalFiles,...}`).
    #[test]
    fn workspace_card_aggregates_nested_wire_shape() {
        let ts = "2026-01-01T00:00:00Z".to_string();
        let agent = WorkspaceAgentInfo {
            id: AgentId::from("agent-1"),
            name: "Builder".to_string(),
            status: AgentStatus::Active,
            specialist: Some("implementor".to_string()),
            last_activity: Some(ts.clone()),
            is_streaming: false,
            is_responding: false,
        };
        let summary = WorkspaceAgentSummary {
            count: 1,
            agents: vec![agent],
            agent_ids: vec![AgentId::from("agent-1")],
        };
        let task_stats = WorkspaceTaskStats {
            total: 3,
            completed: 1,
            in_progress: 1,
        };
        let diff = WorkspaceDiffSummary {
            schema_version: 1,
            updated_at: ts.clone(),
            total_files: 2,
            total_additions: 10,
            total_deletions: 4,
            files: vec![],
        };
        let v = serde_json::to_value(&task_stats).unwrap();
        assert_eq!(v["total"], 3);
        assert_eq!(v["completed"], 1);
        assert_eq!(v["inProgress"], 1);

        let v = serde_json::to_value(&summary).unwrap();
        assert_eq!(v["count"], 1);
        assert_eq!(v["agents"][0]["id"], "agent-1");
        assert_eq!(v["agents"][0]["name"], "Builder");
        assert_eq!(v["agents"][0]["status"], "active");
        assert_eq!(v["agents"][0]["specialist"], "implementor");
        assert_eq!(v["agents"][0]["lastActivity"], ts);
        assert_eq!(v["agents"][0]["isStreaming"], false);
        assert_eq!(v["agents"][0]["isResponding"], false);
        // `agentIds` is emitted alongside `agents` (forward-compat TS parity).
        assert_eq!(v["agentIds"][0], "agent-1");
        assert_eq!(v["agentIds"].as_array().unwrap().len(), 1);

        let v = serde_json::to_value(&diff).unwrap();
        assert_eq!(v["schemaVersion"], 1);
        assert_eq!(v["totalFiles"], 2);
        assert_eq!(v["totalAdditions"], 10);
        assert_eq!(v["totalDeletions"], 4);
        assert!(v["files"].is_array());
    }

    /// `WorkspaceDiffSummaryFile` matches the TS per-file wire shape.
    #[test]
    fn workspace_diff_summary_file_wire_shape() {
        let f = WorkspaceDiffSummaryFile {
            path: "src/main.rs".to_string(),
            action: "modify".to_string(),
            additions: 3,
            deletions: 1,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["path"], "src/main.rs");
        assert_eq!(v["action"], "modify");
        assert_eq!(v["additions"], 3);
        assert_eq!(v["deletions"], 1);
    }

    /// `WorkspaceAgentInfo` omits the optional `specialist`/`lastActivity` keys
    /// when absent (not `null`), while the non-optional flags stay present.
    #[test]
    fn workspace_agent_info_optionals_absent() {
        let agent = WorkspaceAgentInfo {
            id: AgentId::from("agent-2"),
            name: "Plain".to_string(),
            status: AgentStatus::Pending,
            specialist: None,
            last_activity: None,
            is_streaming: false,
            is_responding: false,
        };
        let v = serde_json::to_value(&agent).unwrap();
        assert!(v.get("specialist").is_none());
        assert!(v.get("lastActivity").is_none());
        assert_eq!(v["status"], "pending");
        assert_eq!(v["isStreaming"], false);
        assert_eq!(v["isResponding"], false);
    }

    /// `AgentLite` carries the nested `metadata` object (`isBackground`/
    /// `specialist`/`createdByAgentId`/`taskNoteId`) and the activity flags the
    /// iOS `AgentSession.parseAgent` reads.
    #[test]
    fn agent_lite_metadata_and_activity_wire_shape() {
        let ts = "t1".to_string();
        let session = AgentSession {
            id: AgentId::from("agent-1"),
            workspace_id: WorkspaceId::from("ws-1"),
            parent_agent_id: Some(AgentId::from("agent-parent")),
            backend_session_id: None,
            acp_session_id: None,
            name: "Builder".to_string(),
            name_explicitly_set: true,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: Some("implementor".to_string()),
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            created_at: "t0".to_string(),
            updated_at: ts.clone(),
        };
        let lite = AgentLite::from_session(session, 0, None, Some("hi".to_string()), None);
        let v = serde_json::to_value(&lite).unwrap();
        assert_eq!(v["metadata"]["specialist"], "implementor");
        assert_eq!(v["metadata"]["isBackground"], false);
        assert_eq!(v["metadata"]["createdByAgentId"], "agent-parent");
        assert_eq!(v["isStreaming"], false);
        assert_eq!(v["isProcessing"], false);
        assert_eq!(v["isResponding"], false);
        assert_eq!(v["lastUserMessage"], "hi");
        assert_eq!(v["lastActivity"], "t1");
    }

    /// `AgentSession` serializes to the camelCase `agent-session.ts` wire shape:
    /// `backendSessionId`/`acpSessionId`/`nameExplicitlySet`/`isActive`/
    /// `systemPrompt`, with absent optionals omitted and a nested message log.
    #[test]
    fn agent_session_camel_case_parity() {
        let session = AgentSession {
            id: AgentId::from("agent-1"),
            workspace_id: WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: Some(AgentId::from("backend-9")),
            acp_session_id: Some("acp-uuid".to_string()),
            name: "Builder".to_string(),
            name_explicitly_set: true,
            model: Some("opus".to_string()),
            provider: Some("auggie".to_string()),
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![AgentMessage {
                id: "msg-1".to_string(),
                agent_id: AgentId::from("agent-1"),
                seq: 0,
                role: "user".to_string(),
                content: json!([{ "type": "text", "text": "hi" }]),
                created_at: "t0".to_string(),
            }],
            stats: None,
            created_at: "t0".to_string(),
            updated_at: "t1".to_string(),
        };
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            json!({
                "id": "agent-1",
                "workspaceId": "ws-1",
                "backendSessionId": "backend-9",
                "acpSessionId": "acp-uuid",
                "name": "Builder",
                "nameExplicitlySet": true,
                "model": "opus",
                "provider": "auggie",
                "status": "active",
                "isActive": true,
                "messages": [{
                    "id": "msg-1",
                    "agentId": "agent-1",
                    "seq": 0,
                    "role": "user",
                    "contentBlocks": [{ "type": "text", "text": "hi" }],
                    "timestamp": "t0"
                }],
                "createdAt": "t0",
                "updatedAt": "t1"
            })
        );
        let back: AgentSession =
            serde_json::from_value(serde_json::to_value(&session).unwrap()).unwrap();
        assert_eq!(back, session);
    }

    /// `AgentMessage` serializes its block array and timestamp under the TS wire
    /// names `contentBlocks`/`timestamp` (`agent-message.ts`) — never `content`/
    /// `createdAt` — so the iOS conversation view renders populated bubbles.
    #[test]
    fn agent_message_content_blocks_timestamp_wire_parity() {
        let message = AgentMessage {
            id: "msg-1".to_string(),
            agent_id: AgentId::from("agent-1"),
            seq: 0,
            role: "user".to_string(),
            content: json!([{ "type": "text", "text": "hi" }]),
            created_at: "t0".to_string(),
        };
        let value = serde_json::to_value(&message).unwrap();
        assert_eq!(
            value,
            json!({
                "id": "msg-1",
                "agentId": "agent-1",
                "seq": 0,
                "role": "user",
                "contentBlocks": [{ "type": "text", "text": "hi" }],
                "timestamp": "t0"
            })
        );
        let obj = value.as_object().unwrap();
        assert!(obj.contains_key("contentBlocks"));
        assert!(obj.contains_key("timestamp"));
        assert!(!obj.contains_key("content"));
        assert!(!obj.contains_key("createdAt"));
        let back: AgentMessage = serde_json::from_value(value).unwrap();
        assert_eq!(back, message);
    }
}
