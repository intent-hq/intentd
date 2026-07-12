//! intent-core — domain vocabulary for intentd.
//!
//! Leaf crate: it depends on no other workspace crate (§3.2 rule 1). It defines
//! entity ids, the error type, configuration, the wire-facing domain structs,
//! and the cross-layer traits (`WorkspaceApi`, `ContextEngine`) that higher
//! layers implement and consume.

pub mod clock;
pub mod config;
pub mod error;
pub mod events;
pub mod ids;
pub mod model;
pub mod secrets;
pub mod slug;
pub mod traits;

pub use clock::{iso_from_unix_secs, iso_minutes_ago, now_epoch_ms, now_iso, parse_iso};
pub use config::Config;
pub use error::{Error, Result};
pub use events::is_known_event_type;
pub use ids::{AgentId, ClientId, NoteId, WorkspaceId, CHIEF_WORKSPACE_ID};
pub use model::MAX_DELEGATION_DEPTH;
pub use model::WORKSPACE_STATUS_MESSAGE_MAX_LENGTH;
pub use model::{chief_workspace, is_chief_workspace, CHIEF_WORKSPACE_TIMESTAMP};
pub use model::{
    ActorType, AgentActivity, AgentCreateExtra, AgentDelegateInput, AgentLite, AgentMessage,
    AgentMetadata, AgentSession, AgentStatus, AgentWakeCreateOptions, AgentWakeOrCreateInput,
    AnchorContext, AuthorType, Client, Comment, CommentAddResult, CommentAnchor, CommentAnchorType,
    CommentDeleteResult, CommentGetThreadResult, CommentListResult, CommentLocation,
    CommentResolveThreadResult, CommentRespondResult, CommentRespondThread, CommentStatus,
    CommentThread, CommentThreadSummary, CommentType, CommentWire, ContentType, Draft, Event,
    EventActor, EventQueryParams, EventSubscribeResult, EventUnsubscribeResult, FileActivity,
    FileStatus, GitAgentCommitResult, GitBranchStatus, GitBranches, GitCommitResult, GitFileStatus,
    GitMergeConflicts, GitPullResult, GitStatus, KnownRepo, LineAttributionAuthor,
    LineAttributionComputeResult, LineAttributionData, LineAttributionInfo, Memory, Note,
    NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult, NoteEditInput, NoteEditLinesInput,
    NoteEditLinesResult, NoteEditResult, NoteMetadata, NoteRestoreVersionResult,
    NoteSetContentResult, NoteTaskRow, NoteUpdateInput, NoteUpdateMetadataResult, NoteVersion,
    NoteVersionAuthor, NoteVersionSummary, NoteVisibility, ProjectType, PullRequestInfo,
    PullRequestStatus, ReadAssetResult, SaveAssetResult, Script, ScriptCreateParams, ScriptMode,
    ScriptRuntimeState, ScriptStatus, SessionStats, SetupScript, SetupScriptGeneratedBy,
    SuggestionDiff, TaskAssignAgentResult, TaskConvertBlocksResult, TaskCreatePrerequisiteResult,
    TaskGetMyTaskResult, TaskListResult, TaskMarkAsTaskResult, TaskMetadata,
    TaskRemoveAgentFromAllTasksResult, TaskStatus, TaskSubtask, TaskUpdateNoteStatusResult,
    TaskUpdateResult, TaskUpdateStatusResult, TokenUsage, TokenUsageTotals, TopChangedFile,
    Workspace, WorkspaceActivity, WorkspaceAgentInfo, WorkspaceAgentSummary, WorkspaceAttention,
    WorkspaceCreate, WorkspaceCreateInitialAgent, WorkspaceCreateResult, WorkspaceDiffSummary,
    WorkspaceDiffSummaryFile, WorkspaceEventSummary, WorkspaceStatus, WorkspaceTask,
    WorkspaceTaskStats, WorkspaceUpdate,
};
pub use secrets::{default_secrets_path, FileSecretStore, SECRETS_FILE_ENV};
pub use traits::{
    AgentReverseDispatch, BoxFuture, ContextEngine, ContextError, EngineAvailability,
    RetrieveRequest, RetrieveResult, RetrievedItem, ReverseDispatchError, WorkspaceApi,
};
