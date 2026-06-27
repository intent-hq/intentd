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
pub mod traits;

pub use clock::{iso_from_unix_secs, iso_minutes_ago, now_iso, parse_iso};
pub use config::Config;
pub use error::{Error, Result};
pub use events::is_known_event_type;
pub use ids::{AgentId, ClientId, NoteId, WorkspaceId};
pub use model::{
    ActorType, AgentActivity, AgentDelegateInput, AgentLite, AgentMessage, AgentMetadata,
    AgentSession, AgentStatus, AnchorContext, AuthorType, Client, Comment, CommentAddResult,
    CommentAnchor, CommentAnchorType, CommentDeleteResult, CommentGetThreadResult,
    CommentListResult, CommentLocation, CommentResolveThreadResult, CommentRespondResult,
    CommentRespondThread, CommentStatus, CommentThread, CommentThreadSummary, CommentType,
    CommentWire, ContentType, Draft, Event, EventActor, EventQueryParams, EventSubscribeResult,
    EventUnsubscribeResult, FileActivity, FileStatus, GitAgentCommitResult, GitBranches,
    GitCommitResult, GitFileStatus, GitMergeConflicts, GitStatus, KnownRepo, Memory, Note,
    NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult, NoteEditInput, NoteEditLinesInput,
    NoteEditLinesResult, NoteEditResult, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVisibility, PullRequestInfo, PullRequestStatus, ReadAssetResult,
    Script, ScriptCreateParams, ScriptMode, ScriptRuntimeState, ScriptStatus, SessionStats,
    SuggestionDiff, TaskAssignAgentResult, TaskConvertBlocksResult, TaskCreatePrerequisiteResult,
    TaskGetMyTaskResult, TaskMarkAsTaskResult, TaskMetadata, TaskStatus, TaskSubtask,
    TaskUpdateNoteStatusResult, TaskUpdateResult, TaskUpdateStatusResult, TopChangedFile,
    Workspace, WorkspaceActivity, WorkspaceAgentInfo, WorkspaceAgentSummary, WorkspaceAttention,
    WorkspaceCreate, WorkspaceDiffSummary, WorkspaceDiffSummaryFile, WorkspaceEventSummary,
    WorkspaceStatus, WorkspaceTask, WorkspaceTaskStats, WorkspaceUpdate,
};
pub use traits::{
    BoxFuture, ContextEngine, ContextError, EngineAvailability, RetrieveRequest, RetrieveResult,
    RetrievedItem, WorkspaceApi,
};
