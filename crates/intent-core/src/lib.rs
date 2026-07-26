//! intent-core — domain vocabulary for intentd.
//!
//! Leaf crate: it depends on no other workspace crate (§3.2 rule 1). It defines
//! entity ids, the error type, configuration, the wire-facing domain structs,
//! and the cross-layer traits (`WorkspaceApi`, `ContextEngine`) that higher
//! layers implement and consume.

pub mod agent_logs;
pub mod chief_cwd;
pub mod clock;
pub mod config;
pub mod error;
pub mod events;
pub mod ids;
pub mod model;
pub mod path_utils;
pub mod secrets;
pub mod server_control;
pub mod settings_file;
pub mod slug;
pub mod tilde;
pub mod traits;
pub mod turn_attachments;

pub use agent_logs::{
    agent_logs_root, create_agent_log_dir, current_agent_log_file_name, open_agent_log_file,
    sweep_agent_logs, AGENT_LOGS_DIR_NAME, AGENT_LOG_RETENTION_DAYS,
};
pub use chief_cwd::{chief_cwd_root, create_chief_cwd_dir, sweep_chief_cwd, CHIEF_CWD_DIR_NAME};
pub use clock::{iso_from_unix_secs, iso_minutes_ago, now_epoch_ms, now_iso, parse_iso};
pub use config::Config;
pub use error::{Error, Result};
pub use events::is_known_event_type;
pub use ids::{AgentId, ClientId, NoteId, WorkspaceId, CHIEF_WORKSPACE_ID};
pub use model::MAX_DELEGATION_DEPTH;
pub use model::WORKSPACE_STATUS_MESSAGE_MAX_LENGTH;
pub use model::{chief_workspace, is_chief_workspace, CHIEF_WORKSPACE_TIMESTAMP};
pub use model::{lift_app_message_id, USER_APP_MESSAGE_ID_KEY};
pub use model::{
    ActorType, AgentActivity, AgentCreateExtra, AgentDelegateInput, AgentLite, AgentMessage,
    AgentMetadata, AgentSession, AgentStatus, AgentWakeCreateOptions, AgentWakeOrCreateInput,
    AnchorContext, AuthorType, CheckoutMode, Client, Comment, CommentAddResult, CommentAnchor,
    CommentAnchorType, CommentDeleteResult, CommentGetThreadResult, CommentListResult,
    CommentLocation, CommentResolveThreadResult, CommentRespondResult, CommentRespondThread,
    CommentStatus, CommentThread, CommentThreadSummary, CommentType, CommentWire, ContentType,
    ContextItem, Draft, Event, EventActor, EventQueryParams, EventSubscribeResult,
    EventUnsubscribeResult, FileActivity, FileStatus, GitAgentCommitResult, GitBranchStatus,
    GitBranches, GitCommitResult, GitFileStatus, GitMergeConflicts, GitPullResult, GitStatus,
    KnownRepo, LineAttributionAuthor, LineAttributionComputeResult, LineAttributionData,
    LineAttributionInfo, Note, NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult,
    NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult, NoteMetadata,
    NoteRestoreVersionResult, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVersion, NoteVersionAuthor, NoteVersionSummary, NoteVisibility,
    ProjectType, PullRequestInfo, PullRequestStatus, ReadAssetResult, RepoConfig, RepoScript,
    RepoScriptCategory, RepoScriptMode, SaveAssetResult, Script, ScriptCreateParams, ScriptMode,
    ScriptRuntimeState, ScriptStatus, SessionStats, SetupScript, SetupScriptGeneratedBy,
    SuggestionDiff, TaskAgentLink, TaskAssignAgentResult, TaskConvertBlocksResult,
    TaskCreatePrerequisiteResult, TaskGetMyTaskResult, TaskListResult, TaskMarkAsTaskResult,
    TaskMetadata, TaskRemoveAgentFromAllTasksResult, TaskStatus, TaskSubtask,
    TaskUpdateNoteStatusResult, TaskUpdateResult, TaskUpdateStatusResult, TokenUsage,
    TokenUsageTotals, TopChangedFile, Workspace, WorkspaceActivity, WorkspaceAgentInfo,
    WorkspaceAgentSummary, WorkspaceAttention, WorkspaceCreate, WorkspaceCreateInitialAgent,
    WorkspaceCreateResult, WorkspaceDiffSummary, WorkspaceDiffSummaryFile, WorkspaceEventSummary,
    WorkspaceStatus, WorkspaceTask, WorkspaceTaskStats, WorkspaceUpdate,
};
pub use secrets::{default_secrets_path, FileSecretStore, SECRETS_FILE_ENV};
pub use server_control::ServerControl;
pub use settings_file::{
    LegacySettings, SettingsFile, DEFAULT_CONFIG_TEMPLATE, LEGACY_SETTINGS_PATHS,
};
pub use tilde::{expand_tilde, expand_tilde_string, expand_tilde_with};
pub use traits::{
    AgentReverseDispatch, BoxFuture, ContextEngine, ContextError, EngineAvailability, PublishEvent,
    RetrieveRequest, RetrieveResult, RetrievedItem, ReverseDispatchError, WorkspaceApi,
};
pub use turn_attachments::{
    new_attachment_id, AttachmentPolicy, TurnAttachment, TurnAttachmentRegistry, ATTACHMENT_ID_KEY,
};
