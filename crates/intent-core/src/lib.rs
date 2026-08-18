//! intent-core — domain vocabulary for intentd.
//!
//! Leaf crate: it depends on no other workspace crate (§3.2 rule 1). It defines
//! entity ids, the error type, configuration, the wire-facing domain structs,
//! and the cross-layer traits (`WorkspaceApi`, `ContextEngine`) that higher
//! layers implement and consume.

/// Test-binary-wide guard: export `NODE_DISABLE_COMPILE_CACHE=1` before any
/// test runs. `path_utils::login_shell_dirs()` spawns the user's interactive
/// login shell, whose rc files may run node CLIs (nvm/npm, ng completion);
/// those inherit this and skip `module.enableCompileCache()`, which would
/// otherwise leave a `node-compile-cache/` residue at the TMPDIR root.
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn disable_node_compile_cache() {
    std::env::set_var("NODE_DISABLE_COMPILE_CACHE", "1");
}

pub mod agent_configs;
pub mod agent_logs;
pub mod chief_cwd;
pub mod clock;
pub mod config;
pub mod discovery_cache;
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
pub mod transfer;
pub mod turn_attachments;

pub use agent_configs::{
    agent_configs_root, create_agent_configs_dir, sweep_agent_configs, AGENT_CONFIGS_DIR_NAME,
};
pub use agent_logs::{
    agent_logs_root, create_agent_log_dir, current_agent_log_file_name, open_agent_log_file,
    sweep_agent_logs, AGENT_LOGS_DIR_NAME, AGENT_LOG_RETENTION_DAYS,
};
pub use chief_cwd::{chief_cwd_root, create_chief_cwd_dir, sweep_chief_cwd, CHIEF_CWD_DIR_NAME};
pub use clock::{
    iso_from_unix_secs, iso_minutes_ago, iso_ms_from_now, now_epoch_ms, now_iso, parse_iso,
};
pub use config::Config;
pub use discovery_cache::DiscoveryCache;
pub use error::{CloneErrorCategory, Error, Result};
pub use events::is_known_event_type;
pub use ids::{
    AgentId, ClientId, HookId, NoteId, PrMonitorId, WorkspaceGitRootId, WorkspaceId,
    CHIEF_WORKSPACE_ID,
};
pub use model::extract_spec_task_ids;
pub use model::token_usage_reported;
pub use model::MessageOrigin;
pub use model::CURRENT_HARNESS_VERSION;
pub use model::DISMISSED_QUESTIONS_MESSAGE_ID_KEY;
pub use model::LAST_SEEN_MESSAGE_ID_KEY;
pub use model::MAX_DELEGATION_DEPTH;
pub use model::PENDING_QUESTIONS_MESSAGE_ID_KEY;
pub use model::WORKSPACE_STATUS_MESSAGE_MAX_LENGTH;
pub use model::{
    cap_json_value, last_tool_use_preview, slim_body_size, ConversationProjection,
    SLIM_PROJECTION_BUDGET_BYTES,
};
pub use model::{chief_workspace, is_chief_workspace, CHIEF_WORKSPACE_TIMESTAMP};
pub use model::{lift_app_message_id, USER_APP_MESSAGE_ID_KEY};
pub use model::{
    ActorType, AgentActivity, AgentCreateExtra, AgentDelegateInput, AgentLite, AgentMessage,
    AgentMetadata, AgentSession, AgentStatus, AgentWakeCreateOptions, AgentWakeOrCreateInput,
    AnchorContext, AuthorType, BatchTaskEntry, BatchTaskOptions, CheckoutMode, Client, Comment,
    CommentAddResult, CommentAnchor, CommentAnchorType, CommentDeleteResult,
    CommentGetThreadResult, CommentListResult, CommentLocation, CommentResolveThreadResult,
    CommentRespondResult, CommentRespondThread, CommentStatus, CommentThread, CommentThreadSummary,
    CommentType, CommentWire, ContentType, ContextItem, CreatedTaskEntry, DiskUsageBreakdownEntry,
    Draft, Event, EventActor, EventQueryParams, EventSubscribeResult, EventUnsubscribeResult,
    FileActivity, FileStatus, GitAgentCommitResult, GitBranchStatus, GitBranches, GitCommitResult,
    GitFileStatus, GitMergeConflicts, GitPullResult, GitStatus, Hook, HookState, KnownRepo,
    LineAttributionAuthor, LineAttributionComputeResult, LineAttributionData, LineAttributionInfo,
    Note, NoteAddInput, NoteAddResult, NoteCreate, NoteCreateResult, NoteDeleteResult,
    NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult, NoteMetadata,
    NoteRestoreVersionResult, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVersion, NoteVersionAuthor, NoteVersionSummary, NoteVisibility,
    PrMonitor, PrMonitorState, ProjectType, PullRequestInfo, PullRequestStatus, ReadAssetResult,
    RepoConfig, RepoScript, RepoScriptCategory, RepoScriptMode, SaveAssetResult, Script,
    ScriptCreateParams, ScriptMode, ScriptRuntimeState, ScriptStatus, SessionStats, SetupScript,
    SetupScriptGeneratedBy, SuggestionDiff, TaskAgentLink, TaskAssignAgentResult,
    TaskConvertBlocksResult, TaskCreatePrerequisiteResult, TaskGetMyTaskResult, TaskListResult,
    TaskMarkAsTaskResult, TaskMetadata, TaskRemoveAgentFromAllTasksResult, TaskSetRelationsResult,
    TaskStatus, TaskSubtask, TaskUpdateNoteStatusResult, TaskUpdateResult, TaskUpdateStatusResult,
    TokenUsage, TokenUsageTotals, TopChangedFile, UsageCost, Workspace, WorkspaceActivity,
    WorkspaceAgentInfo, WorkspaceAgentSummary, WorkspaceAttention, WorkspaceCreate,
    WorkspaceCreateInitialAgent, WorkspaceCreateResult, WorkspaceDiffSummary,
    WorkspaceDiffSummaryFile, WorkspaceDiskUsage, WorkspaceDisplayStatus, WorkspaceEventSummary,
    WorkspaceGitRoot, WorkspaceGitRootSource, WorkspaceStatus, WorkspaceTask, WorkspaceTaskStats,
    WorkspaceUpdate,
};
pub use path_utils::prewarm_login_shell_path;
pub use secrets::{default_secrets_path, FileSecretStore, SECRETS_FILE_ENV};
pub use server_control::ServerControl;
pub use settings_file::{
    FlushQueuedMessagesMode, LegacySettings, SettingsFile, DEFAULT_CONFIG_TEMPLATE,
    LEGACY_SETTINGS_PATHS,
};
pub use tilde::{expand_tilde, expand_tilde_string, expand_tilde_with};
pub use traits::{
    AgentReverseDispatch, BoxFuture, ContextEngine, ContextError, EngineAvailability, PublishEvent,
    RetrieveRequest, RetrieveResult, RetrievedItem, ReverseDispatchError, WorkspaceApi,
};
pub use turn_attachments::{
    new_attachment_id, AttachmentPolicy, TurnAttachment, TurnAttachmentRegistry, ATTACHMENT_ID_KEY,
};
