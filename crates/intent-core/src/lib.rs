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

pub use clock::{now_iso, parse_iso};
pub use config::Config;
pub use error::{Error, Result};
pub use events::is_known_event_type;
pub use ids::{AgentId, NoteId, WorkspaceId};
pub use model::{
    ActorType, AnchorContext, AuthorType, Comment, CommentAddResult, CommentAnchor,
    CommentAnchorType, CommentDeleteResult, CommentGetThreadResult, CommentListResult,
    CommentLocation, CommentRespondResult, CommentRespondThread, CommentStatus, CommentThread,
    CommentThreadSummary, CommentType, CommentWire, ContentType, Event, EventActor, Note,
    NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult, NoteEditInput, NoteEditLinesInput,
    NoteEditLinesResult, NoteEditResult, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVisibility, ReadAssetResult, SuggestionDiff,
    TaskAssignAgentResult, TaskConvertBlocksResult, TaskCreatePrerequisiteResult,
    TaskGetMyTaskResult, TaskMarkAsTaskResult, TaskMetadata, TaskStatus, TaskSubtask,
    TaskUpdateNoteStatusResult, TaskUpdateResult, TaskUpdateStatusResult, Workspace,
    WorkspaceActivity, WorkspaceAttention, WorkspaceCreate, WorkspaceStatus, WorkspaceUpdate,
};
pub use traits::{BoxFuture, ContextEngine, WorkspaceApi};
