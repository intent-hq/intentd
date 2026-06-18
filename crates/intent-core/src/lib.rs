//! intent-core — domain vocabulary for intentd.
//!
//! Leaf crate: it depends on no other workspace crate (§3.2 rule 1). It defines
//! entity ids, the error type, configuration, the wire-facing domain structs,
//! and the cross-layer traits (`WorkspaceApi`, `ContextEngine`) that higher
//! layers implement and consume.

pub mod clock;
pub mod config;
pub mod error;
pub mod ids;
pub mod model;
pub mod traits;

pub mod events {
    //! Canonical event-bus message types (§16 subscription model).

    /// Domain event placeholder. Variants are added as features land.
    #[derive(Debug, Clone)]
    #[non_exhaustive]
    pub enum Event {}
}

pub use clock::now_iso;
pub use config::Config;
pub use error::{Error, Result};
pub use events::Event;
pub use ids::{AgentId, NoteId, WorkspaceId};
pub use model::{
    AuthorType, Comment, CommentAnchor, CommentAnchorType, CommentStatus, CommentThread,
    CommentType, ContentType, Note, NoteAddInput, NoteAddResult, NoteCreate, NoteDeleteResult,
    NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult, NoteSetContentResult,
    NoteTaskRow, NoteUpdateInput, NoteUpdateMetadataResult, NoteVisibility, ReadAssetResult,
    TaskMetadata, TaskStatus, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceCreate,
    WorkspaceStatus, WorkspaceUpdate,
};
pub use traits::{BoxFuture, ContextEngine, WorkspaceApi};
