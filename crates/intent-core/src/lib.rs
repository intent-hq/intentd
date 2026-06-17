//! intent-core — domain vocabulary for intentd.
//!
//! Leaf crate: it depends on no other workspace crate (§3.2 rule 1). It defines
//! entity ids, the error type, configuration, event types, and the cross-layer
//! traits (`WorkspaceApi`, `ContextEngine`) that higher layers implement and
//! consume. Stub only — no behavior yet.

pub mod ids {
    //! Strongly-typed entity identifiers (§9.5).

    /// Identifier for a workspace.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct WorkspaceId(pub String);

    /// Identifier for a note.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct NoteId(pub String);

    /// Identifier for an agent.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct AgentId(pub String);
}

pub mod error {
    //! Typed domain errors that map to JSON-RPC error codes (§11).

    /// Domain error type. Variants are added as features land.
    #[derive(Debug, thiserror::Error)]
    pub enum Error {
        /// Functionality is not yet implemented (bootstrap skeleton).
        #[error("not implemented")]
        NotImplemented,
    }

    /// Convenience result alias used across the workspace.
    pub type Result<T> = std::result::Result<T, Error>;
}

pub mod config {
    //! Daemon configuration and path resolution (§11.2).

    /// Daemon configuration. Fields are added as features land.
    #[derive(Debug, Clone, Default)]
    pub struct Config;
}

pub mod events {
    //! Canonical event-bus message types (§16 subscription model).

    /// Domain event placeholder. Variants are added as features land.
    #[derive(Debug, Clone)]
    #[non_exhaustive]
    pub enum Event {}
}

pub mod traits {
    //! Cross-layer traits implemented by higher crates.

    /// Business-logic surface that `intent-acp` calls back into, breaking the
    /// `services → acp → services` cycle. Implemented in `intent-services` and
    /// handed to the ACP client as an `Arc<dyn WorkspaceApi>` (§3.2 rule 3,
    /// §6.8).
    pub trait WorkspaceApi: Send + Sync {}

    /// Context-engine abstraction implemented by `intent-context` (§3.1).
    pub trait ContextEngine: Send + Sync {}
}

pub use config::Config;
pub use error::{Error, Result};
pub use events::Event;
pub use ids::{AgentId, NoteId, WorkspaceId};
pub use traits::{ContextEngine, WorkspaceApi};
