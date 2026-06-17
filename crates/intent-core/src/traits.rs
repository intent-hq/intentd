//! Cross-layer traits implemented by higher crates (§3.2, §6.8).

use std::future::Future;
use std::pin::Pin;

use crate::error::{Error, Result};
use crate::ids::WorkspaceId;
use crate::model::{Note, Workspace};

/// Boxed, `Send` future — keeps [`WorkspaceApi`] object-safe so it can be held
/// as `Arc<dyn WorkspaceApi>` (the agent→BE callback handle, §6.8).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Business-logic read surface that `intent-acp` calls back into and the
/// transport router dispatches to. Defined here in the leaf crate; the real,
/// store-backed implementation lives in `intent-services` (§3.2 rule 3). The
/// default bodies return an internal error so downstream stubs compile until
/// they override these methods.
pub trait WorkspaceApi: Send + Sync {
    /// List workspaces, optionally including archived ones (PROTOCOL §5.1).
    fn list_workspaces(&self, include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        let _ = include_archived;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_workspaces not implemented".to_string(),
            ))
        })
    }

    /// List notes in a workspace (PROTOCOL §5.2).
    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let _ = workspace_id;
        Box::pin(async {
            Err(Error::Internal(
                "WorkspaceApi::list_notes not implemented".to_string(),
            ))
        })
    }
}

/// Context-engine abstraction implemented by `intent-context` (§3.1).
pub trait ContextEngine: Send + Sync {}
