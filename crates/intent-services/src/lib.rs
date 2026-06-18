//! intent-services — the shared business-logic surface (§3.1).
//!
//! Depends on core, store, git, sourcecontrol, acp, context, providers, pty,
//! and search (§3.2). Sibling feature modules never import each other; they
//! communicate through the store and the event bus (§3.2 rule 4). This slice
//! implements the read-only `WorkspaceApi` surface (`workspace.list` /
//! `note.list`) over `intent-store`.

use intent_core::{
    now_iso, BoxFuture, Note, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceCreate,
    WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
};
use intent_store::Store;

pub use intent_core::{Error, Result, WorkspaceApi};

/// Aggregate service handle wired by the binary composition root. It implements
/// `WorkspaceApi` so it can be handed to `intent-acp` as `Arc<dyn WorkspaceApi>`
/// (§6.8) and dispatched to by the transport router.
#[derive(Clone)]
pub struct Services {
    store: Store,
}

impl Services {
    /// Wire the services surface over a persistence handle.
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Borrow the underlying store (composition-root / diagnostics use).
    pub fn store(&self) -> &Store {
        &self.store
    }
}

impl WorkspaceApi for Services {
    fn list_workspaces(&self, include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        let store = self.store.clone();
        Box::pin(async move { store.list_workspaces(include_archived).await })
    }

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move { store.get_workspace(&id).await })
    }

    fn create_workspace(&self, input: WorkspaceCreate) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let now = now_iso();
            let id = WorkspaceId::new();
            // The branch defaults to the workspace id, mirroring the TS service.
            let branch = input.branch.unwrap_or_else(|| id.0.clone());
            let ws = Workspace {
                id,
                title: input.title.unwrap_or_default(),
                branch,
                base_ref: input.base_ref,
                base_commit_sha: input.base_commit_sha,
                status: WorkspaceStatus::Active,
                status_message: input.status_message,
                // Derived, read-only; never persisted (§9.9).
                activity: WorkspaceActivity::Idle,
                attention: WorkspaceAttention::None,
                created_at: now.clone(),
                updated_at: now,
                last_activity: None,
                tags: input.tags.unwrap_or_default(),
                path: input.path,
                repository_owner: input.repository_owner,
                repository_name: input.repository_name,
                worktree_path: input.worktree_path,
                scope: input.scope,
                skip_worktree: input.skip_worktree.unwrap_or(false),
                setup_script: input.setup_script,
                is_remote: input.is_remote.unwrap_or(false),
                default_model: input.default_model,
                pr_number: None,
                pr_url: None,
                archived: false,
                archived_at: None,
            };
            store.insert_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            if let Some(v) = update.title {
                ws.title = v;
            }
            if let Some(v) = update.status_message {
                ws.status_message = Some(v);
            }
            if let Some(v) = update.branch {
                ws.branch = v;
            }
            if let Some(v) = update.base_ref {
                ws.base_ref = Some(v);
            }
            if let Some(v) = update.base_commit_sha {
                ws.base_commit_sha = Some(v);
            }
            if let Some(v) = update.status {
                ws.status = v;
            }
            if let Some(v) = update.tags {
                ws.tags = v;
            }
            if let Some(v) = update.path {
                ws.path = Some(v);
            }
            if let Some(v) = update.repository_owner {
                ws.repository_owner = Some(v);
            }
            if let Some(v) = update.repository_name {
                ws.repository_name = Some(v);
            }
            if let Some(v) = update.worktree_path {
                ws.worktree_path = Some(v);
            }
            if let Some(v) = update.scope {
                ws.scope = Some(v);
            }
            if let Some(v) = update.skip_worktree {
                ws.skip_worktree = v;
            }
            if let Some(v) = update.setup_script {
                ws.setup_script = Some(v);
            }
            if let Some(v) = update.is_remote {
                ws.is_remote = v;
            }
            if let Some(v) = update.default_model {
                ws.default_model = Some(v);
            }
            if let Some(v) = update.pr_number {
                ws.pr_number = Some(v);
            }
            if let Some(v) = update.pr_url {
                ws.pr_url = Some(v);
            }
            if let Some(v) = update.last_activity {
                ws.last_activity = Some(v);
            }
            if let Some(v) = update.attention {
                ws.attention = v;
            }
            if let Some(v) = update.archived {
                ws.archived = v;
            }
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        Box::pin(async move { store.delete_workspace(&id).await })
    }

    fn archive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            let now = now_iso();
            ws.status = WorkspaceStatus::Archived;
            ws.archived = true;
            ws.archived_at = Some(now.clone());
            ws.updated_at = now;
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            ws.status = WorkspaceStatus::Active;
            ws.archived = false;
            ws.archived_at = None;
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            ws.attention = WorkspaceAttention::None;
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn mark_seen(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            // "Seen" clears the unread flag; review-required attention persists.
            if ws.attention == WorkspaceAttention::Unread {
                ws.attention = WorkspaceAttention::None;
                ws.updated_at = now_iso();
                store.update_workspace(&ws).await?;
            }
            Ok(ws)
        })
    }

    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let store = self.store.clone();
        let id = workspace_id.clone();
        Box::pin(async move { store.list_notes(&id).await })
    }
}

// Core domain service modules (§3.1).
pub mod notes {}
pub mod tasks {}
pub mod comments {}
pub mod workspace {}
pub mod agent {}
pub mod git {}
pub mod pr {}
pub mod script {}
pub mod file {}
pub mod event {}
pub mod drafts {} // §9.10

// Agent-Ecosystem modules (§18).
pub mod rules {}
pub mod specialists {}
pub mod mcp_servers {}
pub mod memories {}

// Code Changes Review modules (§17).
pub mod file_tracking {}
pub mod diffs {}
pub mod accept_changes {}
pub mod metrics {}

// Integrations & Ops modules (§19).
pub mod token_usage {}
pub mod session_stats {}
pub mod setup_scripts {}
