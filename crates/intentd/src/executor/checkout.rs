//! Where a remote agent works on the node: an isolated git worktree of the
//! commit head recorded as the agent's `base` ref, fetched from head over
//! the link's git connection, and the WIP pushes that send the agent's work
//! back under its own refs.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context as _};

use intent_core::{AgentId, BoxFuture, WorkspaceId};
use intent_git::node_git::{AgentRemote, CheckoutCache};

use super::storage::Storage;

/// Longest a WIP push may take; a lost git connection fails it sooner.
const WIP_PUSH_TIMEOUT: Duration = Duration::from_secs(120);

/// How the runner gets an agent's working directory and sends its work back.
pub(super) trait Checkouts: Send + Sync {
    /// The agent's working directory at `base_commit`. A relaunched agent
    /// (same id) keeps the checkout it already has, with its work.
    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        base_commit: &'a str,
        repo_key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<PathBuf>>;

    /// Push the agent's committed `HEAD` to its `head` ref and a commit of
    /// its dirty tree to its `wip` ref.
    fn push_wip<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        worktree: &'a Path,
    ) -> BoxFuture<'a, anyhow::Result<()>>;
}

/// Worktrees in the node's per-repository bare caches.
pub(super) struct GitCheckouts {
    cache: CheckoutCache,
    storage: Arc<Storage>,
}

impl GitCheckouts {
    pub(super) fn new(cache: CheckoutCache, storage: Arc<Storage>) -> Self {
        Self { cache, storage }
    }
}

impl Checkouts for GitCheckouts {
    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        base_commit: &'a str,
        repo_key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<PathBuf>> {
        Box::pin(async move {
            let worktree = self.storage.agent_worktree(agent_id)?;
            if worktree.join(".git").exists() {
                return Ok(worktree);
            }
            let remote = AgentRemote::new(&workspace_id.0, &agent_id.0)?;
            let fetched = self
                .cache
                .checkout(repo_key, &remote, &worktree)
                .await
                .context("fetch the agent's base commit from head")?;
            ensure!(
                fetched == base_commit,
                "head's base ref is at {fetched}, not the launched commit {base_commit}"
            );
            Ok(worktree)
        })
    }

    fn push_wip<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        worktree: &'a Path,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let remote = AgentRemote::new(&workspace_id.0, &agent_id.0)?;
            tokio::time::timeout(
                WIP_PUSH_TIMEOUT,
                self.cache.git().push_wip(worktree, &remote),
            )
            .await
            .context("WIP push timed out")??;
            Ok(())
        })
    }
}

/// Test checkouts: a plain working directory per agent, and a record of the
/// WIP pushes the runner asked for.
#[cfg(test)]
pub(super) struct PlainCheckouts {
    storage: Arc<Storage>,
    pub(super) pushes: std::sync::Mutex<Vec<AgentId>>,
    /// Fail every checkout, like a base head cannot serve.
    pub(super) fail: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
impl PlainCheckouts {
    pub(super) fn new(storage: Arc<Storage>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            pushes: std::sync::Mutex::default(),
            fail: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
impl Checkouts for PlainCheckouts {
    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        _workspace_id: &'a WorkspaceId,
        _base_commit: &'a str,
        _repo_key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<PathBuf>> {
        Box::pin(async move {
            ensure!(
                !self.fail.load(std::sync::atomic::Ordering::SeqCst),
                "head refused the fetch"
            );
            self.storage.agent_dir(agent_id)
        })
    }

    fn push_wip<'a>(
        &'a self,
        agent_id: &'a AgentId,
        _workspace_id: &'a WorkspaceId,
        _worktree: &'a Path,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.pushes.lock().unwrap().push(agent_id.clone());
            Ok(())
        })
    }
}
