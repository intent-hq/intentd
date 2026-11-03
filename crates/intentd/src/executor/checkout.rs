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
pub(super) const WIP_PUSH_TIMEOUT: Duration = Duration::from_secs(120);

/// Longest a final stop's push may take (nodeProtocol 9).
pub(super) const FINAL_PUSH_TIMEOUT: Duration = Duration::from_secs(60);

/// An agent's prepared working directory.
#[derive(Debug)]
pub(super) struct Prepared {
    pub(super) worktree: PathBuf,
    /// Prepended to the agent's first turn: its saved `wip` work could not
    /// be restored on top of its saved head.
    pub(super) notice: Option<String>,
}

/// How the runner gets an agent's working directory and sends its work back.
pub(super) trait Checkouts: Send + Sync {
    /// The agent's working directory at `base_commit`. A relaunched agent
    /// (same id) keeps the checkout it already has, with its work; one whose
    /// checkout was removed starts from its saved `head` ref with its saved
    /// `wip` changes restored on top.
    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        base_commit: &'a str,
        repo_key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Prepared>>;

    /// The agent's existing checkout on this machine, if it has one (it
    /// stays after the agent's process stops). Saved scripts run in it.
    fn existing(&self, agent_id: &AgentId) -> Option<PathBuf>;

    /// Push the agent's committed `HEAD` to its `head` ref and a commit of
    /// its dirty tree to its `wip` ref, within `timeout`.
    fn push_wip<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        worktree: &'a Path,
        timeout: Duration,
    ) -> BoxFuture<'a, anyhow::Result<()>>;

    /// The agents with a directory here.
    fn agents(&self) -> Vec<AgentId>;

    /// Remove the agent's directory and its worktree's registration in the
    /// caches; removing an agent with none is a no-op.
    fn remove<'a>(&'a self, agent_id: &'a AgentId) -> BoxFuture<'a, anyhow::Result<()>>;

    /// Test seam: runs as the bind sweep is about to remove a checkout.
    #[cfg(test)]
    fn before_sweep_removal<'a>(&'a self, agent_id: &'a AgentId) -> BoxFuture<'a, ()> {
        let _ = agent_id;
        Box::pin(async {})
    }
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
    fn existing(&self, agent_id: &AgentId) -> Option<PathBuf> {
        let worktree = self.storage.agent_worktree(agent_id).ok()?;
        worktree.join(".git").exists().then_some(worktree)
    }

    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        base_commit: &'a str,
        repo_key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Prepared>> {
        Box::pin(async move {
            let worktree = self.storage.agent_worktree(agent_id)?;
            if worktree.join(".git").exists() {
                return Ok(Prepared {
                    worktree,
                    notice: None,
                });
            }
            let remote = AgentRemote::new(&workspace_id.0, &agent_id.0)?;
            let checkout = self
                .cache
                .checkout(repo_key, &remote, &worktree)
                .await
                .context("fetch the agent's refs from head")?;
            ensure!(
                checkout.base == base_commit,
                "head's base ref is at {}, not the launched commit {base_commit}",
                checkout.base
            );
            let notice = checkout
                .unrestored_wip
                .map(|wip| unrestored_wip_notice(&remote, &checkout.at, &wip));
            Ok(Prepared { worktree, notice })
        })
    }

    fn push_wip<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        worktree: &'a Path,
        timeout: Duration,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let remote = AgentRemote::new(&workspace_id.0, &agent_id.0)?;
            tokio::time::timeout(timeout, self.cache.git().push_wip(worktree, &remote))
                .await
                .context("WIP push timed out")??;
            Ok(())
        })
    }

    fn agents(&self) -> Vec<AgentId> {
        self.storage.agent_ids()
    }

    fn remove<'a>(&'a self, agent_id: &'a AgentId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.storage.remove_agent(agent_id)?;
            // The worktree's registration in its cache now points nowhere.
            self.cache
                .prune()
                .await
                .map_err(|e| anyhow::anyhow!("prune the checkout caches: {e}"))
        })
    }
}

/// What the agent's first turn is told when its saved `wip` work could not
/// be restored on top of its saved head (the work is pinned on head).
fn unrestored_wip_notice(remote: &AgentRemote, at: &str, wip: &str) -> String {
    let pinned = remote.unapplied_ref(wip);
    format!(
        "Note from Intent: this agent's checkout on this machine was removed and has been \
         recreated at its saved head commit {at}. Its uncommitted work (commit {wip}) could \
         not be applied on top of it and is kept in {pinned}. Inspect it with \
         `git diff {wip}^ {wip}` in this checkout and reapply what you need."
    )
}

/// Test checkouts: a plain working directory per agent, and a record of the
/// WIP pushes the runner asked for.
#[cfg(test)]
pub(super) struct PlainCheckouts {
    storage: Arc<Storage>,
    pub(super) pushes: std::sync::Mutex<Vec<AgentId>>,
    /// The timeout of each push, in order.
    pub(super) push_timeouts: std::sync::Mutex<Vec<Duration>>,
    /// Fail every push, like a head that refuses it.
    pub(super) fail_pushes: std::sync::atomic::AtomicBool,
    /// Fail every removal.
    pub(super) fail_removals: std::sync::atomic::AtomicBool,
    /// Fail every checkout, like a base head cannot serve.
    pub(super) fail: std::sync::atomic::AtomicBool,
    /// Fail every checkout as an unapplied wip that head would not pin.
    pub(super) fail_pin: std::sync::atomic::AtomicBool,
    /// The notice the next checkout reports.
    pub(super) notice: std::sync::Mutex<Option<String>>,
    /// Holds the next sweep removal: signals the first sender when it
    /// starts and waits for the receiver.
    pub(super) hold_sweep: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

#[cfg(test)]
impl PlainCheckouts {
    pub(super) fn new(storage: Arc<Storage>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            pushes: std::sync::Mutex::default(),
            push_timeouts: std::sync::Mutex::default(),
            fail_pushes: std::sync::atomic::AtomicBool::new(false),
            fail_removals: std::sync::atomic::AtomicBool::new(false),
            fail: std::sync::atomic::AtomicBool::new(false),
            fail_pin: std::sync::atomic::AtomicBool::new(false),
            notice: std::sync::Mutex::default(),
            hold_sweep: std::sync::Mutex::default(),
        })
    }
}

#[cfg(test)]
impl Checkouts for PlainCheckouts {
    fn existing(&self, agent_id: &AgentId) -> Option<PathBuf> {
        self.storage.existing_agent_dir(agent_id)
    }

    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        _workspace_id: &'a WorkspaceId,
        _base_commit: &'a str,
        _repo_key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<Prepared>> {
        Box::pin(async move {
            ensure!(
                !self.fail.load(std::sync::atomic::Ordering::SeqCst),
                "head refused the fetch"
            );
            if self.fail_pin.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow::anyhow!("pin the unapplied wip on head: refused")
                    .context("fetch the agent's refs from head"));
            }
            Ok(Prepared {
                worktree: self.storage.agent_dir(agent_id)?,
                notice: self.notice.lock().unwrap().take(),
            })
        })
    }

    fn push_wip<'a>(
        &'a self,
        agent_id: &'a AgentId,
        _workspace_id: &'a WorkspaceId,
        _worktree: &'a Path,
        timeout: Duration,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.pushes.lock().unwrap().push(agent_id.clone());
            self.push_timeouts.lock().unwrap().push(timeout);
            ensure!(
                !self.fail_pushes.load(std::sync::atomic::Ordering::SeqCst),
                "head refused the push"
            );
            Ok(())
        })
    }

    fn agents(&self) -> Vec<AgentId> {
        self.storage.agent_ids()
    }

    fn remove<'a>(&'a self, agent_id: &'a AgentId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            ensure!(
                !self.fail_removals.load(std::sync::atomic::Ordering::SeqCst),
                "the checkout could not be removed"
            );
            self.storage.remove_agent(agent_id)
        })
    }

    fn before_sweep_removal<'a>(&'a self, _agent_id: &'a AgentId) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let hold = self.hold_sweep.lock().unwrap().take();
            if let Some((entered, release)) = hold {
                let _ = entered.send(());
                let _ = release.await;
            }
        })
    }
}
