//! Where a remote agent works on the node: an isolated git worktree of the
//! commit head recorded as the agent's `base` ref, fetched from head over
//! the link's git connection, and the WIP pushes that send the agent's work
//! back under its own refs.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;

use intent_core::node_link::{forge_agent_branch, ForgeLaunch, SubmodulePin};
use intent_core::{AgentId, BoxFuture, WorkspaceId};
use intent_git::node_git::{self, AgentRemote, CheckoutCache, ModuleCheckout};

use super::storage::Storage;

/// Longest a WIP push may take; a lost git connection fails it sooner.
pub(super) const WIP_PUSH_TIMEOUT: Duration = Duration::from_secs(120);

/// Longest a final stop's push may take (nodeProtocol 9).
pub(super) const FINAL_PUSH_TIMEOUT: Duration = Duration::from_secs(60);

/// An agent's prepared working directory.
#[derive(Debug)]
pub(super) struct Prepared {
    pub(super) worktree: PathBuf,
    /// The agent's private temp folder beside the worktree, removed with it.
    pub(super) tmp: PathBuf,
    /// Prepended to the agent's first turn: its saved `wip` work could not
    /// be restored on top of its saved head.
    pub(super) notice: Option<String>,
    /// This launch created the checkout (it did not keep one).
    pub(super) fresh: bool,
}

/// How the runner gets an agent's working directory and sends its work back.
pub(super) trait Checkouts: Send + Sync {
    /// The agent's working directory at `base_commit`, with `submodules`
    /// (nodeProtocol 10) checked out at their pins. A relaunched agent
    /// (same id) keeps the checkout it already has, with its work; one whose
    /// checkout was removed starts from its saved `head` ref with its saved
    /// `wip` changes restored on top, in every repository.
    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        base: (&'a str, &'a str),
        submodules: &'a [SubmodulePin],
        progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, anyhow::Result<Prepared>>;

    /// The machine-local directory a setup script gets as `MAIN_CHECKOUT`
    /// for the repository `repo_key`, created empty.
    fn main_checkout(&self, repo_key: &str) -> anyhow::Result<PathBuf>;

    /// The agent's existing checkout on this machine, if it has one (it
    /// stays after the agent's process stops). Saved scripts run in it.
    fn existing(&self, agent_id: &AgentId) -> Option<PathBuf>;

    /// Push the agent's committed `HEAD` to its `head` ref and a commit of
    /// its dirty tree to its `wip` ref, within `timeout`; its pinned
    /// submodules first, deepest first.
    fn push_wip<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        worktree: &'a Path,
        timeout: Duration,
    ) -> BoxFuture<'a, anyhow::Result<()>>;

    /// The agent's private temp folder, created on demand in its own
    /// directory: also for an agent that works in another agent's checkout
    /// (nodeProtocol 13), so two agents never share one.
    fn agent_tmp(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf>;

    /// The agents with a directory here.
    fn agents(&self) -> Vec<AgentId>;

    /// Remove the agent's directory and its worktree's registration in the
    /// caches; removing an agent with none is a no-op.
    fn remove<'a>(&'a self, agent_id: &'a AgentId) -> BoxFuture<'a, anyhow::Result<()>>;

    /// Give the agent's own checkout `worktree` its agent-local branch and
    /// worktree-scoped `origin` for `forge` (nodeProtocol 15), or remove
    /// them for a launch without one.
    fn forge<'a>(
        &'a self,
        agent_id: &'a AgentId,
        worktree: &'a Path,
        forge: Option<&'a ForgeLaunch>,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        let _ = (agent_id, worktree, forge);
        Box::pin(async { Ok(()) })
    }

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
    fn main_checkout(&self, repo_key: &str) -> anyhow::Result<PathBuf> {
        self.storage.main_checkout(repo_key)
    }

    fn existing(&self, agent_id: &AgentId) -> Option<PathBuf> {
        let worktree = self.storage.agent_worktree(agent_id).ok()?;
        worktree.join(".git").exists().then_some(worktree)
    }

    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        workspace_id: &'a WorkspaceId,
        (base_commit, repo_key): (&'a str, &'a str),
        submodules: &'a [SubmodulePin],
        progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, anyhow::Result<Prepared>> {
        Box::pin(async move {
            node_git::validate_pins(submodules).context("the launch's submodule pins")?;
            let worktree = self.storage.agent_worktree(agent_id)?;
            let remote = AgentRemote::new(&workspace_id.0, &agent_id.0)?;
            let fresh = !worktree.join(".git").exists();
            let mut notices = Vec::new();
            if fresh {
                progress("Fetching the checkout…");
                let checkout = self
                    .cache
                    .checkout(repo_key, &remote, &worktree)
                    .await
                    .context("fetch the agent's refs from head")?;
                if checkout.base != base_commit {
                    self.discard(&worktree).await;
                    anyhow::bail!(
                        "head's base ref is at {}, not the launched commit {base_commit}",
                        checkout.base
                    );
                }
                notices.extend(
                    checkout
                        .unrestored_wip
                        .map(|wip| unrestored_wip_notice(&remote, &checkout.at, &wip)),
                );
            }
            // Recorded before the modules, so a push or removal of a
            // partly prepared checkout knows them.
            write_pins(&pins_file(&worktree), submodules)?;
            // A kept checkout gets the pins it lacks (one prepared before
            // the machine knew them); its present modules are left alone.
            let modules = match self
                .cache
                .checkout_modules_reporting(repo_key, &remote, &worktree, submodules, &|path| {
                    // A kept checkout's output is its setup's: only a fresh
                    // one reports progress, which starts the output over.
                    if fresh {
                        progress(&format!("Fetching {path}…"));
                    }
                })
                .await
            {
                Ok(modules) => modules,
                Err(error) => {
                    if fresh {
                        self.discard(&worktree).await;
                    }
                    return Err(anyhow::anyhow!("{error}").context("check out the submodules"));
                }
            };
            notices.extend(modules.iter().filter_map(|module| {
                let ModuleCheckout {
                    path,
                    at,
                    unrestored_wip,
                    ..
                } = module;
                let wip = unrestored_wip.as_ref()?;
                Some(unrestored_module_notice(&remote, path, at, wip))
            }));
            let notice = (!notices.is_empty()).then(|| notices.join("\n\n"));
            Ok(Prepared {
                tmp: self.storage.agent_tmp(agent_id)?,
                worktree,
                notice,
                fresh,
            })
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
            let pins = read_pins(&pins_file(worktree));
            tokio::time::timeout(
                timeout,
                self.cache.git().push_work_excluding(
                    worktree,
                    &remote,
                    &pins,
                    Some(&super::setup::excludes_dir(worktree)),
                ),
            )
            .await
            .context("WIP push timed out")??;
            Ok(())
        })
    }

    fn agent_tmp(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        self.storage.agent_tmp(agent_id)
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
                .map_err(|e| anyhow::anyhow!("prune the checkout caches: {e}"))?;
            self.cache
                .delete_branch(&forge_agent_branch(agent_id))
                .await;
            Ok(())
        })
    }

    fn forge<'a>(
        &'a self,
        agent_id: &'a AgentId,
        worktree: &'a Path,
        forge: Option<&'a ForgeLaunch>,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let local = forge_agent_branch(agent_id);
            match forge {
                Some(forge) => self
                    .cache
                    .set_forge(worktree, &local, &forge.remote_url, &forge.branch)
                    .await
                    .map_err(|e| anyhow::anyhow!("set up the agent's branch and origin: {e}")),
                None => {
                    self.cache.clear_forge(worktree, &local).await;
                    Ok(())
                }
            }
        })
    }
}

impl GitCheckouts {
    /// Remove a checkout this launch created and could not finish, so the
    /// next launch starts fresh.
    async fn discard(&self, worktree: &Path) {
        if let Err(error) = std::fs::remove_dir_all(worktree) {
            tracing::warn!(%error, "node: could not remove a partial checkout");
        }
        let _ = std::fs::remove_file(pins_file(worktree));
        if let Err(error) = self.cache.prune().await {
            tracing::warn!(%error, "node: could not prune the checkout caches");
        }
    }
}

/// Where an agent's submodule pins are kept: beside its worktree, in its
/// own storage directory, so neither a push nor the agent sees them.
fn pins_file(worktree: &Path) -> PathBuf {
    worktree.with_file_name("submodules.json")
}

fn write_pins(file: &Path, pins: &[SubmodulePin]) -> anyhow::Result<()> {
    let body = serde_json::to_vec(pins).context("encode the submodule pins")?;
    let scratch = file.with_extension("json.tmp");
    std::fs::write(&scratch, body).context("write the submodule pins")?;
    std::fs::rename(&scratch, file).context("write the submodule pins")
}

/// The pins recorded with the checkout at `worktree`: its owner's, for an
/// agent that shares it.
pub(super) fn recorded_pins(worktree: &Path) -> Vec<SubmodulePin> {
    read_pins(&pins_file(worktree))
}

/// The agent's recorded pins; none for a checkout prepared without them.
fn read_pins(file: &Path) -> Vec<SubmodulePin> {
    match std::fs::read(file) {
        Ok(body) => serde_json::from_slice(&body).unwrap_or_else(|error| {
            tracing::warn!(%error, "node: unreadable submodule pins; pushing the superproject only");
            Vec::new()
        }),
        Err(_) => Vec::new(),
    }
}

/// [`unrestored_wip_notice`] for the submodule at `path`, whose wip is
/// pinned in that submodule's repository.
fn unrestored_module_notice(remote: &AgentRemote, path: &str, at: &str, wip: &str) -> String {
    let pinned = remote.unapplied_ref(wip);
    format!(
        "Note from Intent: the submodule {path} of this agent's checkout was recreated at its \
         saved head commit {at}. Its uncommitted work (commit {wip}) could not be applied on top \
         of it and is kept in {pinned} in that submodule's repository. Inspect it with \
         `git -C {path} diff {wip}^ {wip}` and reapply what you need."
    )
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
    /// Panic in the next push (after any hold), like a bug in the cleanup.
    pub(super) panic_push: std::sync::atomic::AtomicBool,
    /// Holds the next push: signals the first sender when it starts and
    /// waits for the receiver.
    pub(super) hold_push: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
    /// Fail every checkout as an unapplied wip that head would not pin.
    pub(super) fail_pin: std::sync::atomic::AtomicBool,
    /// The notice the next checkout reports.
    pub(super) notice: std::sync::Mutex<Option<String>>,
    /// The submodule pins of each checkout prepared, in order.
    pub(super) prepared_pins: std::sync::Mutex<Vec<Vec<SubmodulePin>>>,
    /// Every push waits here first, so a test can hold pushes until a
    /// number of them are under way together.
    pub(super) push_gate: Option<Arc<tokio::sync::Barrier>>,
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
            hold_push: std::sync::Mutex::default(),
            panic_push: std::sync::atomic::AtomicBool::new(false),
            notice: std::sync::Mutex::default(),
            prepared_pins: std::sync::Mutex::default(),
            push_gate: None,
            hold_sweep: std::sync::Mutex::default(),
        })
    }

    /// Checkouts whose every push first waits at `gate`.
    pub(super) fn with_push_gate(
        storage: Arc<Storage>,
        gate: Arc<tokio::sync::Barrier>,
    ) -> Arc<Self> {
        let mut checkouts = Arc::into_inner(Self::new(storage)).expect("just created");
        checkouts.push_gate = Some(gate);
        Arc::new(checkouts)
    }
}

#[cfg(test)]
impl Checkouts for PlainCheckouts {
    fn main_checkout(&self, repo_key: &str) -> anyhow::Result<PathBuf> {
        self.storage.main_checkout(repo_key)
    }

    fn existing(&self, agent_id: &AgentId) -> Option<PathBuf> {
        self.storage.existing_agent_dir(agent_id)
    }

    fn prepare<'a>(
        &'a self,
        agent_id: &'a AgentId,
        _workspace_id: &'a WorkspaceId,
        _base: (&'a str, &'a str),
        submodules: &'a [SubmodulePin],
        progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, anyhow::Result<Prepared>> {
        Box::pin(async move {
            for pin in submodules {
                progress(&format!("Fetching {}…", pin.path));
            }
            anyhow::ensure!(
                !self.fail.load(std::sync::atomic::Ordering::SeqCst),
                "head refused the fetch"
            );
            self.prepared_pins.lock().unwrap().push(submodules.to_vec());
            if self.fail_pin.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow::anyhow!("pin the unapplied wip on head: refused")
                    .context("fetch the agent's refs from head"));
            }
            let fresh = self.storage.existing_agent_dir(agent_id).is_none();
            Ok(Prepared {
                worktree: self.storage.agent_dir(agent_id)?,
                tmp: self.storage.agent_tmp(agent_id)?,
                notice: self.notice.lock().unwrap().take(),
                fresh,
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
            let hold = self.hold_push.lock().unwrap().take();
            if let Some((entered, release)) = hold {
                let _ = entered.send(());
                let _ = release.await;
            }
            assert!(
                !self
                    .panic_push
                    .swap(false, std::sync::atomic::Ordering::SeqCst),
                "test: the push panicked"
            );
            if let Some(gate) = &self.push_gate {
                gate.wait().await;
            }
            anyhow::ensure!(
                !self.fail_pushes.load(std::sync::atomic::Ordering::SeqCst),
                "head refused the push"
            );
            Ok(())
        })
    }

    fn agent_tmp(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        self.storage.agent_tmp(agent_id)
    }

    fn agents(&self) -> Vec<AgentId> {
        self.storage.agent_ids()
    }

    fn remove<'a>(&'a self, agent_id: &'a AgentId) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            anyhow::ensure!(
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
