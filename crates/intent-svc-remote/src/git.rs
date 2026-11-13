//! Head's authority over the node's git sessions: an agent's refs are
//! served only on the node's current binding, only for an agent recorded
//! on that binding's lease, and only from that agent's workspace. A module
//! session (nodeProtocol 10) reaches only a submodule repository named by
//! the agent's recorded pins, still a gitlink of the workspace's checkout.
//! A sub-agent's forge session (nodeProtocol 17) reads the workspace's
//! GitHub repository, refreshed here first ([`forge_refresh`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use intent_core::node_link::{
    ForgeFetchLaunch, LeaseBinding, FORGE_FETCH_PROTOCOL, HOMES_PROTOCOL,
    PREPARED_CHECKOUT_PROTOCOL,
};
use intent_core::remote_launch::RemotePlacementView;
use intent_core::{AgentId, BoxFuture, Error, Result, WorkspaceId};
use intent_git::node_git::{self, forge, AgentRemote, GitService, ModuleRefusal};
use intent_transport::node_link::{git_codes, ForgeServe, NodeGitAccess, FORGE_REFRESH_FAILED};
use tokio::time::Instant;

use crate::connector::Shared;
use crate::lock;
use intent_services::feature_api::Services;

pub(crate) struct HeadGitAccess {
    shared: Arc<Shared>,
    /// Rewritten pins this access made before a home push, by workspace:
    /// dropped once the push turned out a fast-forward.
    fresh_pins: Mutex<HashMap<WorkspaceId, Vec<String>>>,
}

impl HeadGitAccess {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            fresh_pins: Mutex::new(HashMap::new()),
        }
    }
}

fn denied() -> Error {
    Error::Forbidden(git_codes::DENIED.into())
}

fn unavailable(what: &str) -> Error {
    Error::Internal(format!("{}: {what}", git_codes::UNAVAILABLE))
}

/// How long a successful refresh serves later forge sessions.
const REFRESH_REUSE: Duration = Duration::from_secs(5);

/// Points the forge refresh at another host than `https://github.com/`, as
/// `INTENTD_GITHUB_API_BASE_URI` does for the API: the e2e tests use a local
/// stand-in. The backend's token is offered only to `https://github.com`.
const GIT_BASE_ENV: &str = "INTENTD_GITHUB_GIT_BASE_URL";

/// One repository's refreshes: when the last successful one ended, and
/// when the last one of any outcome ended, with its outcome.
#[derive(Default)]
struct RefreshState {
    succeeded: Option<Instant>,
    ended: Option<(Instant, bool)>,
}

/// Per repository (its git common directory): held while a refresh runs.
type RefreshSlot = Arc<tokio::sync::Mutex<RefreshState>>;

static REFRESHES: LazyLock<Mutex<HashMap<PathBuf, RefreshSlot>>> = LazyLock::new(Mutex::default);

/// Run `fetch` for `slot`'s repository, shared among sessions close
/// together: one that arrived while a fetch ran gets that fetch's outcome,
/// failed or not, and one within [`REFRESH_REUSE`] after a successful fetch
/// uses it. Any later session fetches again.
async fn coalesced<F>(slot: &RefreshSlot, fetch: impl FnOnce() -> F) -> bool
where
    F: std::future::Future<Output = bool>,
{
    let arrived = Instant::now();
    let mut state = slot.lock().await;
    if let Some((ended, ok)) = state.ended {
        if ended > arrived {
            return ok;
        }
    }
    if state
        .succeeded
        .is_some_and(|done| arrived.duration_since(done) <= REFRESH_REUSE)
    {
        return true;
    }
    let ok = fetch().await;
    let now = Instant::now();
    state.ended = Some((now, ok));
    if ok {
        state.succeeded = Some(now);
    }
    ok
}

/// Refresh `repository`'s forge namespace from `url` (see [`coalesced`]).
/// Worktrees of one repository share the namespace, so they share a slot.
async fn forge_refresh(repository: &Path, url: &str, token: Option<&str>) -> bool {
    let key = forge::common_dir(repository)
        .await
        .unwrap_or_else(|| repository.to_path_buf());
    let slot = REFRESHES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(key)
        .or_default()
        .clone();
    coalesced(&slot, || async {
        match forge::refresh(repository, url, token).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "forge refresh from GitHub failed; serving the last fetched refs");
                // Under the repository's lock, so one session seeds.
                if !forge::has_refs(repository).await {
                    if let Err(error) = forge::seed_from_origin(repository).await {
                        tracing::warn!(%error, "could not copy the origin refs for the forge fallback");
                    }
                }
                false
            }
        }
    })
    .await
}

/// The URL head fetches for `launch`.
fn fetch_url(launch: &ForgeFetchLaunch) -> String {
    match std::env::var(GIT_BASE_ENV) {
        Ok(base) if !base.is_empty() => launch.remote_url.replacen("https://github.com/", &base, 1),
        _ => launch.remote_url.clone(),
    }
}

impl HeadGitAccess {
    /// The workspace checkout `remote`'s agent may read GitHub from: a
    /// sub-agent on `binding`'s lease, its workspace `origin` on GitHub.
    async fn forge_checkout(
        &self,
        binding: &LeaseBinding,
        remote: &AgentRemote,
    ) -> Result<(PathBuf, ForgeFetchLaunch)> {
        let current = self
            .shared
            .current_link()
            .ok_or_else(|| Error::InvalidParams("node-link-stale".into()))?;
        binding.ensure_current(current.binding())?;
        if self.shared.protocol() < FORGE_FETCH_PROTOCOL {
            return Err(Error::InvalidParams(git_codes::INVALID.into()));
        }
        let agent_id = AgentId::from(remote.agent_id());
        if self.shared.store.get_final_stop(&agent_id).await?.is_some() {
            return Err(denied());
        }
        let record = self
            .shared
            .store
            .get_remote_agent(&agent_id)
            .await?
            .ok_or_else(denied)?;
        if record.lease_id != binding.lease_id
            || record.workspace_id.as_str() != remote.workspace_id()
        {
            return Err(denied());
        }
        let session = self
            .shared
            .store
            .get_agent_session_summary(&agent_id)
            .await?;
        let shares = RemotePlacementView::from_metadata(session.metadata.as_ref())
            .is_some_and(|view| view.shared_checkout_of.is_some());
        if session.parent_agent_id.is_none() && !session.is_background && !shares {
            return Err(denied());
        }
        let workspace = self
            .shared
            .store
            .get_workspace(&WorkspaceId::from(remote.workspace_id()))
            .await?;
        let checkout = crate::launch::checkout_path(&workspace).ok_or_else(denied)?;
        let probe = checkout.clone();
        let origin = tokio::task::spawn_blocking(move || intent_git::remote::origin_url(&probe))
            .await
            .map_err(|e| Error::Internal(format!("node git: read the origin: {e}")))??;
        let launch = origin
            .as_deref()
            .and_then(ForgeFetchLaunch::for_github)
            .ok_or_else(denied)?;
        Ok((checkout, launch))
    }
}

impl HeadGitAccess {
    /// The workspace checkout of `remote`'s home and its workspace: only on
    /// the current binding, for the workspace's current home, on this
    /// machine.
    async fn home_checkout(
        &self,
        binding: &LeaseBinding,
        remote: &AgentRemote,
    ) -> Result<(PathBuf, WorkspaceId)> {
        let current = self
            .shared
            .current_link()
            .ok_or_else(|| Error::InvalidParams("node-link-stale".into()))?;
        binding.ensure_current(current.binding())?;
        let workspace_id = WorkspaceId::from(remote.workspace_id());
        let home = self
            .shared
            .store
            .get_workspace_home(&workspace_id)
            .await?
            .ok_or_else(denied)?;
        if home.home_id.as_str() != remote.agent_id()
            || home.node_id.as_deref() != Some(self.shared.machine_id().as_str())
        {
            return Err(denied());
        }
        let workspace = self.shared.store.get_workspace(&workspace_id).await?;
        let checkout = crate::launch::checkout_path(&workspace).ok_or_else(denied)?;
        Ok((checkout, workspace_id))
    }

    /// The repository a home session reaches (nodes.md "Workspace homes" →
    /// "Refs"): the workspace checkout, or one of its submodules. Before a
    /// push of the superproject, the current `home/head` is pinned as
    /// `home/rewritten/<sha>`, so a rewrite keeps the old commits; a push
    /// that cannot be pinned is refused.
    async fn home_repository(
        &self,
        binding: &LeaseBinding,
        remote: &AgentRemote,
        service: GitService,
    ) -> Result<PathBuf> {
        let (checkout, workspace_id) = self.home_checkout(binding, remote).await?;
        if let Some(key) = remote.module_key() {
            let key = key.to_owned();
            let module = tokio::task::spawn_blocking(move || {
                let pin = node_git::submodule_pins(&checkout)
                    .ok()
                    .and_then(|pins| pins.into_iter().find(|pin| pin.module_key == key));
                pin.map(|pin| node_git::module_repository(&checkout, &pin.path, &key))
            })
            .await
            .map_err(|e| Error::Internal(format!("node git: resolve the submodule: {e}")))?;
            return match module {
                Some(Ok(module)) => Ok(module.worktree),
                Some(Err(ModuleRefusal::Unavailable(what))) => Err(unavailable(&what)),
                Some(Err(ModuleRefusal::Denied)) | None => Err(denied()),
            };
        }
        if service == GitService::ReceivePack {
            let repository = checkout.clone();
            let remote = remote.clone();
            let pinned = tokio::task::spawn_blocking(move || {
                crate::homes::pin_home_head(&repository, &remote)
            })
            .await
            .map_err(|e| Error::Internal(format!("node git: pin the home's head: {e}")))?
            .map_err(|error| unavailable(&format!("could not pin the home's head: {error}")))?;
            if let Some(sha) = pinned {
                lock(&self.fresh_pins)
                    .entry(workspace_id)
                    .or_default()
                    .push(sha);
            }
        }
        Ok(checkout)
    }
}

impl NodeGitAccess for HeadGitAccess {
    fn forge_supported(&self) -> bool {
        self.shared.protocol() >= FORGE_FETCH_PROTOCOL
    }

    fn homes_supported(&self) -> bool {
        self.shared.protocol() >= HOMES_PROTOCOL
            && self
                .shared
                .services()
                .is_some_and(Services::workspace_homes)
    }

    fn received<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if !remote.is_home() || remote.module_key().is_some() {
                return;
            }
            let workspace_id = WorkspaceId::from(remote.workspace_id());
            let pins = lock(&self.fresh_pins)
                .remove(&workspace_id)
                .unwrap_or_default();
            let Ok((checkout, _)) = self.home_checkout(binding, remote).await else {
                return;
            };
            let remote = remote.clone();
            let settled = tokio::task::spawn_blocking(move || {
                crate::homes::settle_home_push(&checkout, &remote, &pins)
            })
            .await;
            match settled {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(workspace = %workspace_id, %error, "remote machine: could not settle a home push");
                }
                Err(error) => {
                    tracing::warn!(workspace = %workspace_id, %error, "remote machine: settling a home push panicked");
                }
            }
        })
    }

    fn forge<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
    ) -> BoxFuture<'a, Result<ForgeServe>> {
        Box::pin(async move {
            let (repository, launch) = self.forge_checkout(binding, remote).await?;
            let registry = self
                .shared
                .services()
                .and_then(intent_services::feature_api::Services::settings_registry);
            let token = intent_services::github_backend_token(registry.as_deref()).await;
            let token = token.map(zeroize::Zeroizing::new);
            let fresh = forge_refresh(
                &repository,
                &fetch_url(&launch),
                token.as_deref().map(String::as_str),
            )
            .await;
            if fresh {
                return Ok(ForgeServe {
                    repository,
                    warning: None,
                });
            }
            if !forge::has_refs(&repository).await {
                return Err(unavailable("no GitHub refs fetched yet"));
            }
            Ok(ForgeServe {
                repository,
                warning: Some(FORGE_REFRESH_FAILED),
            })
        })
    }

    fn repository<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
        service: GitService,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            let current = self
                .shared
                .current_link()
                .ok_or_else(|| Error::InvalidParams("node-link-stale".into()))?;
            binding.ensure_current(current.binding())?;
            if remote.module_key().is_some() && self.shared.protocol() < PREPARED_CHECKOUT_PROTOCOL
            {
                return Err(Error::InvalidParams(git_codes::INVALID.into()));
            }
            if remote.is_home() {
                return self.home_repository(binding, remote, service).await;
            }
            let agent_id = AgentId::from(remote.agent_id());
            // A final stop admits the agent's last push (receive-pack only)
            // into the workspace's repository, even once its session,
            // placement or worktree is gone.
            if let Some(stop) = self.shared.store.get_final_stop(&agent_id).await? {
                if stop.lease_id != binding.lease_id
                    || stop.workspace_id.as_str() != remote.workspace_id()
                    || service != GitService::ReceivePack
                {
                    return Err(denied());
                }
                let repo_path = match remote.module_key() {
                    Some(key) => {
                        &stop
                            .submodules
                            .iter()
                            .find(|module| module.module_key == key)
                            .ok_or_else(denied)?
                            .git_dir
                    }
                    None => &stop.repo_path,
                };
                let repo = PathBuf::from(repo_path);
                if repo_path.is_empty() || !repo.is_dir() {
                    return Err(unavailable("the workspace's repository is gone"));
                }
                return Ok(repo);
            }
            let record = self
                .shared
                .store
                .get_remote_agent(&agent_id)
                .await?
                .ok_or_else(denied)?;
            if record.lease_id != binding.lease_id
                || record.workspace_id.as_str() != remote.workspace_id()
            {
                return Err(denied());
            }
            let workspace = self
                .shared
                .store
                .get_workspace(&WorkspaceId::from(remote.workspace_id()))
                .await?;
            let checkout = crate::launch::checkout_path(&workspace).ok_or_else(denied)?;
            let Some(key) = remote.module_key() else {
                return Ok(checkout);
            };
            let session = self
                .shared
                .store
                .get_agent_session_summary(&agent_id)
                .await?;
            let pin = RemotePlacementView::from_metadata(session.metadata.as_ref())
                .and_then(|view| {
                    view.submodules
                        .unwrap_or_default()
                        .into_iter()
                        .find(|pin| pin.module_key == key)
                })
                .ok_or_else(denied)?;
            let key = key.to_owned();
            let module = tokio::task::spawn_blocking(move || {
                node_git::module_repository(&checkout, &pin.path, &key)
            })
            .await
            .map_err(|e| Error::Internal(format!("node git: resolve the submodule: {e}")))?;
            match module {
                Ok(module) => Ok(module.worktree),
                Err(ModuleRefusal::Denied) => Err(denied()),
                Err(ModuleRefusal::Unavailable(what)) => Err(unavailable(&what)),
            }
        })
    }
}

#[cfg(test)]
mod refresh_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    async fn counted(fetches: &AtomicUsize, ok: bool) -> bool {
        fetches.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(1)).await;
        ok
    }

    #[tokio::test(start_paused = true)]
    async fn sessions_that_wait_on_a_failed_fetch_share_its_failure() {
        let slot = RefreshSlot::default();
        let fetches = AtomicUsize::new(0);
        let results = tokio::join!(
            coalesced(&slot, || counted(&fetches, false)),
            coalesced(&slot, || counted(&fetches, false)),
            coalesced(&slot, || counted(&fetches, false)),
            coalesced(&slot, || counted(&fetches, false)),
        );
        assert_eq!(results, (false, false, false, false));
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        // A session after that fetch ended fetches again.
        assert!(!coalesced(&slot, || counted(&fetches, false)).await);
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_successful_fetch_serves_sessions_for_five_seconds() {
        let slot = RefreshSlot::default();
        let fetches = AtomicUsize::new(0);
        assert!(coalesced(&slot, || counted(&fetches, true)).await);
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert!(coalesced(&slot, || counted(&fetches, false)).await);
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!coalesced(&slot, || counted(&fetches, false)).await);
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }
}
