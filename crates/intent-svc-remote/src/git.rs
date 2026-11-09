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
    ForgeFetchLaunch, LeaseBinding, FORGE_FETCH_PROTOCOL, PREPARED_CHECKOUT_PROTOCOL,
};
use intent_core::remote_launch::RemotePlacementView;
use intent_core::{AgentId, BoxFuture, Error, Result, WorkspaceId};
use intent_git::node_git::{self, forge, AgentRemote, GitService, ModuleRefusal};
use intent_transport::node_link::{git_codes, ForgeServe, NodeGitAccess, FORGE_REFRESH_FAILED};
use tokio::time::Instant;

use crate::connector::Shared;

pub(crate) struct HeadGitAccess {
    shared: Arc<Shared>,
}

impl HeadGitAccess {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
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

/// Points the forge refresh at another host than `https://github.com/`
/// (tests: a local stand-in).
const GIT_BASE_ENV: &str = "INTENTD_GITHUB_GIT_BASE_URL";

/// Per workspace repository: held while a refresh runs; the end of the last
/// successful one.
type RefreshSlot = Arc<tokio::sync::Mutex<Option<Instant>>>;

static REFRESHES: LazyLock<Mutex<HashMap<PathBuf, RefreshSlot>>> = LazyLock::new(Mutex::default);

/// Refresh `repository`'s forge namespace from `url`, sharing one fetch
/// among sessions close together: one that arrives while a fetch runs
/// waits for it, and one within [`REFRESH_REUSE`] after a successful fetch
/// uses it. A failed fetch is not reused.
async fn forge_refresh(repository: &Path, url: &str, token: Option<&str>) -> bool {
    let slot = REFRESHES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(repository.to_path_buf())
        .or_default()
        .clone();
    let arrived = Instant::now();
    let mut last = slot.lock().await;
    if last.is_some_and(|done| done >= arrived || arrived.duration_since(done) <= REFRESH_REUSE) {
        return true;
    }
    match forge::refresh(repository, url, token).await {
        Ok(()) => {
            *last = Some(Instant::now());
            true
        }
        Err(error) => {
            tracing::warn!(%error, "forge refresh from GitHub failed; serving the last fetched refs");
            false
        }
    }
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

impl NodeGitAccess for HeadGitAccess {
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
            if !forge::has_refs(&repository).await && !forge::seed_from_origin(&repository).await? {
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
