//! Head's authority over the node's git sessions: an agent's refs are
//! served only on the node's current binding, only for an agent recorded
//! on that binding's lease, and only from that agent's workspace. A module
//! session (nodeProtocol 10) reaches only a submodule repository named by
//! the agent's recorded pins, still a gitlink of the workspace's checkout.

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::node_link::{LeaseBinding, PREPARED_CHECKOUT_PROTOCOL};
use intent_core::remote_launch::RemotePlacementView;
use intent_core::{AgentId, BoxFuture, Error, Result, WorkspaceId};
use intent_git::node_git::{self, AgentRemote, GitService, ModuleRefusal};
use intent_transport::node_link::{git_codes, NodeGitAccess};

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

impl NodeGitAccess for HeadGitAccess {
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
