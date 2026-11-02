//! Head's authority over the node's git sessions: an agent's refs are
//! served only on the node's current binding, only for an agent recorded
//! on that binding's lease, and only from that agent's workspace.

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::node_link::LeaseBinding;
use intent_core::{AgentId, BoxFuture, Error, Result, WorkspaceId};
use intent_git::node_git::AgentRemote;
use intent_transport::node_link::NodeGitAccess;

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
    Error::Forbidden("node-git-denied".into())
}

impl NodeGitAccess for HeadGitAccess {
    fn repository<'a>(
        &'a self,
        binding: &'a LeaseBinding,
        remote: &'a AgentRemote,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            let current = self
                .shared
                .current_link()
                .ok_or_else(|| Error::InvalidParams("node-link-stale".into()))?;
            binding.ensure_current(current.binding())?;
            let agent_id = AgentId::from(remote.agent_id());
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
            crate::launch::checkout_path(&workspace).ok_or_else(denied)
        })
    }
}
