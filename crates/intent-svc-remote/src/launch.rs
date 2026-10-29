//! The remote-launch feature port: admission against the configured node,
//! placing a created agent on it (its `base` ref and its record on the
//! lease), and the runtime that starts it there on its first turn.
//!
//! Auth is the selected provider profile's environment overlay, resolved
//! on head under the local selection rules and sent with the launch; the
//! node expands `~` against its own home and applies it like a local launch.
//! With no profile selected (and no Default-configuration overrides) head
//! sends nothing and the node uses its own provider login. A selected
//! profile that cannot be resolved fails the launch; it never falls back to
//! another account. Only environment values are sent: login files stay on
//! the node.

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::node_link::{LaunchCommand, LaunchProfileEnv};
use intent_core::placement::{
    AgentPlacement, PlacementCheckout, PlacementTarget, PlatformAvailability,
};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteAgentRecord, RemoteLaunchRequest, RemotePlacementView,
};
use intent_core::{AgentId, BoxFuture, Error, Result, Workspace, WorkspaceId};
use intent_git::node_git::{self, AgentRemote};
use intent_services::feature_api::feature_ports::{RemoteLaunchPort, RemoteRuntime};
use intent_services::feature_api::Services;

use crate::RemoteServicesExt as _;

/// The remote-launch port of the feature. Without a running connector
/// (no configured node, or the experimental switch off) it behaves like the
/// unavailable port.
#[derive(Default)]
pub struct RemoteLaunch;

/// The checkout a workspace's agents work in: its worktree, or the
/// repository it was opened on.
pub(crate) fn checkout_path(workspace: &Workspace) -> Option<PathBuf> {
    workspace
        .worktree_path
        .as_ref()
        .or(workspace.repository_path.as_ref())
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
}

fn unavailable(placement: &AgentPlacement, availability: PlatformAvailability) -> Error {
    Error::PlacementUnavailable(Box::new(availability.unavailable(placement.clone())))
}

fn no_node() -> Error {
    Error::InvalidParams(
        "node-unavailable: this daemon has no execution node configured for remote agents".into(),
    )
}

async fn workspace_checkout(svc: &Services, workspace_id: &WorkspaceId) -> Result<PathBuf> {
    let workspace = svc.store().get_workspace(workspace_id).await?;
    checkout_path(&workspace).ok_or_else(|| {
        Error::InvalidParams(
            "remote agents need a workspace with a git repository to start from".into(),
        )
    })
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Internal(format!("remote launch task failed: {e}")))?
}

/// The discovery text: the node, its platforms, and how to place an agent.
fn help_text(node: &crate::RemoteNode) -> String {
    let status = node.status();
    let availability = node.availability();
    let list = |platforms: &[intent_core::placement::AgentPlatform]| {
        if platforms.is_empty() {
            "none".to_owned()
        } else {
            platforms
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        }
    };
    let supported = if status.platform.is_some() {
        list(&availability.supported_platforms)
    } else {
        "unknown until the node first connects".to_owned()
    };
    format!(
        "Remote placement (experimental): execution node \"{name}\" (nodeId `{id}`) is {state}; platforms: {supported}; available now: {available} ({running} of {max} agents running).\n\
         To run an agent there, pass `placement: {{ target: \"remote\" }}` (optionally `os`, `arch`, `nodeId`) in the options of `ws.agent.create` or `ws.agent.delegate`, or as `ws.agent.wakeOrCreate(taskNoteId, contextMessage, model, messageMetadata, reasoningEffort, {{ create: {{ placement }} }})`. A placement that no node can take fails with `placement-unavailable`, listing what is available.\n\
         A remote agent starts from the workspace's last commit (uncommitted changes are not carried), runs with the selected provider profile's environment (account folders such as `CODEX_HOME` / `CLAUDE_CONFIG_DIR` must exist on the node, `~` meaning the node's home), or with the node's own provider login when no profile is selected, and pushes its work to `refs/intent/ws/<workspaceId>/agents/<agentId>/head` (commits) and `.../wip` (its working tree). It has no workspace tools yet.",
        name = status.node_name,
        id = status.node_id,
        state = if status.connected { "connected" } else { "offline" },
        available = list(&availability.available_platforms),
        running = node.running(),
        max = status.max_agents,
    )
}

impl RemoteLaunchPort for RemoteLaunch {
    fn placements<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<PlatformAvailability>> {
        Box::pin(async move {
            Ok(svc.remote_node().map_or_else(
                || PlatformAvailability::none(intent_core::now_iso()),
                |node| node.availability(),
            ))
        })
    }

    fn placement_help<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(async move { svc.remote_node().map(|node| help_text(&node)) })
    }

    fn admit<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        placement: &'a AgentPlacement,
    ) -> BoxFuture<'a, Result<RemotePlacementView>> {
        Box::pin(async move {
            let Some(node) = svc.remote_node() else {
                return Err(unavailable(
                    placement,
                    PlatformAvailability::none(intent_core::now_iso()),
                ));
            };
            let status = node.status();
            if placement
                .node_id
                .as_ref()
                .is_some_and(|id| *id != status.node_id)
            {
                // No configured node has that id: nothing matches.
                return Err(unavailable(
                    placement,
                    PlatformAvailability::none(intent_core::now_iso()),
                ));
            }
            let availability = node.availability();
            let platform = availability
                .available_platforms
                .iter()
                .copied()
                .find(|platform| placement.matches_platform(*platform));
            let Some(platform) = platform else {
                return Err(unavailable(placement, availability));
            };
            let checkout = workspace_checkout(svc, workspace_id).await?;
            let base_commit = blocking(move || node_git::head_commit(&checkout)).await?;
            Ok(RemotePlacementView {
                node_id: status.node_id.clone(),
                lease_id: status.lease_id,
                node_name: status.node_name,
                placement: AgentPlacement {
                    target: Some(PlacementTarget::Remote),
                    checkout: Some(PlacementCheckout::Isolated),
                    os: Some(platform.os),
                    arch: Some(platform.arch),
                    node_id: Some(status.node_id),
                    exclusive: None,
                },
                effective_isolation: EffectiveIsolation::Pending,
                placement_error: None,
                base_commit,
            })
        })
    }

    fn launch<'a>(
        &'a self,
        svc: &'a Services,
        request: RemoteLaunchRequest,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let RemoteLaunchRequest {
                workspace_id,
                agent_id,
                view,
            } = request;
            let checkout = workspace_checkout(svc, &workspace_id).await?;
            let remote = AgentRemote::new(workspace_id.as_str(), agent_id.as_str())?;
            let commit = git2::Oid::from_str(&view.base_commit)
                .map_err(|_| Error::Internal("remote launch: invalid base commit".into()))?;
            blocking(move || node_git::write_base_ref(&checkout, &remote, commit)).await?;
            svc.store()
                .record_remote_agent(&RemoteAgentRecord {
                    agent_id,
                    workspace_id,
                    node_id: view.node_id,
                    lease_id: view.lease_id,
                    created_at: intent_core::now_iso(),
                })
                .await
        })
    }

    fn runtime<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<Arc<RemoteRuntime>>> {
        Box::pin(async move {
            let node = svc.remote_node().ok_or_else(no_node)?;
            let record = svc
                .store()
                .get_remote_agent(agent_id)
                .await?
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "node-agent-unplaced: agent {agent_id} was not placed on the execution node"
                    ))
                })?;
            let status = node.status();
            if record.node_id != status.node_id || record.lease_id != status.lease_id {
                return Err(Error::InvalidParams(format!(
                    "node-agent-unplaced: agent {agent_id} runs under a lease this daemon no longer holds"
                )));
            }
            let session = svc.store().get_agent_session(agent_id).await?;
            let view =
                RemotePlacementView::from_metadata(session.metadata.as_ref()).ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "node-agent-unplaced: agent {agent_id} has no remote placement"
                    ))
                })?;
            let provider = session.provider.unwrap_or_default();
            // Resolved at every start, so the node runs with the profile's
            // current values; an unresolvable profile fails the start.
            let profile_env = svc
                .resolve_remote_profile_env(&provider)
                .await?
                .map(|profile| LaunchProfileEnv(profile.to_node_wire()));
            let checkout = workspace_checkout(svc, &record.workspace_id).await?;
            let repo_key = blocking(move || node_git::repo_key(&checkout)).await?;
            let runtime = node
                .launch(LaunchCommand {
                    agent_id: agent_id.clone(),
                    workspace_id: record.workspace_id,
                    provider: intent_providers::base_provider_id(&provider).to_owned(),
                    model: session.model,
                    base_commit: view.base_commit,
                    repo_key,
                    profile_env,
                })
                .await?;
            Ok(runtime as Arc<RemoteRuntime>)
        })
    }
}
