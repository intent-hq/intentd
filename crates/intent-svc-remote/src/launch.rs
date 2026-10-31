//! The remote-launch feature port: admission against the exposed machines,
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

use intent_core::machines::RemoteMachineState;

use crate::RemoteServicesExt as _;

/// The remote-launch port of the feature. With no exposed machine, or
/// `experimental.remoteAgents` off, it behaves like the unavailable port.
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
        "node-unavailable: this daemon has no remote feature for exposed machines".into(),
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

/// The exposed machines' cached platforms: supported once a machine has
/// reported what it runs; available when its last report has a free slot
/// and it is neither revoked nor in need of an update. Nothing while
/// `experimental.remoteAgents` is off.
async fn machines_availability(svc: &Services) -> Result<PlatformAvailability> {
    let now = intent_core::now_iso();
    if !svc.experimental_remote_agents_enabled() {
        return Ok(PlatformAvailability::none(now));
    }
    let mut supported = Vec::new();
    let mut available = Vec::new();
    for row in svc.store().list_exposed_machines().await? {
        let Some(platform) = row.platform else {
            continue;
        };
        if !supported.contains(&platform) {
            supported.push(platform);
        }
        let usable = !matches!(
            row.state,
            RemoteMachineState::Revoked | RemoteMachineState::UpdateRequired
        ) && row
            .capabilities
            .as_ref()
            .is_none_or(|caps| caps.capacity.running_agents < caps.capacity.agent_cap);
        if usable && !available.contains(&platform) {
            available.push(platform);
        }
    }
    Ok(PlatformAvailability::new(supported, available, now))
}

/// The discovery text: the exposed machines, their platforms, and how to
/// place an agent on one.
async fn help_text(svc: &Services) -> Option<String> {
    if !svc.experimental_remote_agents_enabled() {
        return None;
    }
    let rows = svc.store().list_exposed_machines().await.ok()?;
    if rows.is_empty() {
        return None;
    }
    let feature = svc.remote_feature();
    let machines: Vec<String> = rows
        .iter()
        .map(|row| {
            let connected = feature
                .as_ref()
                .and_then(|feature| feature.running_node(&row.id))
                .is_some_and(|node| node.status().connected);
            let platform = row
                .platform
                .map_or_else(|| "unknown platform".to_owned(), |p| p.to_string());
            let state = if connected {
                "connected".to_owned()
            } else {
                row.state.as_str().to_owned()
            };
            format!(
                "\"{}\" (nodeId `{}`, {platform}, {state})",
                row.name, row.id
            )
        })
        .collect();
    Some(format!(
        "Remote placement (experimental): exposed machines: {}.\n\
         To run an agent on one, pass `placement: {{ target: \"remote\", nodeId }}` (or `os` / `arch` instead of `nodeId`) in the options of `ws.agent.create` or `ws.agent.delegate`, or as `ws.agent.wakeOrCreate(taskNoteId, contextMessage, model, messageMetadata, reasoningEffort, {{ create: {{ placement }} }})`. A placement no machine can take fails with `placement-unavailable`, listing what is available.\n\
         A remote agent starts from the workspace's last commit (uncommitted changes are not carried), runs with the selected provider profile's environment (account folders such as `CODEX_HOME` / `CLAUDE_CONFIG_DIR` must exist on the machine, `~` meaning the machine's home), or with the machine's own provider login when no profile is selected, and pushes its work to `refs/intent/ws/<workspaceId>/agents/<agentId>/head` (commits) and `.../wip` (its working tree).",
        machines.join(", ")
    ))
}

/// The exposed machine a placement names, or the first whose cached
/// platform matches it.
async fn choose_machine(
    svc: &Services,
    placement: &AgentPlacement,
) -> Result<Option<intent_core::machines::ExposedMachine>> {
    let rows = svc.store().list_exposed_machines().await?;
    if let Some(node_id) = &placement.node_id {
        return Ok(rows.into_iter().find(|row| row.id == *node_id));
    }
    Ok(rows.into_iter().find(|row| {
        !matches!(
            row.state,
            RemoteMachineState::Revoked | RemoteMachineState::UpdateRequired
        ) && row
            .platform
            .is_some_and(|platform| placement.matches_platform(platform))
    }))
}

impl RemoteLaunchPort for RemoteLaunch {
    fn placements<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<PlatformAvailability>> {
        Box::pin(machines_availability(svc))
    }

    fn placement_help<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(help_text(svc))
    }

    fn admit<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        placement: &'a AgentPlacement,
    ) -> BoxFuture<'a, Result<RemotePlacementView>> {
        Box::pin(async move {
            if let Some(node_id) = &placement.node_id {
                // No exposed machine has that id: nothing matches.
                if svc.store().get_exposed_machine(node_id).await?.is_none() {
                    return Err(unavailable(
                        placement,
                        PlatformAvailability::none(intent_core::now_iso()),
                    ));
                }
            }
            let (Some(feature), Some(row)) = (
                svc.remote_feature()
                    .filter(|_| svc.experimental_remote_agents_enabled()),
                choose_machine(svc, placement).await?,
            ) else {
                return Err(unavailable(placement, machines_availability(svc).await?));
            };
            let node = feature.node(svc, &row.id).await?;
            // Connect on demand; an unreachable machine can take nothing now.
            if node.connect().await.is_err() {
                let cached = machines_availability(svc).await?;
                let offline = PlatformAvailability::new(
                    cached.supported_platforms,
                    Vec::new(),
                    intent_core::now_iso(),
                );
                return Err(unavailable(placement, offline));
            }
            let status = node.status();
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
            let record = svc
                .store()
                .get_remote_agent(agent_id)
                .await?
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "node-agent-unplaced: agent {agent_id} was not placed on the execution node"
                    ))
                })?;
            let feature = svc.remote_feature().ok_or_else(no_node)?;
            // Held until the runtime is attached, so a replacement or removal
            // of the machine either sees this agent or happens first.
            let _lifecycle = feature.lifecycle(&record.node_id).await;
            let node = feature.node(svc, &record.node_id).await?;
            let status = node.status();
            if record.lease_id != status.lease_id {
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
            let provider = session.provider.clone().unwrap_or_default();
            // Resolved at every start, so the node runs with the profile's
            // current values; an unresolvable profile fails the start.
            let profile_env = svc
                .resolve_remote_profile_env(&provider)
                .await?
                .map(|profile| LaunchProfileEnv(profile.to_node_wire()));
            let agent_features = serde_json::to_value(svc.session_agent_features(&session))
                .map_err(|e| Error::Internal(format!("encode the agent's features: {e}")))?;
            let is_sub_agent = session.parent_agent_id.is_some() || session.is_background;
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
                    agent_features,
                    is_sub_agent,
                })
                .await?;
            Ok(runtime as Arc<RemoteRuntime>)
        })
    }

    fn remote_machines<'a>(
        &'a self,
        svc: &'a Services,
        method: &'static str,
        params: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            let feature = svc.remote_feature().ok_or_else(no_node)?;
            crate::methods::call(feature, svc, method, params).await
        })
    }
}
