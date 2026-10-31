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

use intent_core::machine_selection::{
    exposed_machines_help, select_machines, PlacementMachines, PlacementRequest, RefreshFailure,
};
use intent_core::machines::ExposedMachine;
use intent_core::node_link::MachineCapabilities;
use intent_core::node_link::{LaunchCommand, LaunchProfileEnv};
use intent_core::placement::{
    AgentPlacement, MachineRef, PlacementCheckout, PlacementTarget, PlacementUnavailable,
    PlacementUnavailableReason, PlatformAvailability,
};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteAgentRecord, RemoteLaunchNeeds, RemoteLaunchRequest,
    RemotePlacementView,
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

/// The exposed machines as placement sees them: the stored rows with
/// their last reports, refreshed on demand over the machine's link.
struct ExposedMachines<'a> {
    svc: &'a Services,
    feature: &'a crate::RemoteFeature,
}

/// What a failed refresh means for placement: a version gap, a revoked
/// credential, a report that did not come in time over a live link, or
/// anything else (no route, a closed link) as unreachable.
fn refresh_failure(error: Error) -> RefreshFailure {
    match error {
        Error::MachineUpdateRequired(update) => RefreshFailure::UpdateRequired(update),
        ref timeout
            if intent_transport::node_link::error_code(timeout)
                == Some(intent_transport::node_link::codes::TIMEOUT) =>
        {
            RefreshFailure::Slow
        }
        Error::RemoteMachine(failure)
            if failure.data.get("code").and_then(serde_json::Value::as_str)
                == Some(crate::connect::MACHINE_REVOKED) =>
        {
            RefreshFailure::Revoked
        }
        _ => RefreshFailure::Unreachable,
    }
}

impl PlacementMachines for ExposedMachines<'_> {
    /// The stored rows, each report's running count raised to the agents
    /// this backend has attached there since (the report can be up to an
    /// hour old).
    fn machines(&self) -> BoxFuture<'_, Result<Vec<ExposedMachine>>> {
        Box::pin(async move {
            let mut rows = self.svc.store().list_exposed_machines().await?;
            for row in &mut rows {
                let attached = self
                    .feature
                    .running_node(&row.id)
                    .map_or(0, |node| u32::try_from(node.running()).unwrap_or(u32::MAX));
                if let Some(capabilities) = row.capabilities.as_mut() {
                    let running = &mut capabilities.capacity.running_agents;
                    *running = (*running).max(attached);
                }
            }
            Ok(rows)
        })
    }

    fn refresh<'a>(
        &'a self,
        machine_id: &'a str,
    ) -> BoxFuture<'a, std::result::Result<MachineCapabilities, RefreshFailure>> {
        Box::pin(async move {
            let node = self
                .feature
                .node(self.svc, machine_id)
                .await
                .map_err(refresh_failure)?;
            node.refresh_capabilities().await.map_err(refresh_failure)
        })
    }
}

/// The discovery text: the exposed machines with their providers, models
/// and sign-in, and how to place an agent on one.
async fn help_text(svc: &Services) -> Option<String> {
    if !svc.experimental_remote_agents_enabled() {
        return None;
    }
    let rows = svc.store().list_exposed_machines().await.ok()?;
    let feature = svc.remote_feature();
    exposed_machines_help(&rows, &|id| {
        feature
            .and_then(|feature| feature.running_node(id))
            .is_some_and(|node| node.status().connected)
    })
}

/// The refusal when no chosen machine could be reached at admission: its
/// version gap or revocation for a single candidate, else
/// `machine-unreachable`.
async fn connect_failure(
    svc: &Services,
    placement: &AgentPlacement,
    mut failures: Vec<(ExposedMachine, Error)>,
) -> Result<Error> {
    let availability = machines_availability(svc).await?;
    let offline = PlatformAvailability::new(
        availability.supported_platforms,
        Vec::new(),
        intent_core::now_iso(),
    );
    if failures.len() == 1 {
        let (row, error) = failures.remove(0);
        let reason = match refresh_failure(error) {
            RefreshFailure::UpdateRequired(update) => {
                return Ok(Error::MachineUpdateRequired(update));
            }
            RefreshFailure::Revoked => PlacementUnavailableReason::MachineRevoked,
            RefreshFailure::Unreachable | RefreshFailure::Slow => {
                PlacementUnavailableReason::MachineUnreachable
            }
        };
        return Ok(Error::PlacementUnavailable(Box::new(
            offline.machine_unavailable(
                placement.clone(),
                reason,
                MachineRef {
                    node_id: row.id,
                    name: row.name,
                },
                None,
            ),
        )));
    }
    let failure = offline.unavailable(placement.clone());
    Ok(Error::PlacementUnavailable(Box::new(
        PlacementUnavailable {
            reason: PlacementUnavailableReason::MachineUnreachable,
            ..failure
        },
    )))
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
        needs: &'a RemoteLaunchNeeds,
    ) -> BoxFuture<'a, Result<RemotePlacementView>> {
        Box::pin(async move {
            let Some(feature) = svc
                .remote_feature()
                .filter(|_| svc.experimental_remote_agents_enabled())
            else {
                return Err(unavailable(
                    placement,
                    PlatformAvailability::none(intent_core::now_iso()),
                ));
            };
            let source = ExposedMachines { svc, feature };
            let request = PlacementRequest {
                placement,
                provider: &needs.provider,
                model: needs.model.as_deref(),
                with_profile: needs.with_profile,
            };
            let candidates =
                select_machines(&source, request, time::OffsetDateTime::now_utc()).await?;
            // Connect on demand, best candidate first.
            let mut failures = Vec::new();
            let mut chosen = None;
            for row in candidates {
                let node = feature.node(svc, &row.id).await?;
                match node.connect().await {
                    Ok(()) => {
                        chosen = Some((row, node));
                        break;
                    }
                    Err(error) => failures.push((row, error)),
                }
            }
            let Some((row, node)) = chosen else {
                return Err(connect_failure(svc, placement, failures).await?);
            };
            let status = node.status();
            let Some(platform) = status.platform.or(row.platform) else {
                return Err(unavailable(placement, node.availability()));
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

    fn manages_machines(&self) -> bool {
        true
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
            let node = feature.node_locked(svc, &record.node_id).await?;
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
