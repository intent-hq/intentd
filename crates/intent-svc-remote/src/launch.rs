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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use intent_core::machine_selection::{
    exposed_machines_help, select_machines, PlacementMachines, PlacementRequest, RefreshFailure,
};
use intent_core::machines::ExposedMachine;
use intent_core::node_link::MachineCapabilities;
use intent_core::node_link::{
    LaunchCommand, LaunchProfileEnv, PreparedCheckout, SubmodulePin, PREPARED_CHECKOUT_PROTOCOL,
};
use intent_core::placement::{
    AgentPlacement, MachineRef, PlacementCheckout, PlacementTarget, PlacementUnavailable,
    PlacementUnavailableReason, PlatformAvailability,
};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteAgentRecord, RemoteCheckoutBase, RemoteLaunchNeeds,
    RemoteLaunchRequest, RemotePlacementView,
};
use intent_core::{AgentId, BoxFuture, Error, Result, Workspace, WorkspaceId};
use intent_git::node_git::{self, AgentRemote};
use intent_services::feature_api::feature_ports::{RemoteLaunchPort, RemoteRuntime};
use intent_services::feature_api::Services;

use intent_core::machines::{MachineUpdateRequired, RemoteMachineState};

use crate::connector::RemoteNode;
use crate::RemoteServicesExt as _;

/// The remote-launch port of the feature. With no exposed machine it
/// behaves like the unavailable port.
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

/// Point the agent's `base` ref in submodule `pin`'s repository at its
/// pinned commit, after checking it is still that submodule of `checkout`.
fn write_module_base_ref(checkout: &Path, remote: &AgentRemote, pin: &SubmodulePin) -> Result<()> {
    let module =
        node_git::module_repository(checkout, &pin.path, &pin.module_key).map_err(|refusal| {
            Error::Internal(format!(
                "remote launch: submodule {} is not the pinned repository: {refusal:?}",
                pin.path
            ))
        })?;
    let commit = git2::Oid::from_str(&pin.commit).map_err(|_| {
        Error::Internal(format!(
            "remote launch: invalid commit for submodule {}",
            pin.path
        ))
    })?;
    node_git::write_base_ref(&module.worktree, remote, commit)
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

/// Whether `workspace`'s agents start from a checkout a machine must
/// prepare: populated submodules, or a setup script (resolved as a local
/// create resolves it, without the create's execute-only parameter).
async fn needs_prepared_checkout(workspace: &Workspace, submodules: &[SubmodulePin]) -> bool {
    if !submodules.is_empty() {
        return true;
    }
    if workspace.skip_worktree || workspace.plain_folder {
        return false;
    }
    let Some(checkout) = checkout_path(workspace) else {
        return false;
    };
    let config = intent_services::feature_api::repo_config::read_repo_config(&checkout).await;
    config.setup_script.is_some_and(|script| !script.is_empty())
        || workspace
            .setup_script
            .as_ref()
            .is_some_and(|setup| !setup.script.is_empty())
}

/// The protocol of `node`'s link, connecting first when none is up.
async fn link_protocol(node: &RemoteNode) -> Result<u32> {
    if node.shared_ref().current_link().is_none() {
        node.connect().await?;
    }
    Ok(node.shared_ref().protocol())
}

/// Refuse a checkout that needs preparing on a machine whose link is older
/// than [`PREPARED_CHECKOUT_PROTOCOL`]: it would start the agent with empty
/// submodules and without the setup script.
/// Nothing is refused while this build does not offer the prepared checkout.
fn require_prepared_checkout(node: &RemoteNode, protocol: u32) -> Result<()> {
    if protocol >= PREPARED_CHECKOUT_PROTOCOL || !node.shared_ref().offers_prepared_checkout() {
        return Ok(());
    }
    let status = node.status();
    Err(Error::MachineUpdateRequired(Box::new(
        MachineUpdateRequired::prepared_checkout(
            status.node_name,
            status
                .intentd_version
                .unwrap_or_else(|| "an older version".into()),
        ),
    )))
}

/// [`require_prepared_checkout`] for an agent of `workspace_id` placed with
/// `view`, when its checkout needs preparing.
async fn check_machine_prepares(
    svc: &Services,
    workspace_id: &WorkspaceId,
    view: &RemotePlacementView,
) -> Result<()> {
    let feature = svc.remote_feature().ok_or_else(no_node)?;
    let node = feature.node(svc, &view.node_id).await?;
    if !node.shared_ref().offers_prepared_checkout() {
        return Ok(());
    }
    let workspace = svc.store().get_workspace(workspace_id).await?;
    if !needs_prepared_checkout(&workspace, view.pins()).await {
        return Ok(());
    }
    require_prepared_checkout(&node, link_protocol(&node).await?)
}

/// Record a start refused for `error` as the agent's `placementError`.
async fn record_refused_start(
    svc: &Services,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    error: &Error,
) {
    let detail = match error {
        Error::MachineUpdateRequired(update) => update.message(),
        other => other.to_string(),
    };
    let placement_error = intent_core::remote_launch::PlacementError::new(
        intent_core::machines::MACHINE_UPDATE_REQUIRED_CODE,
        &detail,
    );
    match crate::view::update(svc.store(), workspace_id, agent_id, |view| {
        view.placement_error = Some(placement_error);
    })
    .await
    {
        Ok(true) => {
            svc.publish_agent_placement_changed(workspace_id, agent_id)
                .await
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(agent = %agent_id, %error, "remote launch: could not record the refused start");
        }
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Internal(format!("remote launch task failed: {e}")))?
}

/// The exposed machines' cached platforms: supported once a machine has
/// reported what it runs; available when its last report has a free slot
/// and it is neither revoked nor in need of an update.
async fn machines_availability(svc: &Services) -> Result<PlatformAvailability> {
    let now = intent_core::now_iso();
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
/// and sign-in, and how to place an agent on one. `None` while no machine
/// is exposed to this backend, so the model sees no remote-placement text.
async fn help_text(svc: &Services) -> Option<String> {
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
            let mut view = self.admit_new_workspace(svc, placement, needs).await?;
            let base = self.checkout_base(svc, workspace_id).await?;
            view.base_commit = base.commit;
            view.submodules = Some(base.submodules);
            check_machine_prepares(svc, workspace_id, &view).await?;
            Ok(view)
        })
    }

    fn admit_new_workspace<'a>(
        &'a self,
        svc: &'a Services,
        placement: &'a AgentPlacement,
        needs: &'a RemoteLaunchNeeds,
    ) -> BoxFuture<'a, Result<RemotePlacementView>> {
        Box::pin(async move {
            let Some(feature) = svc.remote_feature() else {
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
                base_commit: String::new(),
                submodules: None,
            })
        })
    }

    fn checkout_base<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, Result<RemoteCheckoutBase>> {
        Box::pin(async move {
            let checkout = workspace_checkout(svc, workspace_id).await?;
            blocking(move || {
                Ok(RemoteCheckoutBase {
                    commit: node_git::head_commit(&checkout)?,
                    submodules: node_git::submodule_pins(&checkout)?,
                })
            })
            .await
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
            check_machine_prepares(svc, &workspace_id, &view).await?;
            let checkout = workspace_checkout(svc, &workspace_id).await?;
            let remote = AgentRemote::new(workspace_id.as_str(), agent_id.as_str())?;
            let commit = git2::Oid::from_str(&view.base_commit)
                .map_err(|_| Error::Internal("remote launch: invalid base commit".into()))?;
            let submodules = view.pins().to_vec();
            blocking(move || {
                node_git::write_base_ref(&checkout, &remote, commit)?;
                for pin in &submodules {
                    write_module_base_ref(&checkout, &remote, pin)?;
                }
                Ok(())
            })
            .await?;
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

    fn final_stop<'a>(
        &'a self,
        svc: &'a Services,
        agent_ids: &'a [AgentId],
        reason: intent_core::remote_launch::FinalStopReason,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let Some(feature) = svc.remote_feature() else {
                return;
            };
            // One read for the whole list, so deleting a workspace of local
            // agents costs one statement, not one per agent.
            let records = match svc.store().get_remote_agents(agent_ids).await {
                Ok(records) => records,
                Err(error) => {
                    tracing::warn!(%error, "remote agents: could not read their placements; no final stops");
                    return;
                }
            };
            for record in records {
                let agent_id = &record.agent_id;
                let node = match feature.node(svc, &record.node_id).await {
                    Ok(node) => node,
                    Err(error) => {
                        tracing::warn!(agent = %agent_id, machine = %record.node_id, %error, "remote agent: its machine is unavailable; no final stop");
                        continue;
                    }
                };
                if node.status().lease_id != record.lease_id {
                    tracing::info!(agent = %agent_id, "remote agent: placed under an ended lease; no final stop");
                    continue;
                }
                node.final_stop(agent_id, reason).await;
            }
        })
    }

    fn machine_script_link<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<intent_services::feature_api::feature_ports::MachineScriptLink>> {
        Box::pin(async move {
            let record = svc
                .store()
                .get_remote_agent(agent_id)
                .await?
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "node-agent-unplaced: agent {agent_id} was not placed on a machine"
                    ))
                })?;
            let feature = svc.remote_feature().ok_or_else(no_node)?;
            let node = feature.node(svc, &record.node_id).await?;
            node.connect().await?;
            let link = node.shared().current_link().ok_or_else(|| {
                Error::Internal("machine-unreachable: the machine's link closed".into())
            })?;
            let status = node.status();
            let sender = link.clone();
            Ok(
                intent_services::feature_api::feature_ports::MachineScriptLink {
                    node_id: record.node_id.clone(),
                    protocol: link.protocol(),
                    machine_name: status.node_name.clone(),
                    machine_version: status.intentd_version.clone().unwrap_or_default(),
                    send: Arc::new(move |frame| {
                        let link = sender.clone();
                        Box::pin(async move { link.send(&frame).await })
                    }),
                },
            )
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
            let session = svc.store().get_agent_session_summary(agent_id).await?;
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
            // A prepared agent (its pins captured, possibly none) gets
            // `checkout` on every protocol 10 launch, and never starts below
            // 10 when its workspace needs a prepared checkout. An agent
            // placed before pins were captured starts as before.
            let checkout = if view.submodules.is_some() {
                let protocol = link_protocol(&node).await?;
                let workspace = svc.store().get_workspace(&record.workspace_id).await?;
                if needs_prepared_checkout(&workspace, view.pins()).await {
                    if let Err(error) = require_prepared_checkout(&node, protocol) {
                        record_refused_start(svc, &record.workspace_id, agent_id, &error).await;
                        return Err(error);
                    }
                }
                (protocol >= PREPARED_CHECKOUT_PROTOCOL).then(|| PreparedCheckout {
                    submodules: view.pins().to_vec(),
                    setup: None,
                })
            } else {
                None
            };
            let checkout_dir = workspace_checkout(svc, &record.workspace_id).await?;
            let repo_key = blocking(move || node_git::repo_key(&checkout_dir)).await?;
            let runtime = node
                .launch(LaunchCommand {
                    agent_id: agent_id.clone(),
                    workspace_id: record.workspace_id,
                    provider: intent_providers::base_provider_id(&provider).to_owned(),
                    model: session.model.clone(),
                    base_commit: view.base_commit,
                    repo_key,
                    profile_env,
                    agent_features,
                    is_sub_agent,
                    checkout,
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
