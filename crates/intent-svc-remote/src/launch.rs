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
    FinalPush, ForgeLaunch, GitIdentity, LaunchCommand, LaunchProfileEnv, PreparedCheckout,
    SetupCommand, SubmodulePin, MCP_PROXY_PROTOCOL, PREPARED_CHECKOUT_PROTOCOL,
    SHARED_CHECKOUT_PROTOCOL,
};
use intent_core::placement::{
    AgentPlacement, MachineRef, PlacementCheckout, PlacementTarget, PlacementUnavailable,
    PlacementUnavailableReason, PlatformAvailability,
};
use intent_core::remote_launch::{
    BackendParentModule, BackendParentWork, EffectiveIsolation, ParentWork, ParentWorkCopy,
    RemoteAgentRecord, RemoteCheckoutBase, RemoteLaunchNeeds, RemoteLaunchRequest,
    RemotePlacementView, RemoteSetupStatus,
};
use intent_core::{AgentId, BoxFuture, Error, Result, Workspace, WorkspaceId};
use intent_git::hub::AgentRef;
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

/// The `forge` member a top-level agent of `workspace_id` would carry
/// (nodeProtocol 15): the workspace branch and its checkout's `origin`,
/// normalized to HTTPS GitHub; `None` when that `origin` is not on GitHub.
/// The connector drops it unless the machine holds a token.
async fn forge_launch(svc: &Services, workspace_id: &WorkspaceId) -> Option<Box<ForgeLaunch>> {
    let workspace = svc.store().get_workspace(workspace_id).await.ok()?;
    let checkout = checkout_path(&workspace)?;
    let origin = blocking(move || intent_git::remote::origin_url(&checkout))
        .await
        .ok()??;
    ForgeLaunch::for_github(&origin, &workspace.branch).map(Box::new)
}

/// The `launch.gitIdentity` for a workspace whose checkout is `checkout`:
/// the identity a local agent's spawn there would get, or `None`.
async fn git_identity(checkout: PathBuf) -> Option<Box<GitIdentity>> {
    let (name, email) = blocking(move || Ok(intent_git::identity::commit_identity(&checkout)))
        .await
        .ok()??;
    Some(Box::new(GitIdentity { name, email }))
}

/// Whether an agent with `submodules` and `setup` starts from a checkout a
/// machine must prepare.
fn needs_prepared_checkout(submodules: &[SubmodulePin], setup: Option<&SetupCommand>) -> bool {
    !submodules.is_empty() || setup.is_some()
}

/// The protocol of `node`'s link, connecting first when none is up.
async fn link_protocol(node: &RemoteNode) -> Result<u32> {
    if node.shared_ref().current_link().is_none() {
        node.connect().await?;
    }
    Ok(node.shared_ref().protocol())
}

/// What a remote agent's instructions say when its machine's link is older
/// than [`MCP_PROXY_PROTOCOL`] and it would have had user MCP servers.
pub(crate) fn mcp_servers_notice(machine: &str) -> String {
    format!("Your MCP servers are not available on {machine} until Intent there is updated.")
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

/// [`require_prepared_checkout`] for an agent placed with `view`, when its
/// checkout needs preparing.
async fn check_machine_prepares(
    svc: &Services,
    workspace_id: &WorkspaceId,
    view: &RemotePlacementView,
) -> Result<()> {
    // A cloud agent whose sandbox is not started yet: its machine is
    // checked when it links.
    if view.is_unlinked() {
        return Ok(());
    }
    let feature = svc.remote_feature().ok_or_else(no_node)?;
    let node = feature.node(svc, &view.node_id).await?;
    if !node.shared_ref().offers_prepared_checkout() {
        return Ok(());
    }
    let setup = svc.remote_setup_command(workspace_id, view).await?;
    if !needs_prepared_checkout(view.pins(), setup.as_ref()) {
        return Ok(());
    }
    require_prepared_checkout(&node, link_protocol(&node).await?)
}

/// How long the backend waits for a remote agent's machine to answer a
/// secret frame, connecting included (node-link.md § Secrets on the
/// machine).
const MACHINE_SECRET_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long the backend waits for a parent's machine to push before a
/// sub-agent on another machine starts from the parent's last push.
const PARENT_PUSH_WAIT: std::time::Duration = std::time::Duration::from_secs(75);

/// Ask `owner`'s machine to push its checkout now (`pushWip`, nodeProtocol
/// 13); whether it did.
async fn push_parent_now(svc: &Services, owner: &AgentId) -> bool {
    let Some(feature) = svc.remote_feature() else {
        return false;
    };
    let record = match svc.store().get_remote_agent(owner).await {
        Ok(Some(record)) => record,
        Ok(None) => return false,
        Err(error) => {
            tracing::warn!(agent = %owner, %error, "remote launch: could not read the parent's placement");
            return false;
        }
    };
    let Ok(node) = feature.node(svc, &record.node_id).await else {
        return false;
    };
    if node.status().lease_id != record.lease_id {
        return false;
    }
    match link_protocol(&node).await {
        Ok(protocol) if protocol >= SHARED_CHECKOUT_PROTOCOL => {}
        _ => return false,
    }
    match node.push_wip(owner, PARENT_PUSH_WAIT).await {
        Ok(reply) if reply.outcome == FinalPush::Pushed => true,
        Ok(reply) => {
            tracing::info!(agent = %owner, outcome = ?reply.outcome, error = ?reply.error, "remote launch: the parent's machine did not push its work now");
            false
        }
        Err(error) => {
            tracing::warn!(agent = %owner, %error, "remote launch: the parent's machine did not answer the push request");
            false
        }
    }
}

/// Start sub-agent `agent_id` from its parent `owner`'s pushed work: its
/// `base` is the parent's `head`, its `wip` the parent's `wip`, in the
/// superproject and each pinned submodule (nodeProtocol 13). The parent's
/// machine is asked to push first. Without a parent `head` the view is left
/// as admitted (the workspace's last commit).
async fn copy_parent_work(
    svc: &Services,
    workspace_id: &WorkspaceId,
    (checkout, agent_id): (&Path, &AgentId),
    owner: AgentId,
    view: &mut RemotePlacementView,
) -> Result<()> {
    let pushed = push_parent_now(svc, &owner).await;
    let parent = AgentRemote::new(workspace_id.as_str(), owner.as_str())?;
    let child = AgentRemote::new(workspace_id.as_str(), agent_id.as_str())?;
    let pins = view.pins().to_vec();
    let checkout = checkout.to_path_buf();
    let copied = blocking(move || {
        let Some(head) = node_git::read_agent_ref(&checkout, &parent, AgentRef::Head)? else {
            return Ok(None);
        };
        if let Some(wip) =
            node_git::read_agent_ref(&checkout, &parent, AgentRef::Wip)?.filter(|wip| *wip != head)
        {
            node_git::write_agent_ref(&checkout, &child, AgentRef::Wip, wip)?;
        }
        let mut copied_pins = Vec::with_capacity(pins.len());
        for mut pin in pins {
            let module = node_git::module_repository(&checkout, &pin.path, &pin.module_key)
                .map_err(|refusal| {
                    Error::Internal(format!(
                        "remote launch: submodule {} is not the pinned repository: {refusal:?}",
                        pin.path
                    ))
                })?;
            let parent_module = parent.with_module(&pin.module_key)?;
            if let Some(module_head) =
                node_git::read_agent_ref(&module.worktree, &parent_module, AgentRef::Head)?
            {
                pin.commit = module_head.to_string();
                if let Some(wip) =
                    node_git::read_agent_ref(&module.worktree, &parent_module, AgentRef::Wip)?
                        .filter(|wip| *wip != module_head)
                {
                    let child_module = child.with_module(&pin.module_key)?;
                    node_git::write_agent_ref(&module.worktree, &child_module, AgentRef::Wip, wip)?;
                }
            }
            copied_pins.push(pin);
        }
        Ok(Some((head.to_string(), copied_pins)))
    })
    .await?;
    let Some((head, pins)) = copied else {
        return Ok(());
    };
    view.base_commit = head;
    if view.submodules.is_some() {
        view.submodules = Some(pins);
    }
    view.parent_work = Some(ParentWork {
        agent_id: owner,
        copied: Some(if pushed {
            ParentWorkCopy::Pushed
        } else {
            ParentWorkCopy::LastPush
        }),
    });
    let stored = view.clone();
    crate::view::update(svc.store(), workspace_id, agent_id, move |current| {
        *current = stored;
    })
    .await?;
    Ok(())
}

/// An agent's first prepared launch: its machine is preparing the checkout.
/// Later launches keep the last result until the machine reports a new one
/// (a kept checkout reports none).
async fn record_setup_pending(svc: &Services, workspace_id: &WorkspaceId, agent_id: &AgentId) {
    match crate::view::update(svc.store(), workspace_id, agent_id, |view| {
        view.setup.get_or_insert_with(RemoteSetupStatus::pending);
    })
    .await
    {
        Ok(true) => {
            svc.publish_agent_placement_changed(workspace_id, agent_id)
                .await;
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(agent = %agent_id, %error, "remote launch: could not record the pending setup");
        }
    }
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
                .await;
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

/// The checkout a workspace's remote agents start from: its `HEAD` commit
/// and populated submodules.
pub(crate) async fn checkout_base(
    svc: &Services,
    workspace_id: &WorkspaceId,
) -> Result<RemoteCheckoutBase> {
    let checkout = workspace_checkout(svc, workspace_id).await?;
    blocking(move || {
        Ok(RemoteCheckoutBase {
            commit: node_git::head_commit(&checkout)?,
            submodules: node_git::submodule_pins(&checkout)?,
        })
    })
    .await
}

/// Place an agent admitted on a machine there: its `base` refs and its
/// record on the machine's lease.
pub(crate) async fn launch_linked(svc: &Services, request: RemoteLaunchRequest) -> Result<()> {
    let RemoteLaunchRequest {
        workspace_id,
        agent_id,
        mut view,
    } = request;
    check_machine_prepares(svc, &workspace_id, &view).await?;
    let checkout = workspace_checkout(svc, &workspace_id).await?;
    if let Some(owner) = view
        .parent_work
        .as_ref()
        .filter(|work| work.copied.is_none())
        .map(|work| work.agent_id.clone())
    {
        copy_parent_work(svc, &workspace_id, (&checkout, &agent_id), owner, &mut view).await?;
    }
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
}

impl RemoteLaunchPort for RemoteLaunch {
    fn placements<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<PlatformAvailability>> {
        Box::pin(machines_availability(svc))
    }

    fn placement_help<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(help_text(svc))
    }

    fn mcp_servers_notice<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Option<String>> {
        Box::pin(async move {
            let record = svc.store().get_remote_agent(agent_id).await.ok()??;
            let node = svc
                .remote_feature()?
                .node(svc, &record.node_id)
                .await
                .ok()?;
            let protocol = link_protocol(&node).await.ok()?;
            if protocol >= MCP_PROXY_PROTOCOL
                || svc
                    .remote_mcp_proxies(&record.workspace_id)
                    .await
                    .is_empty()
            {
                return None;
            }
            Some(mcp_servers_notice(&node.status().node_name))
        })
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
            if let Some(view) = crate::cloud::admit(feature, svc, placement, needs).await? {
                return Ok(view);
            }
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
                    cloud_machine_id: None,
                },
                effective_isolation: EffectiveIsolation::Pending,
                placement_error: None,
                base_commit: String::new(),
                submodules: None,
                setup: None,
                setup_script: None,
                skills: None,
                shared_checkout_of: None,
                parent_work: None,
                cloud_start: None,
            })
        })
    }

    fn checkout_base<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, Result<RemoteCheckoutBase>> {
        Box::pin(checkout_base(svc, workspace_id))
    }

    fn manages_machines(&self) -> bool {
        true
    }

    fn place_sub_agent<'a>(
        &'a self,
        svc: &'a Services,
        (parent, parent_view): (&'a AgentId, &'a RemotePlacementView),
        child: &'a mut RemotePlacementView,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // A parent that shares a checkout passes on that checkout's owner.
            let owner = parent_view
                .shared_checkout_of
                .clone()
                .unwrap_or_else(|| parent.clone());
            // A child waiting for a new sandbox never shares, even while
            // its parent's own sandbox is still starting (both unlinked).
            if child.is_unlinked() || child.node_id != parent_view.node_id {
                child.parent_work = Some(ParentWork {
                    agent_id: owner,
                    copied: None,
                });
                return Ok(());
            }
            let feature = svc.remote_feature().ok_or_else(no_node)?;
            let node = feature.node(svc, &child.node_id).await?;
            if link_protocol(&node).await? >= SHARED_CHECKOUT_PROTOCOL {
                child.shared_checkout_of = Some(owner);
            }
            Ok(())
        })
    }

    fn backend_parent_work<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        (parent, parent_view): (&'a AgentId, &'a RemotePlacementView),
    ) -> BoxFuture<'a, Result<Option<BackendParentWork>>> {
        Box::pin(async move {
            let owner = parent_view
                .shared_checkout_of
                .clone()
                .unwrap_or_else(|| parent.clone());
            let pushed = push_parent_now(svc, &owner).await;
            let checkout = workspace_checkout(svc, workspace_id).await?;
            let remote = AgentRemote::new(workspace_id.as_str(), owner.as_str())?;
            let pins = parent_view.pins().to_vec();
            let (head, wip, submodules) = blocking(move || {
                let head = node_git::read_agent_ref(&checkout, &remote, AgentRef::Head)?;
                let wip = node_git::read_agent_ref(&checkout, &remote, AgentRef::Wip)?
                    .filter(|wip| Some(*wip) != head);
                let mut submodules = Vec::new();
                for pin in pins {
                    let Ok(module) =
                        node_git::module_repository(&checkout, &pin.path, &pin.module_key)
                    else {
                        continue;
                    };
                    let module_remote = remote.with_module(&pin.module_key)?;
                    let Some(module_head) =
                        node_git::read_agent_ref(&module.worktree, &module_remote, AgentRef::Head)?
                    else {
                        continue;
                    };
                    let module_wip =
                        node_git::read_agent_ref(&module.worktree, &module_remote, AgentRef::Wip)?
                            .filter(|wip| *wip != module_head);
                    submodules.push(BackendParentModule {
                        path: pin.path,
                        head: module_head.to_string(),
                        wip: module_wip.map(|wip| wip.to_string()),
                    });
                }
                Ok((
                    head.map(|head| head.to_string()),
                    wip.map(|wip| wip.to_string()),
                    submodules,
                ))
            })
            .await?;
            Ok(Some(BackendParentWork {
                agent_id: owner,
                head,
                wip,
                submodules,
                copied: if pushed {
                    ParentWorkCopy::Pushed
                } else {
                    ParentWorkCopy::LastPush
                },
                seeded: false,
            }))
        })
    }

    fn setup_output(
        &self,
        svc: &Services,
        node_id: &str,
        agent_id: &AgentId,
    ) -> Option<(String, bool)> {
        svc.remote_feature()?
            .running_node(node_id)?
            .shared_ref()
            .setup_output_tail(agent_id)
    }

    fn launch<'a>(
        &'a self,
        svc: &'a Services,
        request: RemoteLaunchRequest,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // A new cloud agent: its sandbox starts after the reply, and
            // its first turn waits for it.
            if request.view.is_cloud() && request.view.is_unlinked() {
                crate::cloud::begin_start(
                    svc,
                    &request.workspace_id,
                    &request.agent_id,
                    request.view.placement_error.as_ref(),
                )
                .await;
                return Ok(());
            }
            launch_linked(svc, request).await
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
                // An agent that retires itself during a turn finishes that
                // turn first, as a local agent does: stopping it now would
                // fail the turn that asked for the stop.
                let own_turn = reason == intent_core::remote_launch::FinalStopReason::AgentRetired
                    && matches!(
                        intent_core::current_caller(),
                        Some(intent_core::Caller::Agent { agent_id: caller }) if caller == *agent_id
                    );
                if own_turn {
                    let svc = svc.clone();
                    let agent_id = agent_id.clone();
                    let node_id = record.node_id.clone();
                    let lease_id = record.lease_id.clone();
                    intent_core::spawn_daemon(async move {
                        if node
                            .await_turn_before_final_stop(&agent_id, SELF_RETIRE_TURN_BOUND)
                            .await
                            && node.status().lease_id == lease_id
                        {
                            final_stop_placed(&svc, &node, &agent_id, &node_id, reason).await;
                        }
                    });
                    continue;
                }
                final_stop_placed(svc, &node, agent_id, &record.node_id, reason).await;
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

    fn machine_skills<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        query: bool,
    ) -> BoxFuture<'a, intent_services::feature_api::feature_ports::MachineSkills> {
        use intent_services::feature_api::feature_ports::MachineSkills;
        Box::pin(async move {
            let Ok(Some(record)) = svc.store().get_remote_agent(agent_id).await else {
                return MachineSkills::Offline;
            };
            let Some(feature) = svc.remote_feature() else {
                return MachineSkills::Offline;
            };
            match feature.node(svc, &record.node_id).await {
                Ok(node) => node.machine_skills(query).await,
                Err(_) => MachineSkills::Offline,
            }
        })
    }

    fn machine_secret<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        op: intent_services::feature_api::feature_ports::MachineSecretOp,
    ) -> BoxFuture<'a, intent_services::feature_api::feature_ports::MachineSecretAnswer> {
        use intent_services::feature_api::feature_ports::{MachineSecretAnswer, MachineSecretOp};
        Box::pin(async move {
            let forget = match &op {
                MachineSecretOp::Forget { name } => Some(name.clone()),
                MachineSecretOp::Put { .. } | MachineSecretOp::List => None,
            };
            let node = match svc.store().get_remote_agent(agent_id).await {
                Ok(Some(record)) => match svc.remote_feature() {
                    Some(feature) => match feature.node(svc, &record.node_id).await {
                        Ok(node) if node.status().lease_id == record.lease_id => Some(node),
                        _ => None,
                    },
                    None => None,
                },
                _ => None,
            };
            match node {
                Some(node) => node.machine_secret(agent_id, op, MACHINE_SECRET_WAIT).await,
                // No machine to reach: a forget has nothing left to drop.
                None if forget.is_some() => MachineSecretAnswer::Forgotten(false),
                None => MachineSecretAnswer::Offline,
            }
        })
    }

    fn runtime<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<Arc<RemoteRuntime>>> {
        Box::pin(async move {
            let mut record = svc.store().get_remote_agent(agent_id).await?;
            // A cloud agent whose sandbox is starting (or gone) waits for
            // it; a message sent meanwhile runs once it is linked.
            if record.is_none() {
                let session = svc.store().get_agent_session_summary(agent_id).await?;
                if RemotePlacementView::from_metadata(session.metadata.as_ref())
                    .is_some_and(|view| view.is_cloud())
                {
                    crate::cloud::wait_linked(svc, agent_id).await?;
                    record = svc.store().get_remote_agent(agent_id).await?;
                }
            }
            let record = record.ok_or_else(|| {
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
            // placed before pins were captured, or any agent while this
            // build does not offer the prepared checkout, starts as before.
            // A sub-agent sharing its parent's checkout (nodeProtocol 13)
            // gets no checkout of its own.
            let checkout_of = view.shared_checkout_of.clone();
            if checkout_of.is_some() {
                let protocol = link_protocol(&node).await?;
                if protocol < SHARED_CHECKOUT_PROTOCOL {
                    return Err(Error::InvalidParams(format!(
                        "node-link-protocol: \"{}\" speaks nodeProtocol {protocol}; this sub-agent works in its parent's checkout there, which needs {SHARED_CHECKOUT_PROTOCOL}",
                        view.node_name
                    )));
                }
            }
            let checkout = if checkout_of.is_some() {
                None
            } else if view.submodules.is_some() && node.shared_ref().offers_prepared_checkout() {
                let protocol = link_protocol(&node).await?;
                let setup = svc
                    .remote_setup_command(&record.workspace_id, &view)
                    .await?;
                if needs_prepared_checkout(view.pins(), setup.as_ref()) {
                    if let Err(error) = require_prepared_checkout(&node, protocol) {
                        record_refused_start(svc, &record.workspace_id, agent_id, &error).await;
                        return Err(error);
                    }
                }
                (protocol >= PREPARED_CHECKOUT_PROTOCOL).then(|| {
                    Box::new(PreparedCheckout {
                        submodules: view.pins().to_vec(),
                        setup,
                    })
                })
            } else {
                None
            };
            if checkout.is_some() && view.setup.is_none() {
                record_setup_pending(svc, &record.workspace_id, agent_id).await;
            }
            // The user's MCP servers, proxied to this daemon's hub, only on a
            // link that has them; an older machine would start without them
            // anyway (its instructions say so).
            let mcp_proxies = if link_protocol(&node).await? >= MCP_PROXY_PROTOCOL {
                svc.remote_mcp_proxies(&record.workspace_id).await
            } else {
                Vec::new()
            };
            // The same call that puts the condensed reference in the
            // instructions, so the compact description never points at a
            // missing section.
            let compact_tool_descriptions =
                intent_providers::find_provider(intent_providers::base_provider_id(&provider))
                    .is_some_and(|entry| entry.truncates_tool_descriptions);
            let checkout_dir = workspace_checkout(svc, &record.workspace_id).await?;
            // Every agent commits as one on this backend would: resolved
            // from the workspace checkout's config at each start.
            let git_identity = git_identity(checkout_dir.clone()).await;
            let repo_key = blocking(move || node_git::repo_key(&checkout_dir)).await?;
            // Only a top-level agent gets the workspace branch and `origin`.
            let forge = if is_sub_agent || checkout_of.is_some() {
                None
            } else {
                forge_launch(svc, &record.workspace_id).await
            };
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
                    mcp_proxies,
                    compact_tool_descriptions,
                    checkout_of,
                    forge,
                    git_identity,
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

    fn cloud_machines<'a>(
        &'a self,
        svc: &'a Services,
        method: &'static str,
        params: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(crate::cloud::call(svc, method, params))
    }
}

/// Longest an agent that retired itself during a turn keeps running for
/// that turn before its machine stops it.
const SELF_RETIRE_TURN_BOUND: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Send placed agent `agent_id` its final stop on `node`; the agent a cloud
/// sandbox was started for also takes the sandbox with it.
async fn final_stop_placed(
    svc: &Services,
    node: &RemoteNode,
    agent_id: &AgentId,
    node_id: &str,
    reason: intent_core::remote_launch::FinalStopReason,
) {
    node.final_stop(agent_id, reason).await;
    // The agent a sandbox was started for ends for good: its sub-agents
    // there get final stops too, then the sandbox goes.
    if let Ok(Some(row)) = svc.store().get_exposed_machine(node_id).await {
        if row
            .cloud
            .as_ref()
            .is_some_and(|cloud| cloud.agent_id == agent_id.as_str())
        {
            let svc = svc.clone();
            tokio::spawn(async move {
                let Some(feature) = svc.remote_feature() else {
                    return;
                };
                let client = crate::cloud::saved_client(&svc, &row).await;
                crate::cloud::delete_machine(feature, &svc, &row, client.as_ref()).await;
            });
        }
    }
}
