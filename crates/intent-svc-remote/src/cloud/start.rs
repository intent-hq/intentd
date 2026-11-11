//! Placement on E2B: admission, the start of a new sandbox after the create
//! reply, and the cleanup of one that failed or is removed.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::json;
use tokio::sync::watch;

use intent_core::machines::{
    cloud_profile_required, CloudMachine, CloudSandboxState, CloudStart, CloudStartStep,
    ExposedMachine, ModelUnavailable, CLOUD_PROVIDER_E2B_NAME,
};
use intent_core::placement::{
    AgentPlacement, PlacementCheckout, PlacementTarget, PlacementUnavailableReason,
    PlatformAvailability,
};
use intent_core::remote_launch::{
    EffectiveIsolation, PlacementError, RemoteLaunchNeeds, RemoteLaunchRequest, RemotePlacementView,
};
use intent_core::{now_iso, AgentId, Caller, Error, Result, WorkspaceId};
use intent_nodes::cloud::{CloudProvider, CreateInstance};
use intent_services::feature_api::Services;

use super::config::{self, ProviderState};
use super::{bootstrap, StartFailure, StartOutcome};
use crate::methods::AddParams;
use crate::RemoteFeature;

/// Longest wait for a sandbox start: create, boot, tunnel and probe.
pub(crate) const START_WAIT: Duration = Duration::from_secs(10 * 60);

/// Longest cloud machine row name, in bytes.
const MAX_NAME_BYTES: usize = 128;

/// An E2B entry as an agent's view names it: its id (kept on the view's
/// placement) and its name (the view's `nodeName`).
#[derive(Debug, Clone)]
struct EntryRef {
    id: String,
    name: String,
}

impl EntryRef {
    /// The entry of a cloud agent's view; a view saved before entries
    /// existed is on the legacy entry.
    fn of_view(view: &RemotePlacementView) -> Self {
        Self {
            id: view
                .placement
                .cloud_machine_id
                .clone()
                .unwrap_or_else(|| config::legacy_entry().to_owned()),
            name: view.node_name.clone(),
        }
    }

    /// The entry of cloud machine `row`, named as saved (or "E2B" once the
    /// entry is gone).
    async fn of_row(svc: &Services, row: &ExposedMachine) -> Self {
        let id = super::methods::row_entry(row).to_owned();
        let name = config::load(svc, &id)
            .await
            .ok()
            .flatten()
            .map_or_else(|| CLOUD_PROVIDER_E2B_NAME.to_owned(), |saved| saved.name);
        Self { id, name }
    }
}

/// The view of an agent placed on cloud machine `node`, or that waits for
/// its sandbox (`node` `None`), with E2B entry `entry`.
fn view(node: Option<(&crate::RemoteNode, &str)>, entry: &EntryRef) -> RemotePlacementView {
    let (node_id, lease_id) = match node {
        Some((node, node_id)) => (node_id.to_owned(), node.status().lease_id),
        None => (String::new(), String::new()),
    };
    RemotePlacementView {
        cloud_start: node.is_none().then_some(CloudStart {
            step: CloudStartStep::CreatingSandbox,
        }),
        node_id,
        lease_id,
        node_name: entry.name.clone(),
        placement: AgentPlacement {
            target: Some(PlacementTarget::E2b),
            checkout: Some(PlacementCheckout::Isolated),
            cloud_machine_id: Some(entry.id.clone()),
            ..AgentPlacement::default()
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
        launched_on: None,
    }
}

/// The profile checks every launch on a cloud machine passes: it sends a
/// profile environment, since a sandbox has no sign-in of its own. (The
/// environment was already checked for remote compatibility.)
fn check_profile(needs: &RemoteLaunchNeeds) -> Result<()> {
    if needs.sets_profile_env {
        Ok(())
    } else {
        Err(cloud_profile_required(&needs.provider_ref))
    }
}

/// The agent calling, when it runs on a cloud machine (or waits for one):
/// its id and view.
async fn cloud_caller(svc: &Services) -> Result<Option<(AgentId, RemotePlacementView)>> {
    let Some(Caller::Agent { agent_id }) = intent_core::current_caller() else {
        return Ok(None);
    };
    let session = match svc.store().get_agent_session_summary(&agent_id).await {
        Ok(session) => session,
        Err(Error::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(
        RemotePlacementView::from_metadata(session.metadata.as_ref())
            .filter(RemotePlacementView::is_cloud)
            .map(|view| (agent_id, view)),
    )
}

/// `machine-unknown` for `placement`, as if the cloud machine did not exist.
fn machine_unknown(placement: &AgentPlacement, node_id: &str) -> Error {
    Error::PlacementUnavailable(Box::new(
        PlatformAvailability::none(now_iso()).machine_unavailable(
            placement.clone(),
            PlacementUnavailableReason::MachineUnknown,
            intent_core::placement::MachineRef {
                node_id: node_id.to_owned(),
                name: node_id.to_owned(),
            },
            None,
        ),
    ))
}

/// Whether `agent` is `top` or one of its sub-agents.
async fn descends_from(svc: &Services, agent: &AgentId, top: &str) -> Result<bool> {
    let mut current = Some(agent.clone());
    for _ in 0..64 {
        let Some(id) = current else { return Ok(false) };
        if id.as_str() == top {
            return Ok(true);
        }
        current = match svc.store().get_agent_session_summary(&id).await {
            Ok(session) => session.parent_agent_id.clone(),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
    }
    Ok(false)
}

/// Place an agent on the existing cloud machine `node_id`: the sandbox of a
/// cloud caller, or the `nodeId` a sub-agent asked for.
async fn on_machine(
    feature: &RemoteFeature,
    svc: &Services,
    node_id: &str,
    entry: &EntryRef,
) -> Result<RemotePlacementView> {
    let node = feature.node(svc, node_id).await?;
    node.connect().await?;
    Ok(view(Some((&node, node_id)), entry))
}

/// Admission for a placement on E2B, before anything is written. Returns
/// `None` when `placement` is not about a cloud machine.
///
/// 1. The placement a sub-agent of an agent on a cloud machine inherits
///    (none given): the caller's sandbox, once it runs.
/// 2. `target: "remote"` with a cloud machine's `nodeId`: only for a
///    sub-agent of the machine's agent, else `machine-unknown`.
/// 3. An explicit `target: "e2b"`, from any caller: a new sandbox with the
///    E2B entry `placement.cloudMachineId`, else the calling cloud agent's own
///    entry, else the only entry; after the entry, profile and provider
///    checks.
pub(crate) async fn admit(
    feature: &RemoteFeature,
    svc: &Services,
    placement: &AgentPlacement,
    needs: &RemoteLaunchNeeds,
) -> Result<Option<RemotePlacementView>> {
    if let Some(node_id) = placement.node_id.as_deref() {
        let Some(row) = svc
            .store()
            .get_exposed_machine(node_id)
            .await?
            .filter(ExposedMachine::is_cloud)
        else {
            return Ok(None);
        };
        let top = row
            .cloud
            .as_ref()
            .map(|c| c.agent_id.clone())
            .unwrap_or_default();
        let allowed = match intent_core::current_caller() {
            Some(Caller::Agent { agent_id }) => descends_from(svc, &agent_id, &top).await?,
            _ => false,
        };
        if !allowed {
            return Err(machine_unknown(placement, node_id));
        }
        check_profile(needs)?;
        let entry = EntryRef::of_row(svc, &row).await;
        return on_machine(feature, svc, node_id, &entry).await.map(Some);
    }
    if !placement.is_cloud() {
        return Ok(None);
    }
    let shared = if needs.inherited {
        cloud_caller(svc).await?
    } else {
        None
    };
    if let Some((caller, caller_view)) = &shared {
        check_profile(needs)?;
        let linked = if caller_view.is_unlinked() {
            wait_linked(svc, caller).await?
        } else {
            caller_view.clone()
        };
        let entry = EntryRef::of_view(&linked);
        return on_machine(feature, svc, &linked.node_id, &entry)
            .await
            .map(Some);
    }
    let chain = match intent_core::current_caller() {
        Some(Caller::Agent { agent_id }) => chain_entry(svc, Some(agent_id)).await?,
        _ => None,
    };
    let (cloud_machine_id, saved) = config::resolve(
        svc,
        placement.cloud_machine_id.as_deref(),
        chain.as_deref(),
        placement.any_provider(),
    )
    .await?;
    let tag = |error: Error| {
        intent_core::machines::with_cloud_provider(
            intent_core::machines::with_cloud_machine_id(error, &cloud_machine_id),
            &saved.provider,
        )
    };
    admit_entry(&saved, needs).map_err(tag)?;
    Ok(Some(view(
        None,
        &EntryRef {
            id: cloud_machine_id,
            name: saved.name,
        },
    )))
}

/// The checks of a new sandbox with entry `saved`: its key is not known to
/// be rejected, the launch sends a profile, the base provider is in the
/// template.
fn admit_entry(saved: &config::ProviderConfig, needs: &RemoteLaunchNeeds) -> Result<()> {
    if saved.state == ProviderState::KeyRejected {
        return Err(config::key_rejected_by(&saved.provider, None));
    }
    check_profile(needs)?;
    if !config::agent_providers().contains(&needs.provider) {
        return Err(Error::ModelUnavailable(Box::new(ModelUnavailable::new(
            needs.provider.clone(),
            needs.model.clone().unwrap_or_default(),
            [],
            false,
        ))));
    }
    Ok(())
}

/// The entry of the nearest agent from `from` up its parents whose
/// placement names an entry that still exists.
async fn chain_entry(svc: &Services, from: Option<AgentId>) -> Result<Option<String>> {
    let mut current = from;
    for _ in 0..64 {
        let Some(id) = current.take() else { break };
        let session = match svc.store().get_agent_session_summary(&id).await {
            Ok(session) => session,
            Err(Error::NotFound(_)) => break,
            Err(error) => return Err(error),
        };
        let named = RemotePlacementView::from_metadata(session.metadata.as_ref())
            .and_then(|view| view.placement.cloud_machine_id);
        if let Some(entry) = named {
            if config::load(svc, &entry).await?.is_some() {
                return Ok(Some(entry));
            }
        }
        current.clone_from(&session.parent_agent_id);
    }
    Ok(None)
}

/// The entry a new sandbox of cloud agent `agent_id` starts with: the one
/// on its view; or, when its entry was removed, one picked again as for an
/// omitted `cloudMachineId` (from its parents), then kept on its view.
async fn agent_entry(
    svc: &Services,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    view: &RemotePlacementView,
) -> Result<EntryRef> {
    if view.placement.cloud_machine_id.is_some() {
        return Ok(EntryRef::of_view(view));
    }
    let parent = svc
        .store()
        .get_agent_session_summary(agent_id)
        .await?
        .parent_agent_id
        .clone();
    let chain = chain_entry(svc, parent).await?;
    let (id, saved) = config::resolve(svc, None, chain.as_deref(), false).await?;
    let entry = EntryRef {
        id,
        name: saved.name,
    };
    let stored = entry.clone();
    crate::view::update(svc.store(), workspace_id, agent_id, move |view| {
        view.placement.cloud_machine_id = Some(stored.id);
        view.node_name = stored.name;
    })
    .await?;
    svc.publish_agent_placement_changed(workspace_id, agent_id)
        .await;
    Ok(entry)
}

/// Start the sandbox of a just-created (or sandbox-less) cloud agent in the
/// background. A start already in flight for it is kept.
///
/// `seen` is the `placementError` the caller saw on the agent's unlinked
/// view. A start removes itself from the in-flight map only after its
/// outcome is on the view, so the view is read again once this start is
/// registered: when it is linked now, or its error changed, a start ended
/// in between and none is begun (no second sandbox for a linked agent, no
/// instant retry of a start that just failed).
pub(crate) async fn begin_start(
    svc: &Services,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    seen: Option<&PlacementError>,
) {
    let Ok(feature) = super::feature(svc) else {
        return;
    };
    let read_view = || async {
        svc.store()
            .get_agent_session_summary(agent_id)
            .await
            .ok()
            .and_then(|session| RemotePlacementView::from_metadata(session.metadata.as_ref()))
    };
    // The entry is read (or picked again) before the start registers, so
    // removing an entry waits only for its own starts.
    let Some(before) = read_view().await else {
        return;
    };
    let entry = match agent_entry(svc, workspace_id, agent_id, &before).await {
        Ok(entry) => entry,
        Err(error) => {
            if before.is_unlinked() {
                record_failure(svc, workspace_id, agent_id, &StartFailure::of(&error)).await;
            }
            return;
        }
    };
    let (sender, receiver) = watch::channel::<StartOutcome>(None);
    {
        let mut starts = crate::lock(&feature.cloud.starts);
        if starts.contains_key(agent_id.as_str()) {
            return;
        }
        starts.insert(
            agent_id.to_string(),
            super::Start {
                entry: entry.id.clone(),
                outcome: receiver,
            },
        );
    }
    let current = read_view().await;
    let still_waiting = current
        .as_ref()
        .is_some_and(|view| view.is_unlinked() && view.placement_error.as_ref() == seen);
    if !still_waiting {
        crate::lock(&feature.cloud.starts).remove(agent_id.as_str());
        let _ = sender.send(Some(Ok(())));
        return;
    }
    let started_by = super::started_by(svc).await;
    let svc = svc.clone();
    let workspace_id = workspace_id.clone();
    let agent_id = agent_id.clone();
    tokio::spawn(async move {
        let outcome = match start(&svc, &workspace_id, &agent_id, &entry.id, started_by).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let error = intent_core::machines::with_cloud_machine_id(error, &entry.id);
                Err(match config::load(&svc, &entry.id).await {
                    Ok(Some(saved)) => {
                        intent_core::machines::with_cloud_provider(error, &saved.provider)
                    }
                    _ => error,
                })
            }
        };
        let outcome = match outcome {
            Ok(()) => Ok(()),
            Err(error) => {
                let failure = StartFailure::of(&error);
                record_failure(&svc, &workspace_id, &agent_id, &failure).await;
                Err(failure)
            }
        };
        if let Ok(feature) = super::feature(&svc) {
            crate::lock(&feature.cloud.starts).remove(agent_id.as_str());
        }
        let _ = sender.send(Some(outcome));
    });
}

/// Wait until cloud agent `agent_id` runs on its sandbox, starting one when
/// it has none and no start is in flight (an agent whose sandbox is gone,
/// or a start cut by a backend restart). Returns its linked view.
///
/// # Errors
///
/// The start's error, or `cloud-machine-unavailable` when it takes too long.
pub(crate) async fn wait_linked(svc: &Services, agent_id: &AgentId) -> Result<RemotePlacementView> {
    let feature = super::feature(svc)?;
    let deadline = tokio::time::Instant::now() + START_WAIT;
    let mut started = false;
    loop {
        let session = svc.store().get_agent_session_summary(agent_id).await?;
        let Some(view) = RemotePlacementView::from_metadata(session.metadata.as_ref()) else {
            return Err(Error::InvalidParams(format!(
                "node-agent-unplaced: agent {agent_id} has no remote placement"
            )));
        };
        if !view.is_unlinked() {
            return Ok(view);
        }
        let receiver = match feature.cloud.start_of(agent_id) {
            Some(receiver) => receiver,
            None if !started => {
                started = true;
                begin_start(
                    svc,
                    &session.workspace_id,
                    agent_id,
                    view.placement_error.as_ref(),
                )
                .await;
                continue;
            }
            None => {
                // The start ended between our start and our wait: its
                // outcome is on the agent.
                return Err(view.placement_error.map_or_else(
                    || Error::Internal("the cloud machine did not start".into()),
                    |error| {
                        let mut data = serde_json::to_value(&error.data)
                            .ok()
                            .filter(serde_json::Value::is_object)
                            .unwrap_or_else(|| json!({}));
                        data["code"] = json!(error.code);
                        if let Some(retryable) = error.retryable {
                            data["retryable"] = json!(retryable);
                        }
                        Error::remote_machine(true, error.detail, data)
                    },
                ));
            }
        };
        let mut receiver = receiver;
        let finished = tokio::time::timeout_at(deadline, receiver.wait_for(Option::is_some)).await;
        match finished {
            Ok(Ok(outcome)) => {
                if let Some(Err(failure)) = outcome.clone() {
                    return Err(failure.error());
                }
            }
            Ok(Err(_)) => {}
            Err(_) => {
                return Err(intent_core::machines::cloud_machine_unavailable(
                    "api-error",
                    true,
                    None,
                    "the cloud machine did not start in time",
                ))
            }
        }
    }
}

/// Set the agent's start step and announce it.
async fn step(
    svc: &Services,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    step: CloudStartStep,
) {
    if let Ok(true) = crate::view::update(svc.store(), workspace_id, agent_id, |view| {
        view.cloud_start = Some(CloudStart { step });
        view.placement_error = None;
    })
    .await
    {
        svc.publish_agent_placement_changed(workspace_id, agent_id)
            .await;
    }
}

/// Record a failed start: `placementError`, no `cloudStart`.
async fn record_failure(
    svc: &Services,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    failure: &StartFailure,
) {
    tracing::warn!(agent = %agent_id, code = %failure.data["code"], reason = %failure.data["reason"], "cloud machine: the sandbox did not start");
    let error = PlacementError::cloud(&failure.message, &failure.data);
    // A failure after the link (the launch on the new machine) leaves no
    // machine: the agent is unlinked again, so its next message starts a
    // new sandbox.
    match crate::view::update(svc.store(), workspace_id, agent_id, |view| {
        view.cloud_start = None;
        view.placement_error = Some(error);
        view.node_id.clear();
        view.lease_id.clear();
        view.shared_checkout_of = None;
    })
    .await
    {
        Ok(true) => {
            svc.publish_agent_placement_changed(workspace_id, agent_id)
                .await;
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(agent = %agent_id, %error, "cloud machine: could not record the failed start");
        }
    }
}

/// `"<entry name> · <agent name>"`, cut to the row name limit.
pub(super) fn machine_name(entry_name: &str, agent_name: &str) -> String {
    let mut name = format!("{} · {}", entry_name.trim(), agent_name.trim());
    if name.len() > MAX_NAME_BYTES {
        let mut end = MAX_NAME_BYTES;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
    }
    name.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// An idempotency key for one start of `agent_id`: E2B keys must match
/// `[a-z0-9-]{1,255}`.
fn idempotency_key(agent_id: &AgentId) -> String {
    let agent: String = agent_id
        .as_str()
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    format!("intent-{agent}-{}", uuid::Uuid::new_v4().simple())
}

/// One sandbox start: create, bootstrap, add the machine row and launch.
/// The sandbox is deleted when a later step fails.
async fn start(
    svc: &Services,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    cloud_machine_id: &str,
    started_by: intent_core::machines::CloudStartedBy,
) -> Result<()> {
    let feature = super::feature(svc)?;
    step(svc, workspace_id, agent_id, CloudStartStep::CreatingSandbox).await;
    let saved = config::load(svc, cloud_machine_id)
        .await?
        .ok_or_else(config::not_configured)?;
    let key = config::saved_key(svc, cloud_machine_id)
        .await?
        .ok_or_else(config::not_configured)?;
    let client = config::client(&saved, key.clone())?;
    // A rename may have run since admission wrote the entry's name.
    let renamed = crate::view::update(svc.store(), workspace_id, agent_id, |view| {
        view.node_name.clone_from(&saved.name);
    })
    .await;
    if matches!(renamed, Ok(true)) {
        svc.publish_agent_placement_changed(workspace_id, agent_id)
            .await;
    }
    let session = svc.store().get_agent_session_summary(agent_id).await?;
    let backend_id = svc.store().daemon_id().await?;
    let template = saved.template(saved.size);
    let mut metadata = BTreeMap::new();
    metadata.insert("intentBackendId".to_owned(), backend_id.clone());
    metadata.insert("workspaceId".to_owned(), workspace_id.to_string());
    metadata.insert("agentId".to_owned(), agent_id.to_string());
    metadata.insert("intentEntryId".to_owned(), cloud_machine_id.to_owned());
    metadata.insert(
        "intentdVersion".to_owned(),
        config::intentd_version().to_owned(),
    );
    let created = client
        .create(&CreateInstance {
            image: template.clone(),
            timeout_secs: Some(super::lifecycle::SANDBOX_TIMEOUT_SECS),
            pause_on_timeout: Some(true),
            tags: metadata,
            idempotency_key: Some(idempotency_key(agent_id)),
            plan: saved.plan(saved.size),
            disk_gib: saved.northflank.as_ref().map(|nf| nf.disk_gib),
        })
        .await
        .map_err(|error| config::to_error(&error, config::Phase::Create, client.kind()));
    let created = match created {
        Ok(created) => created,
        Err(error) => {
            if is_key_rejected(&error) {
                config::note_key_rejected(svc, cloud_machine_id, &key).await;
            }
            return Err(error);
        }
    };
    let sandbox_id = created.id.clone();
    let linked = async {
        let backend_name = crate::methods::backend_name(svc).await;
        let fingerprint = svc
            .own_machine_routes()
            .await
            .map(|routes| routes.fingerprint);
        let params = bootstrap::mint_params(&backend_id, &backend_name, fingerprint.as_deref());
        step(svc, workspace_id, agent_id, CloudStartStep::StartingDaemon).await;
        let minted = bootstrap::run(&*client, &created, &params).await?;
        step(svc, workspace_id, agent_id, CloudStartStep::Connecting).await;
        let cloud = CloudMachine {
            provider: client.kind().to_owned(),
            cloud_machine_id: cloud_machine_id.to_owned(),
            sandbox_id: sandbox_id.clone(),
            volume_id: created.volume_id.clone(),
            state: CloudSandboxState::Running,
            template: template.clone(),
            size: saved.size,
            started_by,
            agent_id: agent_id.to_string(),
            workspace_id: workspace_id.to_string(),
            started_at: now_iso(),
            running_seconds: 0,
            running_since: Some(now_iso()),
            paused_at: None,
        };
        let machine_id = minted.machine_id.clone();
        crate::methods::add_machine(
            feature,
            svc,
            AddParams {
                machine_id: minted.machine_id,
                name: machine_name(&saved.name, &session.name),
                hosts: minted.hosts,
                port: minted.port,
                tc_address: minted.tc_address,
                fingerprint: minted.fingerprint,
                credential_id: minted.credential_id,
                token: minted.token,
            },
            Some(cloud),
        )
        .await?;
        // `cloudMachines.remove` may have run meanwhile: nothing would be
        // left to delete this sandbox.
        let launched = if config::load(svc, cloud_machine_id).await?.is_none() {
            Err(config::not_configured())
        } else {
            link(feature, svc, workspace_id, agent_id, &machine_id).await
        };
        if let Err(error) = launched {
            // The sandbox itself is deleted once, below.
            if let Ok(Some(row)) = svc.store().get_exposed_machine(&machine_id).await {
                delete_machine(feature, svc, &row, None).await;
            }
            return Err(error);
        }
        Ok(())
    }
    .await;
    if linked.is_err() {
        if let Err(error) = client.delete(&sandbox_id).await {
            tracing::warn!(%error, sandbox = %sandbox_id, "cloud machine: could not delete the sandbox of a failed start");
        }
    }
    linked
}

/// Bind the agent to its new machine: its view gets the machine and lease,
/// loses `cloudStart`, and the remote launch places it there.
async fn link(
    feature: &RemoteFeature,
    svc: &Services,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    machine_id: &str,
) -> Result<()> {
    let node = feature.node(svc, machine_id).await?;
    let lease_id = node.status().lease_id;
    let session = svc.store().get_agent_session_summary(agent_id).await?;
    let mut view =
        RemotePlacementView::from_metadata(session.metadata.as_ref()).ok_or_else(|| {
            Error::Internal(format!(
                "agent {agent_id} lost its placement while its sandbox started"
            ))
        })?;
    view.node_id = machine_id.to_owned();
    view.lease_id = lease_id;
    view.cloud_start = None;
    view.placement_error = None;
    if view.base_commit.is_empty() {
        let base = crate::launch::checkout_base(svc, workspace_id).await?;
        view.base_commit = base.commit;
        view.submodules = Some(base.submodules);
    }
    let stored = view.clone();
    crate::view::update(svc.store(), workspace_id, agent_id, move |current| {
        *current = stored;
    })
    .await?;
    crate::launch::launch_linked(
        svc,
        RemoteLaunchRequest {
            workspace_id: workspace_id.clone(),
            agent_id: agent_id.clone(),
            view,
        },
    )
    .await?;
    svc.publish_agent_placement_changed(workspace_id, agent_id)
        .await;
    Ok(())
}

/// Remove cloud machine `row` for good: final stops for its agents (at most
/// the removal budget; a paused sandbox is resumed first, and one that does
/// not resume gets none), then its connector, row, token, leases and
/// sandbox. Its agents keep their placement without a machine. Returns the
/// agents whose stop was not acknowledged.
pub(crate) async fn delete_machine(
    feature: &RemoteFeature,
    svc: &Services,
    row: &ExposedMachine,
    client: Option<&dyn CloudProvider>,
) -> Vec<String> {
    remove_machine(feature, svc, row, client, Removal::Stop).await
}

/// How [`remove_machine`] ends a machine's sandbox.
#[derive(Clone, Copy)]
pub(crate) enum Removal<'a> {
    /// Final stops (resuming a paused sandbox first), then the E2B delete.
    Stop,
    /// E2B no longer has the sandbox: no stops and no E2B delete; the
    /// agents get the error as their `placementError`.
    Lost(&'a PlacementError),
    /// The sandbox cannot link to this backend: no stops, then the E2B
    /// delete.
    Unlinkable,
}

/// [`delete_machine`], ending the sandbox as `removal` says.
pub(crate) async fn remove_machine(
    feature: &RemoteFeature,
    svc: &Services,
    row: &ExposedMachine,
    client: Option<&dyn CloudProvider>,
    removal: Removal<'_>,
) -> Vec<String> {
    let lost = match removal {
        Removal::Lost(error) => Some(error),
        Removal::Stop | Removal::Unlinkable => None,
    };
    let mut unconfirmed = Vec::new();
    let _lifecycle = feature.lifecycle(&row.id).await;
    let store = svc.store();
    // Another removal may have finished meanwhile.
    let row = match store.get_exposed_machine(&row.id).await {
        Ok(Some(current)) => current,
        Ok(None) => {
            feature.cloud.forget(&row.id);
            return unconfirmed;
        }
        Err(_) => row.clone(),
    };
    let row = &row;
    // The agents placed here, read before their final stops end their
    // records, so each one is left without a machine afterwards.
    let placed = placed_agents(store, &row.id).await.unwrap_or_default();
    let mut stops = matches!(removal, Removal::Stop);
    if let (true, Some(client), Some(cloud)) = (stops, client, &row.cloud) {
        let paused = cloud.state != CloudSandboxState::Running;
        super::lifecycle::set_state(svc, feature, &row.id, CloudSandboxState::Stopping).await;
        if paused {
            stops = super::lifecycle::resume_for_stop(feature, client, row).await;
        }
    }
    let node = if stops {
        feature.node_locked(svc, &row.id).await.ok()
    } else {
        if let Some(node) = feature.running_node(&row.id) {
            node.shared_ref().drop_stops();
            let turn_error = match removal {
                Removal::Unlinkable => super::lifecycle::outdated_turn_error(),
                Removal::Stop | Removal::Lost(_) => "cloud-machine-unavailable: the agent's E2B sandbox was deleted without a final stop; work it pushed is kept".to_owned(),
            };
            node.shared_ref().agents.end_all(&turn_error);
        }
        None
    };
    if let Some(node) = node {
        let stopped = node.stop_all().await;
        if let Err(Error::RemoteMachine(failure)) =
            crate::methods::deliver_stops(feature, &node, &stopped, &row.name).await
        {
            if let Some(ids) = failure.data["unconfirmedStops"].as_array() {
                unconfirmed.extend(ids.iter().filter_map(|id| id.as_str().map(str::to_owned)));
            }
        }
    }
    if let Some(node) = feature.take_node(&row.id) {
        node.shutdown();
    }
    let lost_error = lost.cloned();
    let unlinked = async {
        let head_id = store.daemon_id().await?;
        let mut agents = placed;
        for lease in store.list_owned_node_leases(&head_id).await? {
            if lease.owner.node_id != row.id {
                continue;
            }
            for stop in store.list_final_stops(&lease.owner.lease_id).await? {
                store
                    .finish_final_stop(&stop.agent_id, &stop.stop_id)
                    .await?;
            }
            for agent in store.list_remote_agents(&lease.owner.lease_id).await? {
                if !agents.iter().any(|known| known.agent_id == agent.agent_id) {
                    agents.push(agent);
                }
            }
        }
        store.remove_exposed_machine(&row.id).await?;
        svc.delete_remote_machine_token(&row.id).await?;
        store.end_machine_leases(&row.id, &head_id).await?;
        for agent in agents {
            store.remove_remote_agent(&agent.agent_id).await?;
            let lost_error = lost_error.clone();
            let _ = crate::view::update(store, &agent.workspace_id, &agent.agent_id, |view| {
                view.node_id.clear();
                view.lease_id.clear();
                view.shared_checkout_of = None;
                if lost_error.is_some() {
                    view.placement_error.clone_from(&lost_error);
                }
            })
            .await;
            svc.publish_agent_placement_changed(&agent.workspace_id, &agent.agent_id)
                .await;
        }
        Ok::<(), Error>(())
    }
    .await;
    if let Err(error) = unlinked {
        tracing::warn!(machine = %row.id, %error, "cloud machine: could not remove its records");
    }
    if let (None, Some(client), Some(cloud)) = (lost, client, &row.cloud) {
        if let Err(error) = client.delete(&cloud.sandbox_id).await {
            tracing::warn!(%error, sandbox = %cloud.sandbox_id, "cloud machine: could not delete its sandbox");
        }
    }
    feature.cloud.forget(&row.id);
    svc.publish_cloud_machine_changed(
        json!({ "machineId": row.id, "removed": true }),
        row.cloud.as_ref().map(|cloud| cloud.workspace_id.as_str()),
    )
    .await;
    unconfirmed
}

/// The agents recorded on machine `machine_id`'s leases.
pub(super) async fn placed_agents(
    store: &intent_store::Store,
    machine_id: &str,
) -> Result<Vec<intent_core::remote_launch::RemoteAgentRecord>> {
    let head_id = store.daemon_id().await?;
    let mut agents = Vec::new();
    for lease in store.list_owned_node_leases(&head_id).await? {
        if lease.owner.node_id == machine_id {
            agents.extend(store.list_remote_agents(&lease.owner.lease_id).await?);
        }
    }
    Ok(agents)
}

/// Whether `error` is `cloud-machine-unavailable` reason `key-rejected`.
fn is_key_rejected(error: &Error) -> bool {
    matches!(error, Error::RemoteMachine(failure) if failure.data["reason"] == "key-rejected")
}
