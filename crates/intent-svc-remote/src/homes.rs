//! Workspace homes, orchestrator side (monorepo
//! `docs/protocol/methods/nodes.md`, "Workspace homes", and
//! `node-link.md`, "Workspace homes (nodeProtocol 19)"): choosing a new
//! workspace's home, opening it on its machine (again after every bind) and
//! taking in its backup records.

use std::sync::Arc;
use std::time::Duration;

use intent_core::machine_order::MachineOrderKey;
use intent_core::machines::{
    ExposedMachine, MachineUpdateRequired, RemoteMachineState, UpdateSide,
};
use intent_core::node_link::{
    HomeOpen, HomeOpenOutcome, HomeOpenReply, HomePushReply, JournalBody, JournalRecord, LinkFrame,
    PreparedCheckout, RecordOwner, HOMES_PROTOCOL,
};
use intent_core::placement::{AgentPlacement, PlacementUnavailable, PlacementUnavailableReason};
use intent_core::remote_launch::RemotePlacementView;
use intent_core::{Error, HomeId, HomeState, Result, WorkspaceHome, WorkspaceId};
use intent_git::hub::AgentRef;
use intent_git::node_git::{self, AgentRemote};
use intent_services::feature_api::Services;
use intent_transport::node_link::NodeLink;

use crate::connector::Shared;
use crate::RemoteServicesExt as _;

/// How long a `homeOpen` may take: the machine's own 5 minute bound, plus
/// the trip.
const OPEN_WAIT: Duration = Duration::from_secs(5 * 60 + 30);

/// How long a machine may take to connect while a home is chosen.
const CONNECT_WAIT: Duration = Duration::from_secs(15);

/// The machine's answer to a home frame.
#[derive(Debug)]
pub(crate) enum HomeReply {
    Open(HomeOpenReply),
    #[expect(dead_code, reason = "read once hub operations push the home first")]
    Push(HomePushReply),
}

/// No machine can be a home.
fn no_home_machine() -> Error {
    let failure = intent_core::placement::PlatformAvailability::none(intent_core::now_iso())
        .unavailable(AgentPlacement::default());
    Error::PlacementUnavailable(Box::new(PlacementUnavailable {
        reason: PlacementUnavailableReason::NoAgentMachine,
        ..failure
    }))
}

/// Whether `row`'s last report lets it hold a home.
fn allows_homes(row: &ExposedMachine) -> bool {
    row.capabilities
        .as_ref()
        .is_none_or(|caps| caps.homes != Some(false))
}

/// A machine that is too old to be a home.
fn too_old(row: &ExposedMachine) -> MachineUpdateRequired {
    MachineUpdateRequired {
        machine_name: row.name.clone(),
        machine_version: row.capabilities.as_ref().map_or_else(
            || "an older version".to_owned(),
            |c| c.intentd_version.clone(),
        ),
        backend_version: env!("CARGO_PKG_VERSION").to_owned(),
        update: UpdateSide::Machine,
        hint: None,
        reason: None,
    }
}

enum Candidate {
    Home(WorkspaceHome),
    TooOld(MachineUpdateRequired),
    No,
}

/// Whether the exposed machine `node_id` can be a home now: known, not
/// revoked, online on nodeProtocol 19 with `homes` not `false`.
async fn machine_candidate(svc: &Services, node_id: &str) -> Candidate {
    let Ok(Some(row)) = svc.store().get_exposed_machine(node_id).await else {
        return Candidate::No;
    };
    if row.state == RemoteMachineState::Revoked || row.is_cloud() || !allows_homes(&row) {
        return Candidate::No;
    }
    let Some(feature) = svc.remote_feature() else {
        return Candidate::No;
    };
    let Ok(node) = feature.node(svc, node_id).await else {
        return Candidate::No;
    };
    if node.shared_ref().current_link().is_none()
        && !matches!(
            tokio::time::timeout(CONNECT_WAIT, node.connect()).await,
            Ok(Ok(()))
        )
    {
        return Candidate::No;
    }
    if node.shared_ref().protocol() < HOMES_PROTOCOL {
        return Candidate::TooOld(too_old(&row));
    }
    // The welcome refreshed the report: read it again.
    let row = match svc.store().get_exposed_machine(node_id).await {
        Ok(Some(row)) if allows_homes(&row) => row,
        _ => return Candidate::No,
    };
    Candidate::Home(WorkspaceHome {
        home_id: HomeId::new(),
        node_id: Some(row.id.clone()),
        cloud_machine_id: None,
        name: row.name,
        state: HomeState::Attached,
        since: intent_core::now_iso(),
        last_backup_at: None,
        backup_warning: false,
    })
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::choose_home`].
/// Cloud entries are not homes yet: they come with cloud homes.
pub(crate) async fn choose_home(
    svc: &Services,
    initial: Option<&RemotePlacementView>,
) -> Result<WorkspaceHome> {
    if let Some(view) = initial.filter(|view| !view.is_cloud() && !view.node_id.is_empty()) {
        if let Candidate::Home(home) = machine_candidate(svc, &view.node_id).await {
            return Ok(home);
        }
    }
    let mut too_old_only = None;
    for row in crate::order::current(svc).await? {
        let MachineOrderKey::Machine { node_id } = row.key else {
            continue;
        };
        match machine_candidate(svc, &node_id).await {
            Candidate::Home(home) => return Ok(home),
            Candidate::TooOld(required) => {
                too_old_only.get_or_insert(required);
            }
            Candidate::No => {}
        }
    }
    Err(match too_old_only {
        Some(required) => Error::MachineUpdateRequired(Box::new(required)),
        None => no_home_machine(),
    })
}

/// Point the hub's `home/head` of `remote` at `commit` unless it has one.
fn seed_home_head(repository: &std::path::Path, remote: &AgentRemote, commit: &str) -> Result<()> {
    let repo = git2::Repository::open(repository)
        .map_err(|e| Error::Internal(format!("open the workspace repository: {e}")))?;
    if repo
        .find_reference(&remote.ref_name(AgentRef::Head))
        .is_ok()
    {
        return Ok(());
    }
    let oid = git2::Oid::from_str(commit)
        .map_err(|_| Error::Internal(format!("invalid commit {commit}")))?;
    node_git::write_base_ref(repository, remote, oid)
}

/// `home/rewritten/<sha>` of `remote`'s workspace.
fn rewritten_ref(remote: &AgentRemote, sha: &str) -> String {
    format!("{}rewritten/{sha}", remote.ref_prefix())
}

fn git_error(what: &str) -> impl Fn(git2::Error) -> Error + '_ {
    move |e| Error::Internal(format!("{what}: {e}"))
}

/// Before a home push: pin the current `home/head` as
/// `home/rewritten/<sha>` unless that pin exists. The sha of a pin made
/// here; `None` when there is nothing to pin or it was pinned already.
///
/// # Errors
///
/// When the repository cannot be opened or the pin cannot be written.
pub(crate) fn pin_home_head(
    repository: &std::path::Path,
    remote: &AgentRemote,
) -> Result<Option<String>> {
    let repo =
        git2::Repository::open(repository).map_err(git_error("open the workspace repository"))?;
    let Ok(head) = repo.find_reference(&remote.ref_name(AgentRef::Head)) else {
        return Ok(None);
    };
    let Some(oid) = head.target() else {
        return Ok(None);
    };
    let sha = oid.to_string();
    let pin = rewritten_ref(remote, &sha);
    if repo.find_reference(&pin).is_ok() {
        return Ok(None);
    }
    repo.reference(&pin, oid, false, "pin the home's head before a push")
        .map_err(git_error("pin the home's head"))?;
    Ok(Some(sha))
}

/// After a home push: drop each pin of `pins` the new `home/head` still
/// contains (the push only added commits), and move the orchestrator's
/// worktree, its read-only mirror, to the new `home/head`.
///
/// # Errors
///
/// When the repository cannot be opened, a pin cannot be dropped or the
/// worktree cannot be moved.
pub(crate) fn settle_home_push(
    checkout: &std::path::Path,
    remote: &AgentRemote,
    pins: &[String],
) -> Result<()> {
    let repo =
        git2::Repository::open(checkout).map_err(git_error("open the workspace repository"))?;
    let Some(head) = repo
        .find_reference(&remote.ref_name(AgentRef::Head))
        .ok()
        .and_then(|head| head.target())
    else {
        return Ok(());
    };
    for sha in pins {
        let Ok(old) = git2::Oid::from_str(sha) else {
            continue;
        };
        let kept = old == head || repo.graph_descendant_of(head, old).unwrap_or(false);
        if kept {
            if let Ok(mut pin) = repo.find_reference(&rewritten_ref(remote, sha)) {
                if pin.target() == Some(old) {
                    pin.delete().map_err(git_error("drop a rewritten pin"))?;
                }
            }
        }
    }
    let at = repo.head().ok().and_then(|head| head.target());
    if at != Some(head) {
        let commit = repo
            .find_object(head, Some(git2::ObjectType::Commit))
            .map_err(git_error("read the home's head"))?;
        repo.reset(&commit, git2::ResetType::Hard, None)
            .map_err(git_error("move the mirror to the home's head"))?;
    }
    Ok(())
}

/// The home an agent placed on `view`'s machine works in (nodes.md
/// "Workspace homes" → "Sharing"): the workspace's home when it is on that
/// machine, the placement names no checkout, and the link speaks
/// nodeProtocol 19.
pub(crate) async fn home_checkout_for(
    svc: &Services,
    workspace_id: &WorkspaceId,
    placement: &AgentPlacement,
    view: &RemotePlacementView,
) -> Option<HomeId> {
    if !svc.workspace_homes() || placement.checkout.is_some() || view.is_cloud() {
        return None;
    }
    let home = svc.store().get_workspace_home(workspace_id).await.ok()??;
    if home.node_id.as_deref() != Some(view.node_id.as_str()) {
        return None;
    }
    // The protocol the machine's last bind negotiated, from its row: the
    // placement that calls this may hold the machine's connector.
    let row = svc
        .store()
        .get_exposed_machine(&view.node_id)
        .await
        .ok()??;
    (row.node_protocol.unwrap_or_default() >= HOMES_PROTOCOL).then_some(home.home_id)
}

/// The home checkout `record`'s agent launches into now: the one it was
/// placed in, while it is still the workspace's home on its machine.
/// After a move it gets a checkout of its own.
pub(crate) async fn current_home_checkout(
    svc: &Services,
    record: &intent_core::remote_launch::RemoteAgentRecord,
    view: &RemotePlacementView,
) -> Option<intent_core::node_link::HomeRef> {
    let placed = view.home_checkout.as_ref()?;
    let home = svc
        .store()
        .get_workspace_home(&record.workspace_id)
        .await
        .ok()??;
    (&home.home_id == placed && home.node_id.as_deref() == Some(record.node_id.as_str())).then(
        || intent_core::node_link::HomeRef {
            workspace_id: record.workspace_id.clone(),
            home_id: home.home_id,
        },
    )
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::open_home`].
pub(crate) async fn open_home(svc: &Services, workspace_id: &WorkspaceId) -> Result<()> {
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(());
    };
    let Some(node_id) = home.node_id.clone() else {
        return Ok(());
    };
    let feature = svc
        .remote_feature()
        .ok_or_else(|| Error::Internal("no remote machine feature".into()))?;
    let node = feature.node(svc, &node_id).await?;
    node.shared_ref().set_holds_homes(true);
    let frame = home_open_frame(svc, workspace_id, &home.home_id).await?;
    let request_id = frame.request_id;
    let reply = node
        .home_request(request_id, LinkFrame::HomeOpen(frame), OPEN_WAIT)
        .await?;
    settle_open(svc, workspace_id, &home.home_id, reply).await
}

/// The `homeOpen` for `home_id`, after seeding the hub's home refs.
async fn home_open_frame(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
) -> Result<HomeOpen> {
    let workspace = svc.store().get_workspace(workspace_id).await?;
    let checkout = crate::launch::checkout_path(&workspace).ok_or_else(|| {
        Error::InvalidParams("a workspace home needs a workspace with a git repository".into())
    })?;
    let base = crate::launch::checkout_base(svc, workspace_id).await?;
    let setup = svc.home_setup_command(workspace_id).await?;
    let remote = AgentRemote::home(workspace_id.as_str(), home_id.as_str())?;
    let (repo_key, submodules) = {
        let checkout = checkout.clone();
        let remote = remote.clone();
        let base = base.clone();
        tokio::task::spawn_blocking(move || {
            seed_home_head(&checkout, &remote, &base.commit)?;
            for pin in &base.submodules {
                crate::launch::write_module_base_ref(&checkout, &remote, pin)?;
            }
            Ok::<_, Error>((node_git::repo_key(&checkout)?, base.submodules))
        })
        .await
        .map_err(|e| Error::Internal(format!("seed the home refs: {e}")))??
    };
    Ok(HomeOpen {
        request_id: intent_core::node_link::LinkId::new_v4(),
        workspace_id: workspace_id.clone(),
        home_id: home_id.clone(),
        base_commit: base.commit,
        repo_key,
        checkout: PreparedCheckout { submodules, setup },
        restore: None,
    })
}

/// Record how a `homeOpen` ended.
async fn settle_open(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
    reply: HomeReply,
) -> Result<()> {
    let HomeReply::Open(reply) = reply else {
        return Err(Error::Internal(
            "the machine answered a homeOpen with another reply".into(),
        ));
    };
    match reply.outcome {
        HomeOpenOutcome::Ready => {
            if svc
                .store()
                .set_workspace_home_state(
                    workspace_id,
                    home_id,
                    HomeState::Attached,
                    &intent_core::now_iso(),
                )
                .await?
            {
                svc.publish_workspace_home_changed(workspace_id).await;
            }
            Ok(())
        }
        HomeOpenOutcome::Failed => Err(Error::Internal(format!(
            "the home did not open: {}",
            reply.error.unwrap_or_default()
        ))),
    }
}

/// After a bind on nodeProtocol 19: keep the link up while the machine holds
/// a home, and open each of its homes again (the machine answers `ready` at
/// once for one it holds), so a home missed at create or lost to a restart
/// is opened.
pub(crate) fn after_bind(shared: &Arc<Shared>, link: &NodeLink) {
    if link.protocol() < HOMES_PROTOCOL {
        return;
    }
    let Some(svc) = shared.services().cloned() else {
        return;
    };
    let shared = shared.clone();
    // caller-binding: allow — opens one machine's homes after its bind
    tokio::spawn(async move {
        let machine = shared.machine_id();
        let homes = match svc.store().homes_on_machine(&machine).await {
            Ok(homes) => homes,
            Err(error) => {
                tracing::warn!(%machine, %error, "remote machine: could not read its homes");
                return;
            }
        };
        shared.set_holds_homes(!homes.is_empty());
        if !svc.workspace_homes() {
            return;
        }
        for (workspace_id, _) in homes {
            if let Err(error) = open_home(&svc, &workspace_id).await {
                tracing::warn!(workspace = %workspace_id, %error, "remote machine: could not open the workspace's home");
            }
        }
    });
}

/// Take in a home's journal record: a backup moves `lastBackupAt`. A record
/// for a home that is not the workspace's current one is dropped.
pub(crate) async fn ingest(shared: &Shared, record: JournalRecord) {
    let RecordOwner::Home(home_id) = &record.owner else {
        return;
    };
    let Some(svc) = shared.services() else {
        return;
    };
    match &record.body {
        JournalBody::HomePushed { .. } => {
            match svc
                .store()
                .record_workspace_home_backup(
                    &record.workspace_id,
                    home_id,
                    &shared.machine_id(),
                    &record.recorded_at,
                )
                .await
            {
                Ok(true) => {
                    svc.publish_workspace_home_changed(&record.workspace_id)
                        .await;
                }
                Ok(false) => {
                    tracing::debug!(home = %home_id, "remote machine: backup of a home that is not current; dropped");
                }
                Err(error) => {
                    tracing::warn!(home = %home_id, %error, "remote machine: could not record a home backup");
                }
            }
        }
        _ => {
            tracing::debug!(home = %home_id, seq = record.seq.0, "remote machine: home record not used yet");
        }
    }
}

impl Shared {
    /// Whether the machine holds a home of this backend (keeps the link up).
    pub(crate) fn set_holds_homes(&self, holds: bool) {
        self.holds_homes
            .store(holds, std::sync::atomic::Ordering::SeqCst);
    }
}
