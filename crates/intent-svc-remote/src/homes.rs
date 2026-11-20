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
    HomeOpen, HomeOpenOutcome, HomeOpenReply, HomePushReply, HomeRestore, HomeStopAck,
    HomeStopOutcome, HomeStopReason, HomeStopRecord, JournalBody, JournalRecord, LinkFrame,
    PreparedCheckout, RecordOwner, SetupState, HOMES_PROTOCOL,
};
use intent_core::placement::{AgentPlacement, PlacementUnavailable, PlacementUnavailableReason};
use intent_core::remote_launch::{RemotePlacementView, RemoteSetupState};
use intent_core::{Error, HomeId, HomeState, Result, WorkspaceHome, WorkspaceId};
use intent_git::hub::AgentRef;
use intent_git::node_git::{self, AgentRemote};
use intent_services::feature_api::Services;
use intent_services::home_setup::HomeSetupFinished;
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
pub(crate) fn allows_homes(row: &ExposedMachine) -> bool {
    row.capabilities
        .as_ref()
        .is_none_or(|caps| caps.homes != Some(false))
}

/// A machine that is too old to be a home.
pub(crate) fn too_old(row: &ExposedMachine) -> MachineUpdateRequired {
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

pub(crate) enum Candidate {
    Home(WorkspaceHome),
    TooOld(MachineUpdateRequired),
    No,
}

/// Whether the exposed machine `node_id` can be a home now: known, not
/// revoked, online on nodeProtocol 19 with `homes` not `false`.
pub(crate) async fn machine_candidate(svc: &Services, node_id: &str) -> Candidate {
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
        setup: None,
    })
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::choose_home`].
/// A cloud entry (the initial agent's, or a row of the list) is recorded
/// with no machine: its sandbox starts lazily ([`crate::cloud::home`]).
pub(crate) async fn choose_home(
    svc: &Services,
    initial: Option<&RemotePlacementView>,
) -> Result<WorkspaceHome> {
    if let Some(view) = initial {
        if view.is_cloud() {
            if let Some(entry) = view.placement.cloud_machine_id.as_deref() {
                return Ok(crate::cloud::home::new_home(entry, &view.node_name));
            }
        } else if !view.node_id.is_empty() {
            if let Candidate::Home(home) = machine_candidate(svc, &view.node_id).await {
                return Ok(home);
            }
        }
    }
    let mut too_old_only = None;
    for row in crate::order::current(svc).await? {
        let node_id = match row.key {
            MachineOrderKey::Machine { node_id } => node_id,
            MachineOrderKey::Cloud { cloud_machine_id } => {
                return Ok(crate::cloud::home::new_home(&cloud_machine_id, &row.name));
            }
            MachineOrderKey::Local => continue,
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
/// nodeProtocol 19. A cloud agent waiting for its new sandbox gets the
/// cloud home that sandbox may become; its start keeps it only when the
/// sandbox does ([`crate::cloud::home::take`]).
pub(crate) async fn home_checkout_for(
    svc: &Services,
    workspace_id: &WorkspaceId,
    placement: &AgentPlacement,
    view: &RemotePlacementView,
) -> Option<HomeId> {
    if !svc.workspace_homes() || placement.checkout.is_some() {
        return None;
    }
    if view.is_cloud() && view.is_unlinked() {
        let entry = view.placement.cloud_machine_id.as_deref()?;
        return crate::cloud::home::claimable(svc, workspace_id, entry)
            .await
            .map(|home| home.home_id);
    }
    let home = svc.store().get_workspace_home(workspace_id).await.ok()??;
    // A moving home is about to stop: the agent gets its own checkout.
    if home.node_id.as_deref() != Some(view.node_id.as_str()) || home.state == HomeState::Moving {
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
/// After a move it gets a checkout of its own ([`leave_moved_home`]).
///
/// # Errors
///
/// The store's error: an unread home is not a moved one.
pub(crate) async fn current_home_checkout(
    svc: &Services,
    record: &intent_core::remote_launch::RemoteAgentRecord,
    view: &RemotePlacementView,
) -> Result<Option<intent_core::node_link::HomeRef>> {
    let Some(placed) = view.home_checkout.as_ref() else {
        return Ok(None);
    };
    let Some(home) = svc.store().get_workspace_home(&record.workspace_id).await? else {
        return Ok(None);
    };
    Ok(
        (&home.home_id == placed && home.node_id.as_deref() == Some(record.node_id.as_str())).then(
            || intent_core::node_link::HomeRef {
                workspace_id: record.workspace_id.clone(),
                home_id: home.home_id,
            },
        ),
    )
}

/// nodes.md "Moving the home" step 4: `record`'s agent shared a home that
/// is no longer the workspace's, so it starts in a checkout of its own on
/// the same machine. Its `base` and `wip` refs are written from the hub's
/// `home/head` and `home/wip` (each submodule's too, for the workspace's
/// current submodules), and `view` drops the home and takes the new base.
/// When the workspace has a home, the agent and the user get a notice.
///
/// A retry after a failure part-way redoes every step: the agent's old
/// `wip` refs are cleared first, and the transcript row is added only by
/// the call whose placement update drops the home.
pub(crate) async fn leave_moved_home(
    svc: &Services,
    record: &intent_core::remote_launch::RemoteAgentRecord,
    view: &mut RemotePlacementView,
) -> Result<()> {
    let workspace_id = &record.workspace_id;
    let agent_id = &record.agent_id;
    let moved = svc
        .store()
        .get_workspace_home(workspace_id)
        .await?
        .is_some();
    let checkout = crate::launch::workspace_checkout(svc, workspace_id).await?;
    let current = crate::launch::checkout_base(svc, workspace_id).await?;
    let home = AgentRemote::home(workspace_id.as_str(), &HomeId::new().0)?;
    let agent = AgentRemote::new(workspace_id.as_str(), agent_id.as_str())?;
    let pins = if view.submodules.is_some() {
        current.submodules.clone()
    } else {
        Vec::new()
    };
    let (base, pins) = crate::launch::blocking(move || {
        clear_wip(&checkout, &agent, &pins)?;
        let copied = crate::launch::copy_work(&checkout, &home, &agent, pins.clone())?;
        let (base, pins) = copied.unwrap_or_else(|| {
            tracing::warn!(
                "the moved home has no saved head; the agent starts from the workspace's checkout"
            );
            (current.commit, pins)
        });
        crate::launch::write_base_refs(&checkout, &agent, &base, &pins)?;
        Ok((base, pins))
    })
    .await?;
    view.home_checkout = None;
    view.base_commit = base;
    if view.submodules.is_some() {
        view.submodules = Some(pins);
    }
    let notice = moved.then(|| intent_core::remote_launch::home_moved_notice(&view.node_name));
    if let Some(notice) = &notice {
        svc.store()
            .set_agent_session_metadata_key(
                workspace_id,
                agent_id,
                intent_core::PENDING_HOME_MOVED_NOTICE_KEY,
                notice,
                None,
                None,
            )
            .await?;
    }
    let stored = view.clone();
    let left = crate::view::update(svc.store(), workspace_id, agent_id, move |current| {
        current.home_checkout = None;
        current.base_commit = stored.base_commit;
        current.submodules = stored.submodules;
    })
    .await?;
    if let Some(notice) = notice.filter(|_| left) {
        svc.add_home_moved_transcript_row(workspace_id, agent_id, &notice)
            .await;
    }
    tracing::info!(workspace = %workspace_id, agent = %agent_id, "the agent left its home; it starts in a checkout of its own");
    Ok(())
}

/// Delete `agent`'s `wip` refs in head's repositories, the superproject's
/// and each of `pins`' submodules', so a stale one is never restored.
fn clear_wip(
    checkout: &std::path::Path,
    agent: &AgentRemote,
    pins: &[intent_core::node_link::SubmodulePin],
) -> Result<()> {
    let delete = |repository: &std::path::Path, remote: &AgentRemote| -> Result<()> {
        let repo = git2::Repository::open(repository)
            .map_err(|e| Error::Internal(format!("open the workspace repository: {e}")))?;
        let found = repo.find_reference(&remote.ref_name(AgentRef::Wip));
        match found {
            Ok(mut reference) => reference
                .delete()
                .map_err(|e| Error::Internal(format!("delete the agent's wip ref: {e}"))),
            Err(error) if error.code() == git2::ErrorCode::NotFound => Ok(()),
            Err(error) => Err(Error::Internal(format!(
                "read the agent's wip ref: {error}"
            ))),
        }
    };
    delete(checkout, agent)?;
    for pin in pins {
        if let Ok(module) = node_git::module_repository(checkout, &pin.path, &pin.module_key) {
            delete(&module.worktree, &agent.with_module(&pin.module_key)?)?;
        }
    }
    Ok(())
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::open_home`].
pub(crate) async fn open_home(svc: &Services, workspace_id: &WorkspaceId) -> Result<()> {
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(());
    };
    let Some(node_id) = home.node_id.clone() else {
        return Ok(());
    };
    // A home moving away is not opened again: the move ends it, or gives
    // it back with its old state.
    if home.state == HomeState::Moving {
        return Ok(());
    }
    // nodes.md "Opening the home": a home whose open failed reports again.
    if svc.home_setups().reopening(workspace_id, &home.home_id) {
        svc.publish_workspace_home_changed(workspace_id).await;
    }
    let opened = match send_open(svc, workspace_id, &home.home_id, &node_id, None).await {
        Ok(reply) => settle_open(svc, workspace_id, &home.home_id, reply).await,
        Err(error) => Err(error),
    };
    if opened.is_ok() && svc.home_setups().opened(workspace_id, &home.home_id) {
        svc.publish_workspace_home_changed(workspace_id).await;
    }
    if let Err(error) = &opened {
        let reason = match error {
            Error::Internal(message) => message
                .strip_prefix(OPEN_FAILED_PREFIX)
                .unwrap_or(message)
                .to_owned(),
            other => other.to_string(),
        };
        if svc
            .home_setups()
            .open_failed(workspace_id, &home.home_id, &reason)
        {
            svc.publish_workspace_home_changed(workspace_id).await;
        }
    }
    opened
}

/// How [`settle_open`] starts the error of a `homeOpen` the machine
/// answered `failed`; the rest is the machine's `error`.
const OPEN_FAILED_PREFIX: &str = "the home did not open: ";

/// The hub's `home/head` and `home/wip`, which a new home restores (a
/// move's target, or a rebuilt cloud home); `None` without a `home/head`.
///
/// # Errors
///
/// When the workspace or its repository cannot be read.
pub(crate) async fn home_restore(
    svc: &Services,
    workspace_id: &WorkspaceId,
) -> Result<Option<HomeRestore>> {
    let workspace = svc.store().get_workspace(workspace_id).await?;
    let checkout = crate::launch::checkout_path(&workspace).ok_or_else(|| {
        Error::InvalidParams("a workspace home needs a workspace with a git repository".into())
    })?;
    let remote = AgentRemote::home(workspace_id.as_str(), &HomeId::new().0)?;
    tokio::task::spawn_blocking(move || {
        let repo = git2::Repository::open(&checkout)
            .map_err(git_error("open the workspace repository"))?;
        let read = |kind| {
            repo.find_reference(&remote.ref_name(kind))
                .ok()
                .and_then(|reference| reference.target())
                .map(|oid| oid.to_string())
        };
        Ok(read(AgentRef::Head).map(|head| HomeRestore {
            head,
            wip: read(AgentRef::Wip),
        }))
    })
    .await
    .map_err(|e| Error::Internal(format!("read the home refs: {e}")))?
}

/// Send the `homeOpen` of `home_id` to the exposed machine `node_id` and
/// wait for its answer. A cloud home rebuilt after its sandbox was deleted
/// (nodes.md "Cloud homes" 5) restores the hub's home refs unless
/// `restore` says what to restore.
pub(crate) async fn send_open(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
    node_id: &str,
    restore: Option<HomeRestore>,
) -> Result<HomeReply> {
    let feature = svc
        .remote_feature()
        .ok_or_else(|| Error::Internal("no remote machine feature".into()))?;
    let restore = match restore {
        Some(restore) => Some(restore),
        None if svc
            .store()
            .home_rebuild_of(workspace_id, home_id)
            .await?
            .is_some() =>
        {
            // A rebuild whose backup cannot be read fails, as a failed
            // start: it is never opened as new.
            home_restore(svc, workspace_id).await?
        }
        None => None,
    };
    let node = feature.node(svc, node_id).await?;
    node.shared_ref().set_holds_homes(true);
    let mut frame = home_open_frame(svc, workspace_id, home_id).await?;
    frame.restore = restore;
    let request_id = frame.request_id;
    node.home_request(request_id, LinkFrame::HomeOpen(frame), OPEN_WAIT)
        .await
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
pub(crate) async fn settle_open(
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
            // Only a detached or starting home is attached here: a moving
            // one keeps its state until the move ends.
            for from in [HomeState::Starting, HomeState::Detached] {
                if svc
                    .store()
                    .move_workspace_home_state(
                        workspace_id,
                        home_id,
                        from,
                        HomeState::Attached,
                        &intent_core::now_iso(),
                    )
                    .await?
                {
                    crate::cloud::home::changed(svc);
                    svc.publish_workspace_home_changed(workspace_id).await;
                    break;
                }
            }
            crate::cloud::home::rebuilt(svc, workspace_id, home_id).await;
            Ok(())
        }
        HomeOpenOutcome::Failed => Err(Error::Internal(format!(
            "{OPEN_FAILED_PREFIX}{}",
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
        let stops = match svc.store().list_home_stops(&machine).await {
            Ok(stops) => stops,
            Err(error) => {
                tracing::warn!(%machine, %error, "remote machine: could not read its home stops");
                Vec::new()
            }
        };
        shared.set_holds_homes(!homes.is_empty() || !stops.is_empty());
        // Every pending stop goes again after each bind until its ack, and
        // on a timer while this link lasts: a `failed` ack (a moved home's
        // push did not land) or one that never came is retried.
        let pending = !stops.is_empty();
        for stop in stops {
            send_stop(&shared, &stop).await;
        }
        if pending {
            ensure_stop_resends(&shared);
        }
        if !svc.workspace_homes() {
            return;
        }
        crate::home_state::attach_homes(&svc, &homes).await;
        let mut opened = Vec::new();
        for (workspace_id, _) in homes {
            match open_home(&svc, &workspace_id).await {
                Ok(()) => opened.push(workspace_id),
                Err(error) => {
                    tracing::warn!(workspace = %workspace_id, %error, "remote machine: could not open the workspace's home");
                }
            }
        }
        for workspace_id in opened {
            let svc = svc.clone();
            // caller-binding: allow — asks one home attached again for a backup, apart from the others
            tokio::spawn(async move {
                crate::home_state::push_failing(&svc, &workspace_id).await;
            });
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
    if record_setup(svc, home_id, &record) {
        svc.publish_workspace_home_changed(&record.workspace_id)
            .await;
    }
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
                    shared.home_link.backed_up(home_id);
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
        // A new home checkout finished its setup (nodeProtocol 19 journals
        // it once per checkout): its `autoStart` scripts start now, not when
        // a later `homeOpen` answers `ready` for the checkout it holds.
        JournalBody::SetupFinished { .. } => {
            if !svc.workspace_homes()
                || !auto_start_due(
                    svc.store(),
                    &shared.machine_id(),
                    &shared.lease_id(),
                    &record,
                )
                .await
            {
                return;
            }
            let svc = svc.clone();
            let workspace_id = record.workspace_id.clone();
            // Off the link's ingest loop (a start dials this same link), as
            // the daemon: no client asked for it.
            intent_core::spawn_daemon(async move {
                svc.feature_runtime()
                    .start_auto_start(&svc, workspace_id)
                    .await;
            });
        }
        _ => {
            tracing::debug!(home = %home_id, seq = record.seq.0, "remote machine: home record not used yet");
        }
    }
}

/// Keep a home's `setupStarted` or `setupFinished` `record` as its
/// `setup` (nodes.md "Opening the home"); whether that changed it.
fn record_setup(svc: &Services, home_id: &HomeId, record: &JournalRecord) -> bool {
    let setups = svc.home_setups();
    match &record.body {
        JournalBody::SetupStarted { started_at } => {
            setups.setup_started(&record.workspace_id, home_id, started_at)
        }
        JournalBody::SetupFinished {
            state,
            exit_code,
            error,
            finished_at,
        } => setups.setup_finished(
            &record.workspace_id,
            home_id,
            HomeSetupFinished {
                state: match state {
                    SetupState::Completed => RemoteSetupState::Completed,
                    SetupState::Failed => RemoteSetupState::Failed,
                    SetupState::Skipped => RemoteSetupState::Skipped,
                },
                exit_code: *exit_code,
                error: error.clone(),
                finished_at: finished_at.clone(),
            },
        ),
        _ => false,
    }
}

/// Whether a home's `setupFinished` `record`, from machine `machine_id` on
/// lease `lease_id`, starts the home's `autoStart` scripts: a new checkout
/// whose setup completed or was skipped, of the workspace's current home on
/// that machine, once. The claim is durable and names the record (its
/// lease's journal and sequence), so a record replayed before its ack was
/// saved, also after a restart, starts nothing again. It is made before the
/// scripts start, so a crash or a store error between the two leaves them
/// for the user to start (the record is acked all the same): at most once.
async fn auto_start_due(
    store: &intent_store::Store,
    machine_id: &str,
    lease_id: &str,
    record: &JournalRecord,
) -> bool {
    let (RecordOwner::Home(home_id), JournalBody::SetupFinished { state, .. }) =
        (&record.owner, &record.body)
    else {
        return false;
    };
    // A failed setup leaves the checkout unfit to run services in; the
    // user starts them once it is fixed.
    if *state == intent_core::node_link::SetupState::Failed {
        tracing::info!(home = %home_id, "remote machine: the home's setup failed; no autoStart");
        return false;
    }
    // One transaction: the new home of a move may finish its setup before
    // the switch, which then claims its autoStart from the move.
    match store
        .claim_home_auto_start_or_record_move(
            &record.workspace_id,
            home_id,
            machine_id,
            lease_id,
            record.seq.0,
        )
        .await
    {
        Ok(intent_store::HomeAutoStart::Claimed) => true,
        Ok(intent_store::HomeAutoStart::AtSwitch) => {
            tracing::debug!(home = %home_id, "remote machine: a move's new home finished its setup; autoStart waits for the switch");
            false
        }
        Ok(intent_store::HomeAutoStart::No) => {
            tracing::debug!(home = %home_id, seq = record.seq.0, "remote machine: setup of a home that is not current, or autoStart already claimed; no autoStart");
            false
        }
        Err(error) => {
            tracing::warn!(home = %home_id, %error, "remote machine: could not claim the home's autoStart; it is not started");
            false
        }
    }
}

/// Whether the exposed machine `node_id` holds a home of this backend, is
/// the new home of a move, or owes one a stop: its link is kept up.
///
/// # Errors
///
/// The store's error.
pub(crate) async fn machine_holds_homes(
    store: &intent_store::Store,
    node_id: &str,
) -> Result<bool> {
    Ok(!store.homes_on_machine(node_id).await?.is_empty()
        || !store.list_home_stops(node_id).await?.is_empty()
        || !store.home_moves_to_machine(node_id).await?.is_empty())
}

/// Send `stop` on the machine's current link, when it speaks
/// nodeProtocol 19; otherwise it goes after the next bind.
async fn send_stop(shared: &Shared, stop: &HomeStopRecord) {
    if shared.current_link().is_none() || shared.protocol() < HOMES_PROTOCOL {
        return;
    }
    if let Err(error) = shared.send(&LinkFrame::HomeStop(stop.frame())).await {
        tracing::info!(home = %stop.home_id, %error, "remote machine: could not send a home stop; it goes after the next bind");
    }
}

/// Make sure one worker resends the machine's pending home stops every
/// `tuning.home_stop_resend` while it has a link, until none is left: a
/// `failed` ack (a push that did not land) or one that never came is
/// retried without a reconnect. Called whenever a stop is sent or fails;
/// a worker that ended (no stop left, or no link) is started again.
pub(crate) fn ensure_stop_resends(shared: &Arc<Shared>) {
    use std::sync::atomic::Ordering;
    if shared.home_stop_resends.swap(true, Ordering::SeqCst) {
        return;
    }
    let shared = shared.clone();
    // caller-binding: allow — resends one machine's pending home stops while it is linked
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(shared.tuning.home_stop_resend).await;
            let stops = pending_while_linked(&shared).await;
            if stops.is_empty() {
                shared.home_stop_resends.store(false, Ordering::SeqCst);
                // A stop sent meanwhile found this worker still running:
                // keep going for it, unless another worker took over.
                let again = !pending_while_linked(&shared).await.is_empty();
                if again && !shared.home_stop_resends.swap(true, Ordering::SeqCst) {
                    continue;
                }
                return;
            }
            for stop in stops {
                send_stop(&shared, &stop).await;
            }
        }
    });
}

/// The machine's pending home stops while it has a link; none without
/// one (the next bind sends them and starts a worker again).
async fn pending_while_linked(shared: &Shared) -> Vec<HomeStopRecord> {
    if shared.current_link().is_none() {
        return Vec::new();
    }
    shared
        .store
        .list_home_stops(&shared.machine_id())
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(%error, "remote machine: could not read its home stops");
            Vec::new()
        })
}

/// The switch of the move to `home_id` ([`intent_store::Store::complete_home_move`]:
/// the new home and a `moved` stop of the old one in one store
/// transaction), holding the workspace's home push slot: a push the old
/// home started before the switch lands first, and none of it lands after.
/// `None` when the move is gone. nodes.md "Moving the home" step 3.
///
/// # Errors
///
/// The store's error.
pub(crate) async fn switch_home(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
) -> Result<Option<intent_store::HomeMoveSwitch>> {
    let owner = crate::cloud::owner_started_by(svc, workspace_id).await;
    let _pushes = crate::git::home_push_slot(workspace_id).await?;
    svc.store()
        .complete_home_move(workspace_id, home_id, &owner, &intent_core::now_iso())
        .await
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::send_home_stops`].
pub(crate) async fn send_workspace_stops(svc: &Services, workspace_id: &WorkspaceId) {
    let stops = match svc.store().list_workspace_home_stops(workspace_id).await {
        Ok(stops) => stops,
        Err(error) => {
            tracing::warn!(workspace = %workspace_id, %error, "could not read the workspace's home stops");
            return;
        }
    };
    let Some(feature) = svc.remote_feature() else {
        return;
    };
    for stop in stops {
        let node = match feature.node(svc, &stop.node_id).await {
            Ok(node) => node,
            Err(error) => {
                tracing::info!(home = %stop.home_id, %error, "remote machine: no connector for a home stop yet");
                continue;
            }
        };
        node.shared_ref().set_holds_homes(true);
        if node.shared_ref().current_link().is_some() {
            send_stop(node.shared_ref(), &stop).await;
            ensure_stop_resends(&node.shared());
            continue;
        }
        // The bind sends it.
        // caller-binding: allow — dials the machine owing a home stop; the connector retries with backoff
        tokio::spawn(async move {
            if let Err(error) = node.connect().await {
                tracing::info!(%error, "remote machine: a stopped home's machine is not reachable yet");
            }
        });
    }
}

/// The `home/orphaned/<at>/{head,wip}` refs of `workspace_id` the hub
/// holds, in `repository`.
fn orphaned_refs(
    repository: &std::path::Path,
    workspace_id: &WorkspaceId,
    at: &str,
) -> Vec<String> {
    let Ok(repo) = git2::Repository::open(repository) else {
        return Vec::new();
    };
    ["head", "wip"]
        .into_iter()
        .map(|name| format!("refs/intent/ws/{workspace_id}/home/orphaned/{at}/{name}"))
        .filter(|name| repo.find_reference(name).is_ok())
        .collect()
}

/// Take in a `homeStopAck`: a `removed` ack ends its stop (an ack for
/// another `stopId` changes nothing), and a moved home's notice names the
/// orphaned refs its last push left. A `failed` one keeps the stop, which
/// is sent again on a timer.
pub(crate) async fn stop_acked(shared: &Arc<Shared>, ack: HomeStopAck) {
    let Some(svc) = shared.services() else {
        return;
    };
    if ack.outcome == HomeStopOutcome::Failed {
        tracing::warn!(home = %ack.home_id, "remote machine: a home stop failed on the machine; it is sent again");
        ensure_stop_resends(shared);
        return;
    }
    let finished = match svc
        .store()
        .finish_home_stop(&ack.home_id, &ack.stop_id)
        .await
    {
        Ok(finished) => finished,
        Err(error) => {
            tracing::warn!(home = %ack.home_id, %error, "remote machine: could not record a home stop's ack");
            return;
        }
    };
    let Some(stop) = finished else {
        return;
    };
    if let Ok(holds) = machine_holds_homes(svc.store(), &shared.machine_id()).await {
        shared.set_holds_homes(holds);
    }
    // A cloud home's sandbox goes with its last home, once no agent uses
    // it (nodes.md "Cloud homes" 1).
    crate::cloud::home::release(svc, &shared.machine_id(), None).await;
    let Some(at) = stop
        .orphaned_at
        .clone()
        .filter(|_| stop.reason == HomeStopReason::Moved)
    else {
        return;
    };
    let Ok(workspace) = svc.store().get_workspace(&stop.workspace_id).await else {
        return;
    };
    let repository = std::path::PathBuf::from(crate::final_stop::repo_path(&workspace));
    let workspace_id = stop.workspace_id.clone();
    let refs = tokio::task::spawn_blocking(move || orphaned_refs(&repository, &workspace_id, &at))
        .await
        .unwrap_or_default();
    if refs.is_empty() {
        return;
    }
    let name = svc
        .store()
        .get_exposed_machine(&stop.node_id)
        .await
        .ok()
        .flatten()
        .map(|row| row.name);
    if let Err(error) = svc
        .post_home_orphaned_notice(&stop.workspace_id, name.as_deref(), &refs)
        .await
    {
        tracing::warn!(workspace = %stop.workspace_id, %error, "could not post the old home's notice");
    }
}

impl Shared {
    /// Whether the machine holds a home of this backend (keeps the link up).
    pub(crate) fn set_holds_homes(&self, holds: bool) {
        self.holds_homes
            .store(holds, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use intent_core::node_link::SetupState;
    use intent_core::nodes::NodeCounter;
    use intent_core::Workspace;
    use intent_store::Store;

    use super::*;

    fn setup_finished(home: &str, seq: u64, state: SetupState) -> JournalRecord {
        JournalRecord {
            seq: NodeCounter(seq),
            owner: RecordOwner::Home(HomeId::from(home)),
            workspace_id: WorkspaceId::from("ws-1"),
            turn_id: None,
            recorded_at: intent_core::now_iso(),
            body: JournalBody::SetupFinished {
                state,
                exit_code: Some(0),
                error: None,
                finished_at: intent_core::now_iso(),
            },
        }
    }

    /// A home's `setupFinished` starts its `autoStart` once per checkout:
    /// a record replayed after the orchestrator restarted (the store
    /// reopened, nothing kept in memory) starts nothing, nor does a failed
    /// setup, a record from another machine or of a home that is not
    /// current; a later checkout's record starts them again.
    #[tokio::test]
    async fn setup_finished_starts_auto_start_once_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("homes.db");
        let ws = WorkspaceId::from("ws-1");
        let completed = setup_finished("h1", 7, SetupState::Completed);
        {
            let store = Store::open(&path).await.unwrap();
            store
                .insert_workspace(&Workspace {
                    id: ws.clone(),
                    title: "ws-1".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            store
                .set_workspace_home(
                    &ws,
                    &WorkspaceHome {
                        home_id: HomeId::from("h1"),
                        node_id: Some("node-a".into()),
                        cloud_machine_id: None,
                        name: "Machine A".into(),
                        state: HomeState::Attached,
                        since: intent_core::now_iso(),
                        last_backup_at: None,
                        backup_warning: false,
                        setup: None,
                    },
                )
                .await
                .unwrap();
            let failed = setup_finished("h1", 6, SetupState::Failed);
            assert!(!auto_start_due(&store, "node-a", "lease-1", &failed).await);
            assert!(!auto_start_due(&store, "node-b", "lease-9", &completed).await);
            let stale = setup_finished("h0", 7, SetupState::Completed);
            assert!(!auto_start_due(&store, "node-a", "lease-1", &stale).await);
            assert!(auto_start_due(&store, "node-a", "lease-1", &completed).await);
        }
        let store = Store::open(&path).await.unwrap();
        assert!(
            !auto_start_due(&store, "node-a", "lease-1", &completed).await,
            "the replay after a restart starts nothing"
        );
        let next = setup_finished("h1", 12, SetupState::Skipped);
        assert!(auto_start_due(&store, "node-a", "lease-1", &next).await);
        assert!(!auto_start_due(&store, "node-a", "lease-1", &next).await);
    }
}
