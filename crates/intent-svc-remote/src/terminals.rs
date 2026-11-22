//! Terminals on a workspace home, domain side: find the workspace's home
//! and its linked machine, then start the terminal on its link
//! ([`crate::link::terminals`]).

use std::sync::Arc;

use intent_core::node_link::HOMES_PROTOCOL;
use intent_core::{Result, WorkspaceId};
use intent_pty::home::{HomeExec, HomeExecOpen, HomeTerminalOpen};
use intent_pty::{PtyHost, PtyId};
use intent_services::Services;

pub(crate) use crate::link::terminals::{
    home_moving, home_starting, home_unavailable, HomeTerminals,
};
use crate::RemoteServicesExt as _;

/// `home-unavailable` while `home` is detached: nothing waits on a dial to
/// a machine whose link has been down for the link-loss grace, but its
/// connector is asked to dial again.
pub(crate) fn ensure_attached(svc: &Services, home: &intent_core::WorkspaceHome) -> Result<()> {
    if home.state == intent_core::HomeState::Moving {
        return Err(home_moving(home));
    }
    if home.state == intent_core::HomeState::Detached {
        crate::home_state::redial(svc, home);
        return Err(home_unavailable(
            home,
            "its machine's link has been down for the link-loss grace",
        ));
    }
    Ok(())
}

/// Dial `workspace_id`'s home machine ahead of a terminal start, outside
/// the workspace's archive fence; nothing when it has no home on one.
///
/// # Errors
///
/// `home-unavailable` when the home's machine cannot be reached.
pub(crate) async fn connect_home(svc: &Services, workspace_id: &WorkspaceId) -> Result<()> {
    if !svc.workspace_homes() {
        return Ok(());
    }
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(());
    };
    let home = crate::cloud::home::ready(svc, workspace_id, home).await?;
    ensure_attached(svc, &home)?;
    let Some(node_id) = home.node_id.clone() else {
        return Ok(());
    };
    let feature = svc
        .remote_feature()
        .ok_or_else(|| home_unavailable(&home, "no remote machine feature"))?;
    let node = match feature.node(svc, &node_id).await {
        Ok(node) => node,
        Err(error) => {
            return Err(crate::cloud::home::unreachable(svc, workspace_id, &home, &error).await)
        }
    };
    match node.connect().await {
        Ok(()) => Ok(()),
        Err(error) => Err(crate::cloud::home::unreachable(svc, workspace_id, &home, &error).await),
    }
}

/// The connected machine of `workspace_id`'s home, speaking nodeProtocol
/// 19; `None` when the workspace has no home on a machine.
/// The workspace's home and its connected machine, with the use of the
/// machine the caller holds while it opens its terminal
/// ([`Shared::begin_use`]).
///
/// [`Shared::begin_use`]: crate::connector::Shared::begin_use
async fn linked_home(
    svc: &Services,
    workspace_id: &WorkspaceId,
) -> Result<
    Option<(
        intent_core::WorkspaceHome,
        crate::connector::RemoteNode,
        tokio::sync::OwnedRwLockReadGuard<()>,
    )>,
> {
    if !svc.workspace_homes() {
        return Ok(None);
    }
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(None);
    };
    let home = crate::cloud::home::ready(svc, workspace_id, home).await?;
    ensure_attached(svc, &home)?;
    let Some(node_id) = home.node_id.clone() else {
        return Ok(None);
    };
    let feature = svc
        .remote_feature()
        .ok_or_else(|| home_unavailable(&home, "no remote machine feature"))?;
    let node = match feature.node(svc, &node_id).await {
        Ok(node) => node,
        Err(error) => {
            return Err(crate::cloud::home::unreachable(svc, workspace_id, &home, &error).await)
        }
    };
    let gate = node.shared_ref().begin_use().await;
    if let Err(error) = node.connect().await {
        return Err(crate::cloud::home::unreachable(svc, workspace_id, &home, &error).await);
    }
    if node.shared_ref().protocol() < HOMES_PROTOCOL {
        return Err(home_unavailable(
            &home,
            "its machine needs an intentd update for workspace homes",
        ));
    }
    Ok(Some((home, node, gate)))
}

/// Start a terminal (or, in `exec` mode, a command) on `workspace_id`'s
/// home as a remote PTY in `pty`. `None` when the workspace has no home on
/// a machine, so the caller runs it here.
///
/// # Errors
///
/// `home-unavailable` when the home's machine cannot be reached or does
/// not speak nodeProtocol 19.
pub(crate) async fn open(
    svc: &Services,
    workspace_id: &WorkspaceId,
    pty: &Arc<PtyHost>,
    open: HomeTerminalOpen,
) -> Result<Option<PtyId>> {
    let Some((home, node, _use)) = linked_home(svc, workspace_id).await? else {
        return Ok(None);
    };
    crate::link::terminals::open_on(node.shared_ref(), &home, workspace_id, pty, open).map(Some)
}

/// Start a `host.execStream` process on `workspace_id`'s home: an
/// exec-mode `ptyOpen` whose output arrives by stream, then its end.
/// `None` when the workspace has no home on a machine.
///
/// # Errors
///
/// `home-unavailable` when the home's machine cannot be reached or does
/// not speak nodeProtocol 19.
pub(crate) async fn open_exec(
    svc: &Services,
    workspace_id: &WorkspaceId,
    open: HomeExecOpen,
) -> Result<Option<HomeExec>> {
    let Some((home, node, _use)) = linked_home(svc, workspace_id).await? else {
        return Ok(None);
    };
    Ok(Some(crate::link::terminals::open_exec_on(
        node.shared_ref(),
        &home,
        workspace_id,
        open,
    )))
}
