//! Home runs of saved scripts, domain side: find the workspace's home and
//! its linked machine, then start or stop the run on its link
//! ([`crate::link::home_scripts`]).

use std::sync::Arc;
use std::time::Duration;

use intent_core::node_link::HOMES_PROTOCOL;
use intent_core::{Result, WorkspaceId};
use intent_pty::home::HomeScriptOpen;
use intent_pty::{PtyHost, PtyId};
use intent_services::Services;

pub(crate) use crate::link::home_scripts::Stopped;
use crate::terminals::home_unavailable;
use crate::RemoteServicesExt as _;

/// Start one process of `workspace_id`'s home run of a saved script as a
/// remote PTY in `pty`. `None` when the workspace has no home on a
/// machine, so the caller runs it here.
///
/// # Errors
///
/// `home-unavailable` when the home's machine is not linked now (the
/// caller dials it first, outside its locks) or does not speak
/// nodeProtocol 19.
pub(crate) async fn open(
    svc: &Services,
    workspace_id: &WorkspaceId,
    pty: &Arc<PtyHost>,
    open: HomeScriptOpen,
) -> Result<Option<PtyId>> {
    if !svc.workspace_homes() {
        return Ok(None);
    }
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(None);
    };
    let home = crate::cloud::home::ready(svc, workspace_id, home).await?;
    crate::terminals::ensure_attached(svc, &home)?;
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
    // The caller dialed before taking its script locks, so no lock waits on
    // a dial here. The start is a use of the machine: it waits out an idle
    // pause, and when that pause closed the link meanwhile, it dials once
    // more (bounded), which resumes the sandbox.
    let _use = node.begin_use().await;
    if node.current_link().is_none() && feature.cloud.is_parked(&node_id) {
        if let Err(error) = node.connect().await {
            return Err(crate::cloud::home::unreachable(svc, workspace_id, &home, &error).await);
        }
    }
    if node.protocol() < HOMES_PROTOCOL {
        return Err(home_unavailable(
            &home,
            "its machine needs an intentd update for workspace homes",
        ));
    }
    node.start_home_script(&home, workspace_id, pty, open)
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::stop_home_script`].
pub(crate) async fn stop(
    svc: &Services,
    workspace_id: &WorkspaceId,
    pty: &Arc<PtyHost>,
    id: PtyId,
    wait: Duration,
) -> Result<()> {
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Ok(());
    };
    let Some(node_id) = home.node_id.clone() else {
        return Ok(());
    };
    let Some(feature) = svc.remote_feature() else {
        return Ok(());
    };
    let Ok(node) = feature.node(svc, &node_id).await else {
        return Ok(());
    };
    match node.stop_home_script(pty, id, wait).await {
        Stopped::Ended | Stopped::NotHeld => Ok(()),
        Stopped::Unconfirmed => Err(home_unavailable(
            &home,
            "the run's stop was not confirmed in time; it is still stopping there",
        )),
    }
}
