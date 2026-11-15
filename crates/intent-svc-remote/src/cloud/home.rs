//! Cloud homes, orchestrator side (monorepo
//! `docs/protocol/methods/nodes.md`, "Choosing the home" and "Cloud
//! homes"): a cloud entry chosen as a workspace's home is recorded with no
//! machine and starts lazily. The first agent sandbox of that entry in the
//! workspace becomes the home, or a call that needs the home starts a
//! home-only sandbox. While it starts the home is `starting`; a call waits
//! up to [`START_WAIT`] for it, then fails `home-unavailable` with
//! `starting: true`. A start that fails detaches the home at once.
//!
//! A home whose sandbox was deleted (paused too long, or the provider lost
//! it) is rebuilt: in the store transaction that drops the sandbox's homes
//! ([`sandbox_removed`]) it gets a new `homeId` with no sandbox, so its
//! next use starts a new sandbox of the same entry, which opens it from
//! the hub's `home/head` and `home/wip` and posts a notice ([`rebuilt`]).
//! It is never detached for the loss; a rebuild that fails is, at once.

use std::time::Duration;

use intent_core::{AgentId, HomeId, HomeState, Result, WorkspaceHome, WorkspaceId};
use intent_services::feature_api::Services;
use tokio::sync::watch;

use super::{StartFailure, StartOutcome};
use crate::RemoteServicesExt as _;

/// How long a call waits for a starting cloud home.
pub(crate) const START_WAIT: Duration = Duration::from_secs(10);

/// The key a home-only start of `workspace_id` registers under (in the
/// starts in flight, and as the sandbox's `agentId` tag and row owner).
pub(crate) fn start_key(workspace_id: &WorkspaceId) -> String {
    format!("home-{workspace_id}")
}

/// A new workspace's home on cloud entry `cloud_machine_id` (named
/// `name`): recorded with no machine; its sandbox starts on first use.
pub(crate) fn new_home(cloud_machine_id: &str, name: &str) -> WorkspaceHome {
    WorkspaceHome {
        home_id: HomeId::new(),
        node_id: None,
        cloud_machine_id: Some(cloud_machine_id.to_owned()),
        name: name.to_owned(),
        state: HomeState::Attached,
        since: intent_core::now_iso(),
        last_backup_at: None,
        backup_warning: false,
    }
}

/// Whether a call on `home` waits for a sandbox: a cloud home that is
/// starting, or that runs nowhere yet.
fn needs_start(home: &WorkspaceHome) -> bool {
    home.cloud_machine_id.is_some()
        && match home.state {
            HomeState::Starting => true,
            HomeState::Attached => home.node_id.is_none(),
            HomeState::Detached | HomeState::Moving => false,
        }
}

/// Wake every call waiting for a cloud home.
pub(crate) fn changed(svc: &Services) {
    if let Some(feature) = svc.remote_feature() {
        feature.cloud.home_changed.notify_waiters();
    }
}

async fn announce(svc: &Services, workspace_id: &WorkspaceId) {
    changed(svc);
    svc.publish_workspace_home_changed(workspace_id).await;
}

/// The workspace's home when a new sandbox of entry `cloud_machine_id`
/// becomes it: a cloud home of that entry that runs nowhere and is not
/// moving.
pub(crate) async fn claimable(
    svc: &Services,
    workspace_id: &WorkspaceId,
    cloud_machine_id: &str,
) -> Option<WorkspaceHome> {
    if !svc.workspace_homes() {
        return None;
    }
    let home = svc.store().get_workspace_home(workspace_id).await.ok()??;
    (home.cloud_machine_id.as_deref() == Some(cloud_machine_id)
        && home.node_id.is_none()
        && home.state != HomeState::Moving)
        .then_some(home)
}

/// `home` is starting: announce it unless it already was, or a sandbox
/// already holds it.
pub(crate) async fn mark_starting(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
) {
    match svc
        .store()
        .mark_cloud_home_starting(workspace_id, &home.home_id, &intent_core::now_iso())
        .await
    {
        Ok(true) => {
            tracing::info!(workspace = %workspace_id, "cloud home: starting");
            announce(svc, workspace_id).await;
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(workspace = %workspace_id, %error, "cloud home: could not record the start");
        }
    }
}

/// At start: cloud homes a restart left `starting` with no sandbox are
/// unstarted again (they start at their next use), and announced.
pub(crate) async fn reset_after_restart(svc: &Services) {
    match svc
        .store()
        .reset_unstarted_cloud_homes(&intent_core::now_iso())
        .await
    {
        Ok(workspaces) => {
            for workspace_id in workspaces {
                svc.publish_workspace_home_changed(&workspace_id).await;
            }
        }
        Err(error) => {
            tracing::warn!(%error, "cloud home: could not reset the homes a restart left starting");
        }
    }
}

/// Whether a home-only start of `workspace_id` is in flight.
pub(crate) fn home_start_in_flight(svc: &Services, workspace_id: &WorkspaceId) -> bool {
    svc.remote_feature().is_some_and(|feature| {
        crate::lock(&feature.cloud.starts).contains_key(&start_key(workspace_id))
    })
}

/// Record machine `machine_id` as cloud home `home_id`'s sandbox; `false`
/// when another start took it first (or it is no longer the home).
///
/// # Errors
///
/// The store's error.
pub(crate) async fn claim(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
    cloud_machine_id: &str,
    machine_id: &str,
) -> Result<bool> {
    let claimed = svc
        .store()
        .claim_cloud_home(workspace_id, home_id, cloud_machine_id, machine_id)
        .await?;
    if claimed {
        tracing::info!(workspace = %workspace_id, machine = %machine_id, "cloud home: the sandbox holds the workspace's home");
        changed(svc);
    }
    Ok(claimed)
}

/// Open home `home_id` on its new machine `machine_id` and wait for its
/// checkout and setup; it is `attached` once the machine answers `ready`.
///
/// # Errors
///
/// The open's error.
pub(crate) async fn open(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
    machine_id: &str,
) -> Result<()> {
    let reply = crate::homes::send_open(svc, workspace_id, home_id, machine_id, None).await?;
    crate::homes::settle_open(svc, workspace_id, home_id, reply).await
}

/// The new sandbox `machine_id` of an agent becomes the workspace's cloud
/// home `home`, whose checkout opens (with its setup) before the agent
/// launches. Whether it did: nothing changes when another start took the
/// home first, and a failed open detaches the home; the agent still
/// launches either way, in its own checkout.
///
/// # Errors
///
/// The store's error.
pub(crate) async fn take(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
    cloud_machine_id: &str,
    machine_id: &str,
) -> Result<bool> {
    if !claim(
        svc,
        workspace_id,
        &home.home_id,
        cloud_machine_id,
        machine_id,
    )
    .await?
    {
        return Ok(false);
    }
    if let Err(error) = open(svc, workspace_id, &home.home_id, machine_id).await {
        tracing::warn!(workspace = %workspace_id, %error, "cloud home: the home did not open on the agent's sandbox");
        start_failed(svc, workspace_id, &home.home_id, Some(machine_id)).await;
        return Ok(false);
    }
    Ok(true)
}

/// A start of cloud home `home_id` failed (its sandbox `machine_id`, when
/// it got that far, is gone): the home is detached at once.
pub(crate) async fn start_failed(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
    machine_id: Option<&str>,
) {
    match svc
        .store()
        .detach_cloud_home(workspace_id, home_id, machine_id, &intent_core::now_iso())
        .await
    {
        Ok(true) => {
            tracing::info!(workspace = %workspace_id, "cloud home: the start failed; the home is detached");
            announce(svc, workspace_id).await;
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(workspace = %workspace_id, %error, "cloud home: could not detach the home after a failed start");
        }
    }
}

/// An agent start that raced for cloud home `home` failed (`made`: its
/// sandbox's machine, when it got that far). A home the start holds is
/// detached. An unheld one is left to a home-only start in flight, which
/// may still become the home and settles it itself; with none, it is
/// detached too. A home another sandbox holds is untouched.
pub(crate) async fn agent_start_failed(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
    made: Option<&str>,
) {
    let held = made.is_some()
        && svc
            .store()
            .get_workspace_home(workspace_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|current| current.node_id.as_deref() == made);
    if held || !home_start_in_flight(svc, workspace_id) {
        start_failed(svc, workspace_id, &home.home_id, made).await;
    }
}

/// Cloud machine `machine_id`'s sandbox is gone (deleted, or the provider
/// lost it), before its row goes: each attached home it held becomes a new
/// home to rebuild at its next use (nodes.md "Cloud homes" 5), and the
/// home stops it owed are dropped. The workspaces whose home changed, to
/// announce with [`announce_rebuilds`] once the row is gone.
///
/// # Errors
///
/// The store's error.
pub(crate) async fn sandbox_removed(
    svc: &Services,
    machine_id: &str,
) -> Result<Vec<intent_store::HomeRebuild>> {
    let rebuilds = svc
        .store()
        .rebuild_cloud_homes_on_machine(machine_id, &intent_core::now_iso())
        .await?;
    for rebuild in &rebuilds {
        tracing::info!(workspace = %rebuild.workspace_id, machine = %machine_id, from = %rebuild.from_home_id, home = %rebuild.home_id, "cloud home: its sandbox is gone; the home is rebuilt at its next use");
    }
    Ok(rebuilds)
}

/// Announce the homes [`sandbox_removed`] renewed.
pub(crate) async fn announce_rebuilds(svc: &Services, rebuilds: &[intent_store::HomeRebuild]) {
    if rebuilds.is_empty() {
        return;
    }
    changed(svc);
    for rebuild in rebuilds {
        svc.publish_workspace_home_changed(&rebuild.workspace_id)
            .await;
    }
}

/// Home `home_id` opened: when it rebuilt a cloud home whose sandbox was
/// deleted, the workspace gets its notice, once.
pub(crate) async fn rebuilt(svc: &Services, workspace_id: &WorkspaceId, home_id: &HomeId) {
    match svc.store().finish_home_rebuild(workspace_id, home_id).await {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            tracing::warn!(workspace = %workspace_id, %error, "cloud home: could not record the rebuild");
            return;
        }
    }
    tracing::info!(workspace = %workspace_id, home = %home_id, "cloud home: rebuilt from its backup");
    let Ok(Some(home)) = svc.store().get_workspace_home(workspace_id).await else {
        return;
    };
    if let Err(error) = svc
        .post_home_rebuilt_notice(workspace_id, &home.name, home.last_backup_at.as_deref())
        .await
    {
        tracing::warn!(workspace = %workspace_id, %error, "cloud home: could not post the rebuilt home's notice");
    }
}

/// Whether a call that could not reach cloud home `home`'s sandbox should
/// be told the home starts: the sandbox is gone (its loss is being
/// recorded, or the home was renewed for a rebuild meanwhile), so the next
/// call starts a new one.
pub(crate) async fn sandbox_gone(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
) -> bool {
    let Some(node_id) = home.node_id.as_deref() else {
        return false;
    };
    if home.cloud_machine_id.is_none() {
        return false;
    }
    if svc
        .remote_feature()
        .is_some_and(|feature| feature.cloud.loss(node_id).is_some())
    {
        return true;
    }
    let Ok(Some(current)) = svc.store().get_workspace_home(workspace_id).await else {
        return false;
    };
    matches!(
        svc.store().home_rebuild_of(workspace_id, &current.home_id).await,
        Ok(Some(from)) if from == home.home_id
    )
}

/// The error of a call that could not reach `home`'s machine: `starting`
/// when its cloud sandbox is gone ([`sandbox_gone`]), else
/// `home-unavailable`.
pub(crate) async fn unreachable(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
    error: &intent_core::Error,
) -> intent_core::Error {
    if sandbox_gone(svc, workspace_id, home).await {
        return crate::terminals::home_starting(home);
    }
    crate::terminals::home_unavailable(home, &error.to_string())
}

/// Delete cloud machine `machine_id` in the background once nothing needs
/// it ([`needed`]). `ending` is an agent ending for good now.
///
/// Boxed: the delete's final stops lead back here.
pub(crate) fn release<'a>(
    svc: &'a Services,
    machine_id: &'a str,
    ending: Option<&'a AgentId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(release_inner(svc, machine_id, ending))
}

async fn release_inner(svc: &Services, machine_id: &str, ending: Option<&AgentId>) {
    let Ok(Some(row)) = svc.store().get_exposed_machine(machine_id).await else {
        return;
    };
    if row.cloud.is_none() || needed(svc, &row, ending).await {
        return;
    }
    let svc = svc.clone();
    let ending = ending.cloned();
    // caller-binding: allow — deletes one cloud machine nothing needs any more
    tokio::spawn(async move {
        let Some(feature) = svc.remote_feature() else {
            return;
        };
        let client = super::saved_client(&svc, &row).await;
        super::start::delete_unneeded_machine(
            feature,
            &svc,
            &row,
            client.as_deref(),
            ending.as_ref(),
        )
        .await;
    });
}

/// Whether cloud machine `row` is still needed (nodes.md "Cloud homes" 1):
/// it holds a home, owes a home stop or is a move's target, or an agent
/// uses it. `ending` is an agent ending for good now; when it is the one
/// the sandbox was started for, its own sub-agents there are stopped with
/// it, as for any agent sandbox, but other agents still count. `true` when
/// that cannot be read, so a sandbox is never deleted on a guess.
pub(crate) async fn needed(
    svc: &Services,
    row: &intent_core::machines::ExposedMachine,
    ending: Option<&AgentId>,
) -> bool {
    let Some(cloud) = &row.cloud else {
        return true;
    };
    let store = svc.store();
    match crate::homes::machine_holds_homes(store, &row.id).await {
        Ok(false) => {}
        Ok(true) => {
            tracing::info!(machine = %row.id, "cloud machine: kept, it holds a workspace's home");
            return true;
        }
        Err(error) => {
            tracing::warn!(machine = %row.id, %error, "cloud machine: could not read its homes; kept");
            return true;
        }
    }
    let starter = AgentId::from(cloud.agent_id.as_str());
    let starter_ends = ending == Some(&starter);
    if !starter_ends && uses(svc, &starter).await {
        return true;
    }
    let placed = match super::start::placed_agents(store, &row.id).await {
        Ok(placed) => placed,
        Err(error) => {
            tracing::warn!(machine = %row.id, %error, "cloud machine: could not read its agents; kept");
            return true;
        }
    };
    for agent in placed {
        if Some(&agent.agent_id) == ending || agent.agent_id == starter {
            continue;
        }
        if starter_ends {
            match super::start::descends_from(svc, &agent.agent_id, starter.as_str()).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(_) => return true,
            }
        }
        if uses(svc, &agent.agent_id).await {
            return true;
        }
    }
    false
}

/// Whether agent `agent_id` still uses its machine: it has not ended, and
/// its workspace is neither archived nor deleted. `true` when that cannot
/// be read, so a sandbox is never deleted on a guess.
async fn uses(svc: &Services, agent_id: &AgentId) -> bool {
    let session = match svc.store().get_agent_session_summary(agent_id).await {
        Ok(session) => session,
        Err(intent_core::Error::NotFound(_)) => return false,
        Err(_) => return true,
    };
    if crate::final_stop::session_ended(&session) {
        return false;
    }
    match svc.store().get_workspace(&session.workspace_id).await {
        Ok(workspace) => matches!(
            workspace.status,
            intent_core::WorkspaceStatus::Active | intent_core::WorkspaceStatus::Inactive
        ),
        Err(intent_core::Error::NotFound(_)) => false,
        Err(_) => true,
    }
}

/// The home a call on `workspace_id` uses (nodes.md "Workspace homes"): a
/// cloud home that is starting or runs nowhere is started, and the call
/// waits up to [`START_WAIT`] for it. A detached cloud home with no
/// sandbox is started again for the next call (the app's **Wait**); this
/// call fails as detached.
///
/// # Errors
///
/// `home-unavailable` with `starting: true` when the home still starts
/// after the wait; the store's error.
pub(crate) async fn ready(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: WorkspaceHome,
) -> Result<WorkspaceHome> {
    let restart = home.cloud_machine_id.is_some()
        && home.state == HomeState::Detached
        && home.node_id.is_none();
    if !needs_start(&home) && !restart {
        return Ok(home);
    }
    let Ok(feature) = super::feature(svc) else {
        return Ok(home);
    };
    if restart {
        ensure_started(svc, feature, workspace_id, &home).await;
        return Ok(home);
    }
    wait_started(svc, feature, workspace_id, home).await
}

async fn wait_started(
    svc: &Services,
    feature: &crate::RemoteFeature,
    workspace_id: &WorkspaceId,
    mut home: WorkspaceHome,
) -> Result<WorkspaceHome> {
    let deadline = tokio::time::Instant::now() + START_WAIT;
    loop {
        let woken = feature.cloud.home_changed.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        home = match svc.store().get_workspace_home(workspace_id).await? {
            Some(current) => current,
            None => return Ok(home),
        };
        if !needs_start(&home) {
            return Ok(home);
        }
        ensure_started(svc, feature, workspace_id, &home).await;
        if tokio::time::timeout_at(deadline, woken).await.is_err() {
            return Err(crate::terminals::home_starting(&home));
        }
    }
}

/// Make sure a start of cloud home `home` is in flight: none is begun while
/// a sandbox start of its entry runs for the workspace (an agent's becomes
/// the home). A home whose sandbox holds it but whose checkout was not
/// opened (a restart cut the start) is opened again.
async fn ensure_started(
    svc: &Services,
    feature: &crate::RemoteFeature,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
) {
    let Some(entry) = home.cloud_machine_id.clone() else {
        return;
    };
    let key = start_key(workspace_id);
    let (sender, receiver) = watch::channel::<StartOutcome>(None);
    {
        let mut starts = crate::lock(&feature.cloud.starts);
        if starts
            .values()
            .any(|start| start.workspace == workspace_id.as_str() && start.entry == entry)
        {
            return;
        }
        starts.insert(
            key.clone(),
            super::Start {
                entry: entry.clone(),
                workspace: workspace_id.to_string(),
                outcome: receiver,
            },
        );
    }
    mark_starting(svc, workspace_id, home).await;
    let svc = svc.clone();
    let workspace_id = workspace_id.clone();
    let home_id = home.home_id.clone();
    let node_id = home.node_id.clone();
    // caller-binding: allow — starts one workspace's cloud home in the background
    tokio::spawn(async move {
        let started = match &node_id {
            Some(node_id) => {
                let opened = open(&svc, &workspace_id, &home_id, node_id).await;
                if opened.is_err() {
                    start_failed(&svc, &workspace_id, &home_id, Some(node_id)).await;
                }
                opened
            }
            None => super::start::start_home(&svc, &workspace_id, &home_id, &entry).await,
        };
        if let Err(error) = &started {
            tracing::warn!(workspace = %workspace_id, %error, "cloud home: the home did not start");
        }
        if let Ok(feature) = super::feature(&svc) {
            crate::lock(&feature.cloud.starts).remove(&key);
        }
        let _ = sender.send(Some(started.map_err(|error| StartFailure::of(&error))));
        changed(&svc);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home(state: HomeState, node_id: Option<&str>, cloud: bool) -> WorkspaceHome {
        WorkspaceHome {
            node_id: node_id.map(str::to_owned),
            cloud_machine_id: cloud.then(|| "e2b".to_owned()),
            state,
            ..new_home("e2b", "E2B")
        }
    }

    /// A call waits for a cloud home that is starting or runs nowhere, and
    /// for nothing else: a machine home, a running cloud home, a detached
    /// or moving one.
    #[test]
    fn a_call_waits_only_for_a_cloud_home_that_is_not_running() {
        assert!(needs_start(&home(HomeState::Attached, None, true)));
        assert!(needs_start(&home(HomeState::Starting, None, true)));
        assert!(needs_start(&home(HomeState::Starting, Some("m"), true)));
        assert!(!needs_start(&home(HomeState::Attached, Some("m"), true)));
        assert!(!needs_start(&home(HomeState::Detached, None, true)));
        assert!(!needs_start(&home(HomeState::Moving, None, true)));
        assert!(!needs_start(&home(HomeState::Attached, Some("m"), false)));
    }
}
