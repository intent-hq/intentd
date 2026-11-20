//! `workspace.setHome`, orchestrator side (monorepo
//! `docs/protocol/methods/nodes.md`, "Moving the home"): check the target,
//! record the move (the home is `moving` from then on), then run its steps
//! in the background. The move is kept in the store, so a restart picks it
//! up where it was:
//!
//! 1. the old home's last backup, when it is attached;
//! 2. `homeOpen` of the new home from `home/head` and `home/wip`; a move
//!    to a cloud entry first starts a home-only sandbox and records it on
//!    the move. Clients see a move to a cloud home `starting` meanwhile
//!    (`HomeMove::shown`);
//! 3. the switch: the new home and the old one's `moved` stop in one store
//!    transaction, then the stop goes out.
//!
//! The agents that shared the old home stay on their machine; each gets a
//! checkout of its own at its next start (`homes::leave_moved_home`).
//!
//! A failure before the switch rolls back: the old home gets its old state,
//! and the new home, which its machine may have opened, gets a stop.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use intent_core::node_link::HomeOpenOutcome;
use intent_core::{AgentId, Error, HomeState, Result, WorkspaceHome, WorkspaceId};
use intent_services::feature_api::Services;
use intent_store::HomeMove;
use serde_json::{json, Value};

use crate::homes::{Candidate, HomeReply};
use crate::RemoteServicesExt as _;

/// The workspaces whose move runs in this process: one runner each. `true`
/// asks the runner to look again before it ends: a move was started while
/// it was finishing the previous one.
static RUNNING: LazyLock<Mutex<HashMap<WorkspaceId, bool>>> = LazyLock::new(Mutex::default);

/// What `to` names.
#[derive(Debug, PartialEq, Eq)]
enum Target {
    Agent(AgentId),
    Machine(String),
    Cloud(String),
}

fn coded(code: &str, message: impl Into<String>) -> Error {
    Error::remote_machine(false, message, json!({ "code": code }))
}

fn home_moving() -> Error {
    coded(
        "home-moving",
        "The workspace's home is already moving. Wait for that move to end.",
    )
}

pub(crate) fn target_unavailable(detail: &str) -> Error {
    coded(
        "home-target-unavailable",
        format!("That can't be the workspace's home now: {detail}."),
    )
}

fn text<'a>(object: &'a serde_json::Map<String, Value>, name: &str) -> Result<&'a str> {
    match object.get(name) {
        Some(Value::String(value)) if !value.trim().is_empty() && value.len() <= 256 => {
            Ok(value.as_str())
        }
        None | Some(Value::Null) => Err(Error::InvalidParams(format!("to.{name} is required"))),
        Some(_) => Err(Error::InvalidParams(format!(
            "to.{name} must be a non-blank string of at most 256 bytes"
        ))),
    }
}

fn parse_target(params: &Value) -> Result<Target> {
    let Some(to) = params.get("to").and_then(Value::as_object) else {
        return Err(Error::InvalidParams("to is required".into()));
    };
    if to.contains_key("agentId") {
        return Ok(Target::Agent(AgentId::from(text(to, "agentId")?)));
    }
    match to.get("kind").and_then(Value::as_str) {
        Some("machine") => Ok(Target::Machine(text(to, "nodeId")?.to_owned())),
        Some("cloud") => Ok(Target::Cloud(text(to, "cloudMachineId")?.to_owned())),
        _ => Err(Error::InvalidParams(
            "to must be { agentId }, { kind: \"machine\", nodeId } or { kind: \"cloud\", cloudMachineId }"
                .into(),
        )),
    }
}

/// The new home `target` names, with a fresh `homeId`: a machine, a
/// cloud entry (a sandbox starts for it) or an agent's machine or sandbox.
async fn resolve(
    svc: &Services,
    workspace_id: &WorkspaceId,
    target: Target,
) -> Result<WorkspaceHome> {
    let node_id = match target {
        Target::Machine(node_id) => node_id,
        Target::Cloud(cloud_machine_id) => {
            return crate::cloud::home::entry_target(svc, &cloud_machine_id).await;
        }
        Target::Agent(agent_id) => {
            let record = svc
                .store()
                .get_remote_agent(&agent_id)
                .await?
                .filter(|record| &record.workspace_id == workspace_id)
                .ok_or_else(|| {
                    target_unavailable("that agent is not on a machine of this workspace")
                })?;
            let on_sandbox = svc
                .store()
                .get_exposed_machine(&record.node_id)
                .await?
                .is_some_and(|row| row.is_cloud());
            if on_sandbox {
                return crate::cloud::home::sandbox_target(svc, &record.node_id).await;
            }
            record.node_id
        }
    };
    match crate::homes::machine_candidate(svc, &node_id).await {
        Candidate::Home(home) => Ok(home),
        Candidate::TooOld(required) => Err(Error::MachineUpdateRequired(Box::new(required))),
        Candidate::No => Err(target_unavailable(
            "the machine is unknown, removed, offline or does not allow homes",
        )),
    }
}

/// `workspace.setHome`: start a move of the workspace's home to `to`. The
/// result is the home, now `moving`; the move goes on in the background.
pub(crate) async fn set_home(
    svc: &Services,
    workspace_id: &WorkspaceId,
    params: &Value,
) -> Result<Value> {
    if !svc.workspace_homes() {
        return Err(intent_services::homes_unsupported());
    }
    let target = parse_target(params)?;
    svc.store().get_workspace(workspace_id).await?;
    let home = svc
        .store()
        .get_workspace_home(workspace_id)
        .await?
        .ok_or_else(|| Error::InvalidParams("the workspace has no home".into()))?;
    if home.state == HomeState::Moving || svc.store().get_home_move(workspace_id).await?.is_some() {
        return Err(home_moving());
    }
    let target = resolve(svc, workspace_id, target).await?;
    let home_move = HomeMove {
        workspace_id: workspace_id.clone(),
        target,
        from_home_id: home.home_id.clone(),
        from_state: home.state,
        from_since: home.since.clone(),
        pushed: false,
        started_at: intent_core::now_iso(),
    };
    if !svc.store().begin_home_move(&home_move).await? {
        return Err(home_moving());
    }
    tracing::info!(workspace = %workspace_id, from = %home.home_id, to = %home_move.target.home_id, "the workspace's home is moving");
    end_old_terminals(svc, &home).await;
    // The reply is the moving home, also when the move ends before it is sent.
    let home = WorkspaceHome {
        state: HomeState::Moving,
        since: home_move.started_at.clone(),
        ..home
    };
    svc.publish_workspace_home_changed(workspace_id).await;
    spawn(svc, workspace_id);
    Ok(json!({ "home": home }))
}

/// The old home's open terminals and exec processes end now.
async fn end_old_terminals(svc: &Services, home: &WorkspaceHome) {
    let Some(node) = home
        .node_id
        .as_deref()
        .and_then(|node_id| svc.remote_feature()?.running_node(node_id))
    else {
        return;
    };
    let shared = node.shared_ref();
    shared.terminals.end_home(shared, &home.home_id).await;
}

/// At start: go on with every move a restart interrupted.
pub(crate) async fn resume(svc: &Services) {
    let moves = match svc.store().list_home_moves().await {
        Ok(moves) => moves,
        Err(error) => {
            tracing::warn!(%error, "could not read the home moves to resume");
            return;
        }
    };
    for home_move in moves {
        tracing::info!(workspace = %home_move.workspace_id, "resuming the move of the workspace's home");
        spawn(svc, &home_move.workspace_id);
    }
}

fn spawn(svc: &Services, workspace_id: &WorkspaceId) {
    {
        let mut running = crate::lock(&RUNNING);
        if let Some(again) = running.get_mut(workspace_id) {
            *again = true;
            return;
        }
        running.insert(workspace_id.clone(), false);
    }
    let svc = svc.clone();
    let workspace_id = workspace_id.clone();
    // Its own task, as the daemon: the move outlives the request.
    intent_core::spawn_daemon(async move {
        loop {
            run(&svc, &workspace_id).await;
            let mut running = crate::lock(&RUNNING);
            if running.get(&workspace_id) == Some(&true) {
                running.insert(workspace_id.clone(), false);
                continue;
            }
            running.remove(&workspace_id);
            return;
        }
    });
}

pub(crate) async fn run(svc: &Services, workspace_id: &WorkspaceId) {
    let home_move = match svc.store().get_home_move(workspace_id).await {
        Ok(Some(home_move)) => home_move,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(workspace = %workspace_id, %error, "could not read the home move");
            return;
        }
    };
    if let Err(error) = steps(svc, &home_move).await {
        tracing::warn!(workspace = %workspace_id, %error, "the home move failed; the old home stays");
        // The steps may have recorded the new home's sandbox meanwhile.
        let current = svc
            .store()
            .get_home_move(workspace_id)
            .await
            .ok()
            .flatten()
            .filter(|current| current.target.home_id == home_move.target.home_id)
            .unwrap_or(home_move);
        rollback(svc, &current).await;
    }
}

async fn steps(svc: &Services, home_move: &HomeMove) -> Result<()> {
    let workspace_id = &home_move.workspace_id;
    let target = &home_move.target;
    if !home_move.pushed {
        if home_move.from_state == HomeState::Attached {
            last_backup(svc, home_move).await?;
        }
        if !svc
            .store()
            .mark_home_move_pushed(workspace_id, &target.home_id)
            .await?
        {
            return Ok(());
        }
        if target.cloud_machine_id.is_some() {
            // Clients see the new cloud home starting from now on.
            svc.publish_workspace_home_changed(workspace_id).await;
        }
    }
    let node_id = match (&target.node_id, &target.cloud_machine_id) {
        (Some(node_id), _) => node_id.clone(),
        (None, Some(cloud_machine_id)) => {
            crate::cloud::start_move_home(svc, workspace_id, &target.home_id, cloud_machine_id)
                .await?
        }
        (None, None) => return Err(Error::Internal("the new home is not on a machine".into())),
    };
    let restore = crate::homes::home_restore(svc, workspace_id)
        .await
        .ok()
        .flatten();
    let reply =
        crate::homes::send_open(svc, workspace_id, &target.home_id, &node_id, restore).await?;
    let HomeReply::Open(reply) = reply else {
        return Err(Error::Internal(
            "the machine answered a homeOpen with another reply".into(),
        ));
    };
    if reply.outcome != HomeOpenOutcome::Ready {
        return Err(Error::Internal(format!(
            "the new home did not open: {}",
            reply.error.unwrap_or_default()
        )));
    }
    match crate::homes::switch_home(svc, workspace_id, &target.home_id).await? {
        None => {
            tracing::info!(workspace = %workspace_id, "the home move ended before its switch");
        }
        Some(switch) => {
            tracing::info!(workspace = %workspace_id, home = %target.home_id, "the workspace's home moved");
            crate::cloud::home::changed(svc);
            svc.publish_workspace_home_changed(workspace_id).await;
            if switch.auto_start {
                // The new home finished its setup before the switch.
                let svc = svc.clone();
                let workspace_id = workspace_id.clone();
                intent_core::spawn_daemon(async move {
                    svc.feature_runtime()
                        .start_auto_start(&svc, workspace_id)
                        .await;
                });
            }
            crate::homes::send_workspace_stops(svc, workspace_id).await;
        }
    }
    Ok(())
}

/// Step 1: the old home, attached when the move began, pushes once more.
async fn last_backup(svc: &Services, home_move: &HomeMove) -> Result<()> {
    let workspace_id = &home_move.workspace_id;
    let Some(home) = svc.store().get_workspace_home(workspace_id).await? else {
        return Err(Error::Internal("the workspace has no home".into()));
    };
    if home.home_id != home_move.from_home_id {
        return Err(Error::Internal("the workspace's home changed".into()));
    }
    let old = WorkspaceHome {
        state: HomeState::Attached,
        ..home
    };
    crate::home_calls::push_home(svc, &old, workspace_id).await
}

/// The move failed before its switch: the old home gets its old state back.
async fn rollback(svc: &Services, home_move: &HomeMove) {
    let workspace_id = &home_move.workspace_id;
    match svc
        .store()
        .abort_home_move(
            workspace_id,
            &home_move.target.home_id,
            &intent_core::now_iso(),
        )
        .await
    {
        Ok(true) => {
            // An old cloud home whose sandbox went meanwhile was renewed
            // for a rebuild: a call waiting for it starts it.
            crate::cloud::home::changed(svc);
            svc.publish_workspace_home_changed(workspace_id).await;
            // The target may have opened although its reply was lost: its
            // stop ends its setup and backups and removes the checkout.
            crate::homes::send_workspace_stops(svc, workspace_id).await;
            // An old home whose link went down meanwhile is detached after
            // the link-loss grace, as if it had never moved.
            if let Some(old) = svc
                .store()
                .get_workspace_home(workspace_id)
                .await
                .ok()
                .flatten()
                .and_then(|home| svc.remote_feature()?.running_node(home.node_id.as_deref()?))
            {
                let shared = old.shared_ref();
                if shared.current_link().is_none() {
                    shared
                        .home_link
                        .link_lost(shared.weak(), shared.tuning.link_loss_grace);
                } else if home_move.from_state == HomeState::Detached {
                    // Its machine bound during the move, when a moving home
                    // is not opened: open it now, as that bind would have.
                    if let Err(error) = crate::homes::open_home(svc, workspace_id).await {
                        tracing::info!(workspace = %workspace_id, %error, "the old home did not open again after the rollback");
                    }
                }
            }
            if let Some(node) = home_move
                .target
                .node_id
                .as_deref()
                .and_then(|node_id| svc.remote_feature()?.running_node(node_id))
            {
                if let Ok(holds) =
                    crate::homes::machine_holds_homes(svc.store(), &node.shared_ref().machine_id())
                        .await
                {
                    node.shared_ref().set_holds_homes(holds);
                }
            }
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(workspace = %workspace_id, %error, "could not roll back the home move");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_target_is_an_agent_a_machine_or_a_cloud_entry() {
        let parse = |to: Value| parse_target(&json!({ "to": to }));
        assert_eq!(
            parse(json!({ "agentId": "a-1" })).unwrap(),
            Target::Agent(AgentId::from("a-1"))
        );
        assert_eq!(
            parse(json!({ "kind": "machine", "nodeId": "n-1" })).unwrap(),
            Target::Machine("n-1".into())
        );
        assert_eq!(
            parse(json!({ "kind": "cloud", "cloudMachineId": "e2b" })).unwrap(),
            Target::Cloud("e2b".into())
        );
        for bad in [
            json!({}),
            json!({ "kind": "machine" }),
            json!({ "kind": "boat", "nodeId": "n-1" }),
            json!({ "agentId": "" }),
        ] {
            assert!(matches!(parse(bad), Err(Error::InvalidParams(_))));
        }
        assert!(matches!(
            parse_target(&json!({})),
            Err(Error::InvalidParams(_))
        ));
    }
}
