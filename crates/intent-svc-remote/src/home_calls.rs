//! Files and git on the home, orchestrator side (monorepo
//! `docs/protocol/node-link.md`, "Files and git on the home",
//! nodeProtocol 19): send one routed wire method to the workspace's home
//! machine as a `homeCall` and hand back its reply, and push the home to
//! the hub before a hub operation needs its commits.

use std::time::Duration;

use intent_core::node_link::{
    HomeCall, HomeCallError, HomeCallOutcome, HomePush, HomePushOutcome, LinkFrame, LinkId,
    HOMES_PROTOCOL,
};
use intent_core::{Error, Result, WorkspaceHome, WorkspaceId};
use intent_services::feature_api::Services;
use serde_json::{Map, Value};

use crate::terminals::home_unavailable;
use crate::RemoteServicesExt as _;

/// How long a `homePush` may take before a hub operation gives up.
const PUSH_WAIT: Duration = Duration::from_secs(120);

/// The extra time a call's reply may take past its `timeoutMs` to arrive.
const REPLY_GRACE: Duration = Duration::from_secs(5);

/// `home-unavailable` for a call: `git.agentCommit`'s is not retryable,
/// as its commit may have landed.
fn call_unavailable(home: &WorkspaceHome, method: &str, detail: &str) -> Error {
    let error = home_unavailable(home, detail);
    if method != "git.agentCommit" {
        return error;
    }
    match error {
        Error::RemoteMachine(mut failure) => {
            if let Some(data) = failure.data.as_object_mut() {
                data.insert("retryable".into(), Value::Bool(false));
            }
            Error::RemoteMachine(failure)
        }
        other => other,
    }
}

/// The error of a call that could not reach `home`'s machine: `starting`
/// when its cloud sandbox is gone and the home is rebuilt
/// ([`crate::cloud::home::sandbox_gone`]).
async fn unreached(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
    method: &str,
    error: &Error,
) -> Error {
    if crate::cloud::home::sandbox_gone(svc, workspace_id, home).await {
        return crate::terminals::home_starting(home);
    }
    call_unavailable(home, method, &error.to_string())
}

/// A use of the home's machine, held while a call runs ([`MachineLink::begin_use`]).
type Use = intent_node_connections::UseGuard;

/// The home's machine, connected and speaking nodeProtocol 19. With
/// `gated`, the call is a use of the machine: it waits out an idle pause
/// before it connects (a pause's own pushes are not gated).
///
/// [`MachineLink::begin_use`]: intent_node_connections::MachineLink::begin_use
async fn home_node(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
    method: &str,
    gated: bool,
) -> Result<(crate::Node, Option<Use>)> {
    // Detached: the call never reached the home, so it may be retried.
    crate::terminals::ensure_attached(svc, home)?;
    let node_id = home
        .node_id
        .as_deref()
        .ok_or_else(|| call_unavailable(home, method, "it is not on a machine"))?;
    let feature = svc
        .remote_feature()
        .ok_or_else(|| call_unavailable(home, method, "no remote machine feature"))?;
    let node = match feature.node(svc, node_id).await {
        Ok(node) => node,
        Err(error) => return Err(unreached(svc, workspace_id, home, method, &error).await),
    };
    let gate = if gated {
        Some(node.begin_use().await)
    } else {
        None
    };
    if let Err(error) = node.connect().await {
        return Err(unreached(svc, workspace_id, home, method, &error).await);
    }
    if node.protocol() < HOMES_PROTOCOL {
        return Err(call_unavailable(
            home,
            method,
            "its machine needs an intentd update for workspace homes",
        ));
    }
    Ok((node, gate))
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::home_call`].
pub(crate) async fn home_call(
    svc: &Services,
    home: &WorkspaceHome,
    workspace_id: &WorkspaceId,
    method: &str,
    params: Map<String, Value>,
    timeout: Duration,
) -> Result<std::result::Result<Value, HomeCallError>> {
    let home = &crate::cloud::home::ready(svc, workspace_id, home.clone()).await?;
    let (node, _use) = home_node(svc, workspace_id, home, method, true).await?;
    let frame = HomeCall {
        request_id: LinkId::new_v4(),
        workspace_id: workspace_id.clone(),
        home_id: home.home_id.clone(),
        method: method.to_owned(),
        params,
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    };
    let reply = node
        .home_call(frame, timeout + REPLY_GRACE)
        .await
        .map_err(|error| match error {
            // A request that cannot fit one frame never will: not a
            // reachability problem, and not worth a retry.
            Error::InvalidParams(message) if message.contains("node-link-frame-too-large") => {
                Error::InvalidParams(format!(
                    "home-request-too-large: the request does not fit one frame to the home ({message})"
                ))
            }
            error => call_unavailable(home, method, &error.to_string()),
        })?;
    Ok(match reply.outcome {
        HomeCallOutcome::Ok => Ok(reply.result.unwrap_or(Value::Null)),
        HomeCallOutcome::Err => Err(reply.error.unwrap_or_else(|| HomeCallError {
            code: -32603,
            message: "the home answered an error without detail".to_owned(),
            data: None,
        })),
    })
}

/// See [`intent_services::feature_api::feature_ports::RemoteLaunchPort::push_home`].
/// A failure counts toward the home's `backupWarning`.
pub(crate) async fn push_home(
    svc: &Services,
    home: &WorkspaceHome,
    workspace_id: &WorkspaceId,
) -> Result<()> {
    // A cloud home no sandbox holds has no work beyond the hub's refs:
    // nothing to push, and no reason to start one.
    if home.cloud_machine_id.is_some() && home.node_id.is_none() {
        return Ok(());
    }
    let pushed = push_once(svc, home, workspace_id).await;
    match &pushed {
        Ok(()) => crate::home_state::backup_succeeded(svc, workspace_id, home).await,
        Err(_) => crate::home_state::backup_failed(svc, workspace_id, home).await,
    }
    pushed
}

/// Ask the home for one synchronous push.
async fn push_once(svc: &Services, home: &WorkspaceHome, workspace_id: &WorkspaceId) -> Result<()> {
    // A moving home still pushes from where it is, without waiting for
    // the move's new home.
    let home = &if home.state == intent_core::HomeState::Moving {
        home.clone()
    } else {
        crate::cloud::home::ready(svc, workspace_id, home.clone()).await?
    };
    let (node, _) = home_node(svc, workspace_id, home, "homePush", false).await?;
    let request_id = LinkId::new_v4();
    let frame = LinkFrame::HomePush(HomePush {
        request_id,
        workspace_id: workspace_id.clone(),
        home_id: home.home_id.clone(),
    });
    let reply = node
        .home_request(request_id, frame, PUSH_WAIT)
        .await
        .map_err(|error| home_unavailable(home, &error.to_string()))?;
    let crate::homes::HomeReply::Push(reply) = reply else {
        return Err(Error::Internal(
            "the machine answered a homePush with another reply".into(),
        ));
    };
    match reply.outcome {
        HomePushOutcome::Pushed => Ok(()),
        HomePushOutcome::Failed => Err(home_unavailable(
            home,
            &format!(
                "the home could not push its commits: {}",
                reply.error.unwrap_or_default()
            ),
        )),
    }
}
