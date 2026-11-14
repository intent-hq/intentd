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

use crate::connector::RemoteNode;
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

/// The home's machine, connected and speaking nodeProtocol 19.
async fn home_node(svc: &Services, home: &WorkspaceHome, method: &str) -> Result<RemoteNode> {
    let node_id = home
        .node_id
        .as_deref()
        .ok_or_else(|| call_unavailable(home, method, "it is not on a machine"))?;
    let feature = svc
        .remote_feature()
        .ok_or_else(|| call_unavailable(home, method, "no remote machine feature"))?;
    let node = feature
        .node(svc, node_id)
        .await
        .map_err(|error| call_unavailable(home, method, &error.to_string()))?;
    node.connect()
        .await
        .map_err(|error| call_unavailable(home, method, &error.to_string()))?;
    if node.shared_ref().protocol() < HOMES_PROTOCOL {
        return Err(call_unavailable(
            home,
            method,
            "its machine needs an intentd update for workspace homes",
        ));
    }
    Ok(node)
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
    let node = home_node(svc, home, method).await?;
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
pub(crate) async fn push_home(
    svc: &Services,
    home: &WorkspaceHome,
    workspace_id: &WorkspaceId,
) -> Result<()> {
    let node = home_node(svc, home, "homePush").await?;
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
