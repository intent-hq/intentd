//! `agent.retrySetup` on the backend (nodes.md "Retrying a remote agent's
//! setup"): the remote agent's failed setup script runs again in its kept
//! checkout on its machine (`setupRetry`, nodeProtocol 21). The services
//! layer has checked the agent's own state; this resolves the script again
//! and asks the machine.

use intent_core::machines::MachineUpdateRequired;
use intent_core::node_link::{setup_retry_codes, SetupRetryOutcome, SETUP_RETRY_PROTOCOL};
use intent_core::remote_launch::{
    setup_retry, RemotePlacementView, RemoteSetupState, RemoteSetupStatus,
};
use intent_core::{AgentId, Error, Result};
use intent_services::feature_api::Services;
use serde_json::json;

use crate::RemoteServicesExt as _;

fn refusal(code: &str, message: impl Into<String>) -> Error {
    Error::remote_machine(false, message, json!({ "code": code }))
}

fn not_running() -> Error {
    refusal(
        setup_retry::AGENT_NOT_RUNNING,
        "The agent is not running on its machine; send it a message, then retry setup once that turn ends.",
    )
}

fn unreachable(name: &str, detail: &str) -> Error {
    Error::remote_machine(
        true,
        format!(
            "{name} did not answer the setup retry ({detail}); try again when it is reachable."
        ),
        json!({ "code": crate::connect::MACHINE_UNREACHABLE }),
    )
}

/// The machine's `setupRetryReply.error` as the method's refusal.
fn machine_refusal(error: &str) -> Error {
    let (code, detail) = error.split_once(": ").unwrap_or((error, error));
    match code {
        setup_retry_codes::NOT_RUNNING => refusal(
            setup_retry::AGENT_NOT_RUNNING,
            format!("The agent is not running on its machine ({detail}); send it a message, then retry setup once that turn ends."),
        ),
        setup_retry_codes::SHARED => refusal(
            setup_retry::SHARED_CHECKOUT,
            format!("Retry setup on the agent that owns the checkout ({detail})."),
        ),
        _ => refusal(
            setup_retry::NOT_FAILED,
            format!("Setup has not failed in this agent's checkout ({detail})."),
        ),
    }
}

/// Ask `agent_id`'s machine to run its setup again; the run's status once
/// it has started.
pub(crate) async fn retry(
    svc: &Services,
    agent_id: &AgentId,
    view: &RemotePlacementView,
) -> Result<RemoteSetupStatus> {
    let record = svc
        .store()
        .get_remote_agent(agent_id)
        .await?
        .ok_or_else(not_running)?;
    let feature = svc.remote_feature().ok_or_else(not_running)?;
    let node = feature.node(svc, &record.node_id).await?;
    let status = node.status();
    if status.lease_id != record.lease_id {
        return Err(not_running());
    }
    if node.current_link().is_none() {
        node.connect().await?;
    }
    if node.protocol() < SETUP_RETRY_PROTOCOL {
        let status = node.status();
        return Err(Error::MachineUpdateRequired(Box::new(
            MachineUpdateRequired::setup_retry(
                status.node_name,
                status
                    .intentd_version
                    .unwrap_or_else(|| "an older version".into()),
            ),
        )));
    }
    let setup = svc
        .remote_setup_command(&record.workspace_id, view)
        .await?
        .ok_or_else(|| {
            refusal(
                setup_retry::NO_SCRIPT,
                "The workspace has no setup script now, so there is nothing to run.",
            )
        })?;
    let reply = node
        .setup_retry(agent_id, setup, setup_retry::REPLY_WAIT)
        .await
        .map_err(|error| unreachable(&status.node_name, &error.to_string()))?;
    match reply.outcome {
        SetupRetryOutcome::Started => Ok(RemoteSetupStatus {
            state: RemoteSetupState::Running,
            started_at: reply.started_at,
            ..RemoteSetupStatus::pending()
        }),
        SetupRetryOutcome::Refused => Err(machine_refusal(
            reply
                .error
                .as_deref()
                .unwrap_or(setup_retry_codes::NOT_FAILED),
        )),
    }
}
