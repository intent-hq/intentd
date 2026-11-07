//! E2B cloud machines on the backend (monorepo
//! `docs/protocol/methods/nodes.md`, "Cloud machines (E2B)"): the provider
//! settings behind `cloudMachines.configure` / `list` / `remove`, admission
//! of `placement.target: "e2b"`, and the start of a new sandbox for a
//! top-level agent.
//!
//! A start creates the sandbox from the intent-node template of this
//! backend's exact version, starts `intentd serve` with its tunnel in it,
//! mints its executor credential over E2B's process API (the sandbox's local
//! socket is the owner seat), then adds a hidden machine row through the
//! same probe as `remoteMachines.add` and launches the agent on it like any
//! remote agent. Sub-agents share their parent's sandbox.
//!
//! GUARDRAIL: the E2B key, the sandbox's envd token and the executor token
//! never reach a log, an error, an event, a result or `Debug` output. The
//! key stays in the secret store and the client; the mint output is parsed
//! and dropped.

mod bootstrap;
mod config;
mod methods;
mod start;

use std::collections::HashMap;
use std::sync::Mutex;

use intent_core::{AgentId, Caller, Error, Result};
use intent_services::feature_api::Services;
use tokio::sync::watch;

pub(crate) use methods::call;
pub(crate) use start::{admit, begin_start, wait_linked};

/// A finished start that failed: the error's message and data (with its
/// `code`), so every waiter can rebuild the same error.
#[derive(Debug, Clone)]
pub(crate) struct StartFailure {
    message: String,
    data: serde_json::Value,
}

impl StartFailure {
    fn of(error: &Error) -> Self {
        match error {
            Error::RemoteMachine(failure) => Self {
                message: failure.message.clone(),
                data: failure.data.clone(),
            },
            other => Self {
                message: other.to_string(),
                data: serde_json::json!({ "code": "remote-launch-failed" }),
            },
        }
    }

    fn error(&self) -> Error {
        Error::remote_machine(true, self.message.clone(), self.data.clone())
    }
}

type StartOutcome = Option<std::result::Result<(), StartFailure>>;

/// The cloud machines' share of the remote feature's state.
#[derive(Default)]
pub(crate) struct CloudState {
    /// Sandbox starts in flight, by agent id.
    starts: Mutex<HashMap<String, watch::Receiver<StartOutcome>>>,
    /// `configure` and `remove` take turns.
    settings: tokio::sync::Mutex<()>,
}

impl CloudState {
    fn start_of(&self, agent_id: &AgentId) -> Option<watch::Receiver<StartOutcome>> {
        crate::lock(&self.starts).get(agent_id.as_str()).cloned()
    }
}

/// The principal behind the current call, for `cloud.startedBy`: a wire
/// caller's principal, else this backend's primary principal (the owner).
async fn started_by(svc: &Services) -> intent_core::machines::CloudStartedBy {
    let principal = match intent_core::current_caller() {
        Some(Caller::Wire { principal_id, .. }) => svc.store().get_principal(&principal_id).await,
        _ => svc.store().get_primary_principal().await,
    };
    match principal {
        Ok(principal) => intent_core::machines::CloudStartedBy {
            principal_id: principal.id.to_string(),
            display_name: principal.display_name.or(principal.login),
        },
        Err(_) => intent_core::machines::CloudStartedBy {
            principal_id: String::new(),
            display_name: None,
        },
    }
}

/// The remote feature, or the error of a daemon without it.
fn feature(svc: &Services) -> Result<&crate::RemoteFeature> {
    use crate::RemoteServicesExt as _;
    svc.remote_feature()
        .ok_or_else(|| Error::Unsupported("this daemon cannot start cloud machines".into()))
}
