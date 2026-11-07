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
mod lifecycle;
mod methods;
mod start;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use intent_core::remote_launch::RemotePlacementView;
use intent_core::{AgentId, Caller, Error, Result};
use intent_services::feature_api::Services;
use tokio::sync::watch;

pub(crate) use lifecycle::{spawn_supervisor, CloudGate};
pub(crate) use methods::call;
pub(crate) use start::{admit, begin_start, delete_machine, wait_linked};

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
    /// Paused machines: dialed only on a demand, which resumes them.
    parked: Mutex<HashSet<String>>,
    /// Machines whose sandbox E2B removed, until their removal ends.
    lost: Mutex<HashSet<String>>,
    /// Machines being deleted by the supervisor.
    killing: Mutex<HashSet<String>>,
    /// Per-machine pause/resume locks.
    transitions: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// What the supervisor remembers about each running sandbox.
    activity: Mutex<HashMap<String, lifecycle::Activity>>,
    supervisor: AtomicBool,
    /// Pauses begun per machine.
    pauses: Mutex<HashMap<String, u64>>,
    /// When the reaper first saw each orphan whose start time it cannot read.
    orphans: Mutex<HashMap<String, tokio::time::Instant>>,
}

impl CloudState {
    pub(crate) fn is_parked(&self, machine_id: &str) -> bool {
        crate::lock(&self.parked).contains(machine_id)
    }

    pub(crate) fn park(&self, machine_id: &str) {
        crate::lock(&self.parked).insert(machine_id.to_owned());
    }

    fn count_pause(&self, machine_id: &str) {
        *crate::lock(&self.pauses)
            .entry(machine_id.to_owned())
            .or_default() += 1;
    }

    fn pauses(&self, machine_id: &str) -> u64 {
        crate::lock(&self.pauses)
            .get(machine_id)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn unpark(&self, machine_id: &str) {
        crate::lock(&self.parked).remove(machine_id);
    }

    fn is_lost(&self, machine_id: &str) -> bool {
        crate::lock(&self.lost).contains(machine_id)
    }

    /// Mark the machine lost; `false` when it already was.
    fn mark_lost(&self, machine_id: &str) -> bool {
        crate::lock(&self.lost).insert(machine_id.to_owned())
    }

    fn is_killing(&self, machine_id: &str) -> bool {
        crate::lock(&self.killing).contains(machine_id)
    }

    fn begin_kill(&self, machine_id: &str) -> bool {
        crate::lock(&self.killing).insert(machine_id.to_owned())
    }

    fn end_kill(&self, machine_id: &str) {
        crate::lock(&self.killing).remove(machine_id);
    }

    fn transition_lock(&self, machine_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        crate::lock(&self.transitions)
            .entry(machine_id.to_owned())
            .or_default()
            .clone()
    }

    /// Wait for the machine's pause/resume lock.
    async fn transition(&self, machine_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        self.transition_lock(machine_id).lock_owned().await
    }

    /// Whether a pause or resume of the machine is under way.
    fn in_transition(&self, machine_id: &str) -> bool {
        self.transition_lock(machine_id).try_lock().is_err()
    }

    /// The machine's activity, first seen now, with `busy` noted.
    fn activity(&self, machine_id: &str, busy: bool) -> lifecycle::Activity {
        let mut activity = crate::lock(&self.activity);
        let entry = activity
            .entry(machine_id.to_owned())
            .or_insert_with(lifecycle::Activity::fresh);
        if busy {
            entry.touch();
        }
        *entry
    }

    /// The machine just ran or resumed: its idle time starts now.
    fn touch(&self, machine_id: &str) {
        crate::lock(&self.activity).insert(machine_id.to_owned(), lifecycle::Activity::fresh());
    }

    fn renewed(&self, machine_id: &str) {
        if let Some(entry) = crate::lock(&self.activity).get_mut(machine_id) {
            entry.renewed();
        }
    }

    fn forget_activity(&self, machine_id: &str) {
        crate::lock(&self.activity).remove(machine_id);
    }

    /// Forget everything about a removed machine.
    pub(crate) fn forget(&self, machine_id: &str) {
        self.unpark(machine_id);
        crate::lock(&self.lost).remove(machine_id);
        crate::lock(&self.transitions).remove(machine_id);
        crate::lock(&self.pauses).remove(machine_id);
        self.forget_activity(machine_id);
    }

    /// When the reaper first saw orphan sandbox `sandbox_id` (now, the
    /// first time).
    fn first_seen_orphan(&self, sandbox_id: &str) -> tokio::time::Instant {
        *crate::lock(&self.orphans)
            .entry(sandbox_id.to_owned())
            .or_insert_with(tokio::time::Instant::now)
    }

    /// Forget the first sight of every orphan E2B no longer lists.
    fn keep_orphans<'a>(&self, listed: impl Iterator<Item = &'a str>) {
        let listed: HashSet<&str> = listed.collect();
        crate::lock(&self.orphans).retain(|id, _| listed.contains(id.as_str()));
    }

    fn forget_orphan(&self, sandbox_id: &str) {
        crate::lock(&self.orphans).remove(sandbox_id);
    }

    fn supervisor_started(&self) -> bool {
        self.supervisor.swap(true, Ordering::SeqCst)
    }

    /// The agents whose sandbox start is in flight.
    fn starting_agents(&self) -> HashSet<String> {
        crate::lock(&self.starts).keys().cloned().collect()
    }

    fn start_of(&self, agent_id: &AgentId) -> Option<watch::Receiver<StartOutcome>> {
        crate::lock(&self.starts).get(agent_id.as_str()).cloned()
    }

    /// Every start in flight.
    fn starts_in_flight(&self) -> Vec<watch::Receiver<StartOutcome>> {
        crate::lock(&self.starts).values().cloned().collect()
    }
}

/// The person behind the current call, for `cloud.startedBy`: a wire
/// caller's principal; for an agent caller, the person behind its chain
/// ([`agent_person`]); else, or when unknown, this backend's primary
/// principal (the owner).
async fn started_by(svc: &Services) -> intent_core::machines::CloudStartedBy {
    let person = match intent_core::current_caller() {
        Some(Caller::Wire { principal_id, .. }) => Some(principal_id),
        Some(Caller::Agent { agent_id }) => agent_person(svc, &agent_id).await,
        _ => None,
    };
    let principal = match person {
        Some(principal_id) => match svc.store().get_principal(&principal_id).await {
            Ok(principal) => Ok(principal),
            Err(_) => svc.store().get_primary_principal().await,
        },
        None => svc.store().get_primary_principal().await,
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

/// The person behind agent `agent_id`: whoever started the sandbox of the
/// closest agent of its chain (itself first, then its parents) that runs
/// on a cloud machine, else the person recorded as its creator. `None`
/// when neither is known (an agent created before the record existed).
async fn agent_person(svc: &Services, agent_id: &AgentId) -> Option<intent_core::PrincipalId> {
    let store = svc.store();
    let mut creator = None;
    let mut current = Some(agent_id.clone());
    for _ in 0..64 {
        let Some(id) = current else { break };
        let Ok(session) = store.get_agent_session_summary(&id).await else {
            break;
        };
        let metadata = session.metadata.as_ref();
        if let Some(view) = RemotePlacementView::from_metadata(metadata)
            .filter(|view| view.is_cloud() && !view.is_unlinked())
        {
            let starter = store
                .get_exposed_machine(&view.node_id)
                .await
                .ok()
                .flatten()
                .and_then(|row| row.cloud)
                .map(|cloud| cloud.started_by.principal_id)
                .filter(|id| !id.is_empty());
            if let Some(starter) = starter {
                return Some(intent_core::PrincipalId::from_string(starter));
            }
        }
        if creator.is_none() {
            creator = metadata
                .and_then(|m| m.get(intent_store::CREATED_BY_PRINCIPAL_METADATA_KEY))
                .and_then(serde_json::Value::as_str)
                .map(intent_core::PrincipalId::from_string);
        }
        current = session.parent_agent_id.clone();
    }
    creator
}

/// A client for the saved E2B key, if one is saved.
pub(crate) async fn saved_client(svc: &Services) -> Option<intent_nodes::e2b::E2bClient> {
    config::saved_client(svc).await.ok()
}

/// The remote feature, or the error of a daemon without it.
fn feature(svc: &Services) -> Result<&crate::RemoteFeature> {
    use crate::RemoteServicesExt as _;
    svc.remote_feature()
        .ok_or_else(|| Error::Unsupported("this daemon cannot start cloud machines".into()))
}
