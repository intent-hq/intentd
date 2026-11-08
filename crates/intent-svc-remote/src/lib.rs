//! Remote agents for intentd: the backend side of daemon-to-daemon remote
//! agents.
//!
//! The owner exposes a machine to this backend (`remoteMachines.add`, after
//! the machine minted an executor credential for it). The backend keeps the
//! machine's routes and certificate pin in its store and the credential's
//! token in its secret store, and connects to the machine's executor link
//! only when it is needed: to place or drive an agent there, or to refresh
//! its capabilities. A link with nothing to do is closed after an idle
//! timeout; a link lost while agents are live there is redialed, and the
//! hello's `ackedSeq` makes the machine replay what this backend has not
//! ingested.
//!
//! Each machine's connector ingests its journal in sequence order, hands
//! each record to the [`RemoteAgentRuntime`] of its agent, persists the
//! highest contiguous sequence and only then acks it, so a reconnect (or a
//! backend restart) replays exactly the records not yet ingested.
//!
//! [`RemoteAgentRuntime`] is the agent manager's `AgentRuntime` for an agent
//! running on a machine: prompts, cancels and stops go out as link frames,
//! and the machine's journal comes back as the runtime's notifications and
//! turn outcomes.
//!
//! [`RemoteLaunch`] is the feature's remote-launch port: admission against
//! the exposed machines, placing a created agent on one, the runtime that
//! starts it there on its first turn, and the `remoteMachines.*` methods.
//!
//! The feature sits above `intent-services`; the composition root registers
//! it with [`register`] and hands it the tailcat binary with
//! [`RemoteServicesExt::configure_remote_machines`]. No daemon setting
//! gates it: the frontend's Labs flag decides whether a user sees it, and
//! the model is told about remote placement only while a machine is
//! exposed to this backend. In node mode no connector ever starts.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_core::{Error, Result};
use intent_services::feature_api::Services;

mod agents;
mod cloud;
mod connect;
mod connector;
mod final_stop;
mod forge_token;
mod git;
mod ingest;
mod launch;
mod lease;
mod machine;
mod methods;
mod rpc;
mod runtime;
mod view;

pub use connector::{NodeStatus, RemoteNode};
pub use launch::RemoteLaunch;
pub use runtime::RemoteAgentRuntime;

#[cfg(test)]
mod tests;

/// The remote feature's state: one connector per exposed machine, started
/// when the machine is first needed.
#[derive(Default)]
pub struct RemoteFeature {
    nodes: Mutex<HashMap<String, RemoteNode>>,
    /// Per-machine lifecycle locks: adding, replacing or removing a machine
    /// and starting an agent on it take turns, so no agent starts under a
    /// credential that is being replaced or removed.
    lifecycles: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    tailcat_bin: Mutex<Option<PathBuf>>,
    /// Connector timing; the defaults unless a test shortens them.
    tuning: Mutex<Option<connector::Tuning>>,
    /// E2B cloud machines: starts in flight and the settings lock.
    pub(crate) cloud: cloud::CloudState,
    /// The GitHub token every linked machine holds (nodeProtocol 15).
    forge: std::sync::OnceLock<Arc<forge_token::ForgeTokens>>,
}

impl RemoteFeature {
    /// The running connector to machine `machine_id`, if one was started.
    #[must_use]
    pub fn running_node(&self, machine_id: &str) -> Option<RemoteNode> {
        lock(&self.nodes).get(machine_id).cloned()
    }

    /// Every running connector.
    #[must_use]
    pub fn running_nodes(&self) -> Vec<RemoteNode> {
        lock(&self.nodes).values().cloned().collect()
    }

    /// How many exposed machines this backend holds a live link to now.
    #[must_use]
    pub fn connected_machines(&self) -> usize {
        lock(&self.nodes)
            .values()
            .filter(|node| node.status().connected)
            .count()
    }

    pub(crate) fn take_node(&self, machine_id: &str) -> Option<RemoteNode> {
        lock(&self.nodes).remove(machine_id)
    }

    /// Wait for machine `machine_id`'s lifecycle lock.
    pub(crate) async fn lifecycle(&self, machine_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let machine = lock(&self.lifecycles)
            .entry(machine_id.to_owned())
            .or_default()
            .clone();
        machine.lock_owned().await
    }

    pub(crate) fn tailcat_bin(&self) -> Option<PathBuf> {
        lock(&self.tailcat_bin).clone()
    }

    pub(crate) fn tuning(&self) -> connector::Tuning {
        lock(&self.tuning).clone().unwrap_or_default()
    }

    /// The GitHub token the machines hold, its watcher started.
    fn forge(&self, svc: &Services) -> Arc<forge_token::ForgeTokens> {
        let forge = self
            .forge
            .get_or_init(|| Arc::new(forge_token::ForgeTokens::for_services(svc)))
            .clone();
        let tuning = self.tuning();
        forge.watch(svc, tuning.forge_refresh, tuning.forge_check);
        forge
    }

    /// The connector to exposed machine `machine_id`, started if needed
    /// under the machine's lifecycle lock, so a credential replacement or
    /// removal in progress finishes first and the connector is built from
    /// its outcome, not from the lease and token it replaces. Starting
    /// dials nothing.
    ///
    /// # Errors
    ///
    /// `not-found` for an unknown machine; when its token is missing from
    /// the secret store or its lease has ended.
    pub(crate) async fn node(&self, svc: &Services, machine_id: &str) -> Result<RemoteNode> {
        if !svc.node_mode() {
            if let Some(node) = self.running_node(machine_id) {
                return Ok(node);
            }
        }
        let _lifecycle = self.lifecycle(machine_id).await;
        self.node_locked(svc, machine_id).await
    }

    /// [`Self::node`] for a caller already holding `machine_id`'s lifecycle
    /// lock.
    ///
    /// # Errors
    ///
    /// As [`Self::node`].
    pub(crate) async fn node_locked(&self, svc: &Services, machine_id: &str) -> Result<RemoteNode> {
        // Node mode turns this daemon's orchestration off, including acting
        // as a backend: no outbound connector, so no remote agent can reach
        // its workspaces through forwarded calls. The inbound executor link
        // is separate and stays available.
        if svc.node_mode() {
            return Err(Error::NodeMode(
                "exposed machines are off: this daemon runs in node mode".into(),
            ));
        }
        if let Some(node) = self.running_node(machine_id) {
            return Ok(node);
        }
        let row = svc
            .store()
            .get_exposed_machine(machine_id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("exposed machine {machine_id}")))?;
        let token = svc
            .load_remote_machine_token(machine_id)
            .await?
            .ok_or_else(|| {
                Error::Internal(format!(
                    "the executor token of machine {machine_id} is missing; make the machine available again"
                ))
            })?;
        // A cloud machine dials through its sandbox's gate; a paused one
        // stays parked until something asks for its link.
        let gate = row.cloud.as_ref().map(|cloud| {
            if matches!(
                cloud.state,
                intent_core::machines::CloudSandboxState::Paused
                    | intent_core::machines::CloudSandboxState::Pausing
                    | intent_core::machines::CloudSandboxState::Resuming
            ) {
                self.cloud.park(machine_id);
            }
            cloud::CloudGate::new(svc.clone(), machine_id)
        });
        let target = machine::MachineTarget::new(&row, &token, self.tailcat_bin())?
            .tapped(svc.link_traffic().clone())
            .gated(gate);
        let node = RemoteNode::start(
            svc.store().clone(),
            target,
            self.tuning(),
            Some(svc.clone()),
            Some(self.forge(svc)),
        )
        .await?;
        let mut nodes = lock(&self.nodes);
        if let Some(existing) = nodes.get(machine_id) {
            node.shutdown();
            return Ok(existing.clone());
        }
        nodes.insert(machine_id.to_owned(), node.clone());
        Ok(node)
    }
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Register the remote feature and its remote-launch port on `services`.
/// Nothing dials a machine until one is needed.
#[must_use]
pub fn register(services: Services) -> Services {
    services
        .with_feature(Arc::new(RemoteFeature::default()))
        .with_remote_launch_port(Arc::new(RemoteLaunch))
}

/// Remote entry points the daemon calls on [`Services`].
/// See [`RemoteServicesExt::resume_exposed_machines`]. Starting a connector
/// dials nothing inline: each one connects in the background, with backoff
/// while it is unreachable, so a machine that is down never slows the start.
async fn resume_exposed_machines(svc: &Services) -> Vec<String> {
    let Some(feature) = svc.remote_feature() else {
        return Vec::new();
    };
    if svc.node_mode() {
        return Vec::new();
    }
    cloud::spawn_supervisor(svc);
    let candidates = match machines_with_live_records(svc).await {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::warn!(%error, "remote machines: could not read the recorded agents at start");
            return Vec::new();
        }
    };
    let mut started = Vec::new();
    for machine_id in candidates {
        match feature.node(svc, &machine_id).await {
            Ok(_) => started.push(machine_id),
            Err(error) => {
                tracing::warn!(machine = %machine_id, %error, "remote machine: could not start its connector at start");
            }
        }
    }
    started
}

/// The exposed machines with a live lease that records agents that may
/// still run.
async fn machines_with_live_records(svc: &Services) -> Result<Vec<String>> {
    let store = svc.store();
    let head_id = store.daemon_id().await?;
    let mut machines = Vec::new();
    for lease in store.list_owned_node_leases(&head_id).await? {
        let machine_id = &lease.owner.node_id;
        if lease.state.is_terminal() || machines.contains(machine_id) {
            continue;
        }
        // A paused cloud machine stays paused: its connector starts (and
        // resumes it) when something asks for it.
        match store.get_exposed_machine(machine_id).await? {
            None => continue,
            Some(row)
                if row.cloud.as_ref().is_some_and(|cloud| {
                    cloud.state != intent_core::machines::CloudSandboxState::Running
                }) =>
            {
                continue
            }
            Some(_) => {}
        }
        if connector::has_live_records(store, &lease.owner.lease_id).await? {
            machines.push(machine_id.clone());
        }
    }
    Ok(machines)
}

pub trait RemoteServicesExt {
    /// Give the feature the sitter-shipped tailcat binary, so machines can
    /// also be reached over their tailcat address. Without it only direct
    /// routes are tried.
    fn configure_remote_machines(&self, tailcat_bin: Option<PathBuf>);

    /// At daemon start: start the connector to every exposed machine that
    /// may still run this backend's agents, so it reconnects in the
    /// background and claims them. Returns the machines started.
    fn resume_exposed_machines(&self) -> impl std::future::Future<Output = Vec<String>> + Send;

    /// The remote feature, when registered.
    fn remote_feature(&self) -> Option<&RemoteFeature>;
}

impl RemoteServicesExt for Services {
    fn configure_remote_machines(&self, tailcat_bin: Option<PathBuf>) {
        if let Some(feature) = self.remote_feature() {
            *lock(&feature.tailcat_bin) = tailcat_bin;
        }
    }

    fn resume_exposed_machines(&self) -> impl std::future::Future<Output = Vec<String>> + Send {
        let svc = self.clone();
        async move { resume_exposed_machines(&svc).await }
    }

    fn remote_feature(&self) -> Option<&RemoteFeature> {
        self.feature::<RemoteFeature>()
    }
}
