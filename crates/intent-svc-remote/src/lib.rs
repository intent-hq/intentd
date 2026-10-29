//! Remote agents for intentd: the head side of simplified remote launch.
//!
//! Head dials the one execution node configured in a private file
//! ([`CONFIG_FILE`] in the data directory), binds it with a durably advanced
//! link generation, and keeps the link up with backoff. The node streams
//! journal records over the link; head ingests them in sequence order, hands
//! each to the [`RemoteAgentRuntime`] of its agent, persists the highest
//! contiguous sequence and only then acks it, so a reconnect (or a head
//! restart) replays exactly the records head has not ingested.
//!
//! [`RemoteAgentRuntime`] is the agent manager's `AgentRuntime` for an agent
//! running on the node: prompts, cancels and stops go out as link frames, and
//! the node's journal comes back as the runtime's notifications and turn
//! outcomes, so turn orchestration, queueing and transcripts stay those of a
//! local agent.
//!
//! [`RemoteLaunch`] is the feature's remote-launch port: admission against
//! the node, placing a created agent on it, and the runtime that starts the
//! agent there on its first turn.
//!
//! The feature sits above `intent-services`; the composition root registers
//! it with [`register`] and the daemon starts the connector at boot with
//! [`RemoteServicesExt::start_remote_node`]. It is experimental: the
//! connector starts only when [`EXPERIMENTAL_ENV`] is `1` and the
//! configuration file exists. Otherwise no node is dialed, every remote
//! placement fails as `placement-unavailable`, and no daemon behavior
//! changes. No client capability is advertised either way.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use intent_core::Result;
use intent_services::feature_api::Services;

mod agents;
mod config;
mod connector;
mod git;
mod ingest;
mod launch;
mod lease;
mod runtime;
mod view;

pub use config::{HeadNodeConfig, CONFIG_FILE};
pub use connector::{NodeStatus, RemoteNode};
pub use launch::RemoteLaunch;
pub use runtime::RemoteAgentRuntime;

/// The experimental switch: remote agents run only when this environment
/// variable is `1` when the daemon starts (and a node is configured).
pub const EXPERIMENTAL_ENV: &str = "INTENTD_EXPERIMENTAL_REMOTE_LAUNCH";

/// Whether the experimental switch is on in this process.
#[must_use]
pub fn experimental_enabled() -> bool {
    std::env::var(EXPERIMENTAL_ENV).is_ok_and(|value| value.trim() == "1")
}

#[cfg(test)]
mod tests;

/// Test seam: make `node` the running connector.
#[cfg(test)]
pub(crate) fn install_node(services: &Services, node: RemoteNode) {
    let feature = services
        .feature::<RemoteFeature>()
        .expect("the remote feature is registered");
    *lock(&feature.node) = Some(node);
}

/// The remote feature's state: the running connector, once started.
#[derive(Default)]
pub struct RemoteFeature {
    node: Mutex<Option<RemoteNode>>,
}

impl RemoteFeature {
    /// The connector to the configured node, if one was started.
    #[must_use]
    pub fn node(&self) -> Option<RemoteNode> {
        lock(&self.node).clone()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Register the remote feature and its remote-launch port on `services`.
/// Nothing dials a node until [`RemoteServicesExt::start_remote_node`]
/// runs.
#[must_use]
pub fn register(services: Services) -> Services {
    services
        .with_feature(Arc::new(RemoteFeature::default()))
        .with_remote_launch_port(Arc::new(RemoteLaunch))
}

/// Remote entry points the daemon calls on [`Services`].
pub trait RemoteServicesExt {
    /// Start the connector to the node configured in `data_dir`'s
    /// [`CONFIG_FILE`]. Returns `false`, starting nothing, when the
    /// experimental switch ([`EXPERIMENTAL_ENV`]) is off, the file does not
    /// exist, or a connector already runs.
    ///
    /// # Errors
    ///
    /// An unreadable or invalid configuration (the message never quotes the
    /// lease token), or failing to record the node and its lease.
    fn start_remote_node(&self, data_dir: &Path) -> impl Future<Output = Result<bool>> + Send;

    /// The connector to the configured node, if one runs.
    fn remote_node(&self) -> Option<RemoteNode>;
}

impl RemoteServicesExt for Services {
    fn start_remote_node(&self, data_dir: &Path) -> impl Future<Output = Result<bool>> + Send {
        let path = data_dir.join(CONFIG_FILE);
        async move {
            if !experimental_enabled() {
                if path.exists() {
                    tracing::info!(
                        "remote node: {CONFIG_FILE} is present but remote agents are experimental; set {EXPERIMENTAL_ENV}=1 to enable them"
                    );
                }
                return Ok(false);
            }
            start_configured(self, data_dir).await
        }
    }

    fn remote_node(&self) -> Option<RemoteNode> {
        self.feature::<RemoteFeature>()?.node()
    }
}

/// [`RemoteServicesExt::start_remote_node`] past the experimental switch.
pub(crate) async fn start_configured(services: &Services, data_dir: &Path) -> Result<bool> {
    let Some(feature) = services.feature::<RemoteFeature>() else {
        return Err(intent_core::Error::Internal(
            "the remote feature is not registered".into(),
        ));
    };
    let Some(config) = HeadNodeConfig::read_if_present(&data_dir.join(CONFIG_FILE))? else {
        return Ok(false);
    };
    if feature.node().is_some() {
        return Ok(false);
    }
    let node = RemoteNode::start(services.store().clone(), config).await?;
    let mut slot = lock(&feature.node);
    if slot.is_some() {
        node.shutdown();
        return Ok(false);
    }
    *slot = Some(node);
    Ok(true)
}
