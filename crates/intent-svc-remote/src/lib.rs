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
//! The feature sits above `intent-services`; the composition root registers
//! it with [`register`] and the daemon starts the connector at boot with
//! [`RemoteServicesExt::start_remote_node`]. No configuration file means no
//! connector, and no daemon behavior changes.

use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};

use intent_core::Result;
use intent_services::feature_api::Services;

mod agents;
mod config;
mod connector;
mod ingest;
mod lease;
mod runtime;

pub use config::{HeadNodeConfig, CONFIG_FILE};
pub use connector::{NodeStatus, RemoteNode};
pub use runtime::RemoteAgentRuntime;

#[cfg(test)]
mod tests;

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

/// Register the remote feature on `services`. Nothing dials a node until
/// [`RemoteServicesExt::start_remote_node`] runs.
#[must_use]
pub fn register(services: Services) -> Services {
    services.with_feature(Arc::new(RemoteFeature::default()))
}

/// Remote entry points the daemon calls on [`Services`].
pub trait RemoteServicesExt {
    /// Start the connector to the node configured in `data_dir`'s
    /// [`CONFIG_FILE`]. Returns `false`, starting nothing, when the file
    /// does not exist or a connector already runs.
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
            let Some(feature) = self.feature::<RemoteFeature>() else {
                return Err(intent_core::Error::Internal(
                    "the remote feature is not registered".into(),
                ));
            };
            let Some(config) = HeadNodeConfig::read_if_present(&path)? else {
                return Ok(false);
            };
            if feature.node().is_some() {
                return Ok(false);
            }
            let node = RemoteNode::start(self.store().clone(), config).await?;
            let mut slot = lock(&feature.node);
            if slot.is_some() {
                node.shutdown();
                return Ok(false);
            }
            *slot = Some(node);
            Ok(true)
        }
    }

    fn remote_node(&self) -> Option<RemoteNode> {
        self.feature::<RemoteFeature>()?.node()
    }
}
