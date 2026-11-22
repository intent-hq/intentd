//! The head's outbound side: one connector per machine it links to.
//!
//! The domain holds an `Arc<dyn NodeConnections>` and never the connector
//! map itself, so a later transport can own the connectors in another
//! process. [`InProcessNodeConnections`] keeps them in this one.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use intent_core::{BoxFuture, Result};
use intent_store::Store;

use crate::connector::{RemoteNode, Tuning};
use crate::link::forge_token::ForgeTokens;
use crate::machine::MachineTarget;
use crate::{lock, NodeInbound};

/// A held lifecycle lock: the machine's lifecycle is free again once it is
/// dropped. Opaque, so a connector owner in another process can hand out
/// its own kind of lock.
pub type LifecycleGuard = Box<dyn Send + Sync>;

/// The connectors to the machines this head links to, and the lock each
/// machine's lifecycle changes take turns under.
pub trait NodeConnections: Send + Sync + 'static {
    /// The running connector to `machine_id`, if one was started.
    fn running(&self, machine_id: &str) -> Option<RemoteNode>;

    /// Every running connector.
    fn all(&self) -> Vec<RemoteNode>;

    /// How many machines hold a live link now.
    fn connected_machines(&self) -> usize;

    /// Start `target`'s connector, unless one started meanwhile: then that
    /// one is kept and the new one is shut down. Starting dials nothing.
    fn start(
        &self,
        store: Store,
        target: MachineTarget,
        tuning: Tuning,
        inbound: Option<Arc<dyn NodeInbound>>,
        forge: Option<Arc<ForgeTokens>>,
    ) -> BoxFuture<'_, Result<RemoteNode>>;

    /// Forget `machine_id`'s connector and hand it back, still running.
    fn take(&self, machine_id: &str) -> Option<RemoteNode>;

    /// Wait for `machine_id`'s lifecycle lock: adding, replacing or
    /// removing a machine and starting an agent on it take turns, so no
    /// agent starts under a credential that is being replaced or removed.
    fn lifecycle<'a>(&'a self, machine_id: &'a str) -> BoxFuture<'a, LifecycleGuard>;
}

/// [`NodeConnections`] for connectors running in this process.
#[derive(Default)]
pub struct InProcessNodeConnections {
    nodes: Mutex<HashMap<String, RemoteNode>>,
    lifecycles: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl NodeConnections for InProcessNodeConnections {
    fn running(&self, machine_id: &str) -> Option<RemoteNode> {
        lock(&self.nodes).get(machine_id).cloned()
    }

    fn all(&self) -> Vec<RemoteNode> {
        lock(&self.nodes).values().cloned().collect()
    }

    fn connected_machines(&self) -> usize {
        lock(&self.nodes)
            .values()
            .filter(|node| node.status().connected)
            .count()
    }

    fn start(
        &self,
        store: Store,
        target: MachineTarget,
        tuning: Tuning,
        inbound: Option<Arc<dyn NodeInbound>>,
        forge: Option<Arc<ForgeTokens>>,
    ) -> BoxFuture<'_, Result<RemoteNode>> {
        Box::pin(async move {
            let machine_id = target.id.clone();
            let node = RemoteNode::start(store, target, tuning, inbound, forge).await?;
            let mut nodes = lock(&self.nodes);
            if let Some(existing) = nodes.get(&machine_id) {
                node.shutdown();
                return Ok(existing.clone());
            }
            nodes.insert(machine_id, node.clone());
            Ok(node)
        })
    }

    fn take(&self, machine_id: &str) -> Option<RemoteNode> {
        lock(&self.nodes).remove(machine_id)
    }

    fn lifecycle<'a>(&'a self, machine_id: &'a str) -> BoxFuture<'a, LifecycleGuard> {
        let machine = lock(&self.lifecycles)
            .entry(machine_id.to_owned())
            .or_default()
            .clone();
        Box::pin(async move { Box::new(machine.lock_owned().await) as LifecycleGuard })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use intent_core::nodes::NodeCounter;

    use super::*;

    /// One task per entry of `machines` takes that machine's lifecycle lock
    /// and holds it briefly; the most tasks that held a lock at once.
    async fn most_at_once(connections: &Arc<InProcessNodeConnections>, machines: &[&str]) -> usize {
        let held = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for machine in machines {
            let (connections, held, most) = (connections.clone(), held.clone(), most.clone());
            let machine = (*machine).to_owned();
            tasks.push(tokio::spawn(async move {
                let _guard = connections.lifecycle(&machine).await;
                let now = held.fetch_add(1, Ordering::SeqCst) + 1;
                most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                held.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        most.load(Ordering::SeqCst)
    }

    #[tokio::test(start_paused = true)]
    async fn one_machine_lifecycle_runs_one_holder_at_a_time() {
        let connections = Arc::new(InProcessNodeConnections::default());
        assert_eq!(most_at_once(&connections, &["m1", "m1", "m1"]).await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn different_machines_do_not_wait_on_each_other() {
        let connections = Arc::new(InProcessNodeConnections::default());
        assert_eq!(most_at_once(&connections, &["m1", "m2", "m3"]).await, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_waiter_leaves_the_lock_free() {
        let connections = Arc::new(InProcessNodeConnections::default());
        let held = connections.lifecycle("m1").await;
        let waiter = {
            let connections = connections.clone();
            tokio::spawn(async move { drop(connections.lifecycle("m1").await) })
        };
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!waiter.is_finished(), "the waiter queues behind the holder");
        waiter.abort();
        drop(held);
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(1)),
            connections.lifecycle("m1"),
        )
        .await
        .expect("the lock is free once its holder and the dropped waiter are gone");
    }

    /// A store holding a live lease on machine `m1`, and `m1`'s target.
    async fn leased_machine() -> (tempfile::TempDir, Store, MachineTarget) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("intent.db")).await.unwrap();
        let target = MachineTarget::from_parts(
            "m1",
            "Machine",
            vec!["127.0.0.1".into()],
            7443,
            None,
            &"a".repeat(64),
            &"t".repeat(32),
            None,
        )
        .unwrap();
        let owner = crate::lease::new_owner(&store, "m1").await.unwrap();
        crate::lease::record_new_lease(&store, &target, &owner, NodeCounter(1))
            .await
            .unwrap();
        (dir, store, target)
    }

    #[tokio::test]
    async fn a_second_start_keeps_the_running_connector() {
        let (_dir, store, target) = leased_machine().await;
        let connections = InProcessNodeConnections::default();
        let first = connections
            .start(store.clone(), target.clone(), Tuning::default(), None, None)
            .await
            .unwrap();
        let second = connections
            .start(store, target, Tuning::default(), None, None)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first.shared(), &second.shared()));
        assert!(Arc::ptr_eq(
            &connections.running("m1").unwrap().shared(),
            &first.shared()
        ));
        assert_eq!(connections.all().len(), 1);
        let taken = connections.take("m1").unwrap();
        assert!(Arc::ptr_eq(&taken.shared(), &first.shared()));
        assert!(connections.running("m1").is_none());
        taken.shutdown();
    }

    #[tokio::test]
    async fn a_start_without_a_live_lease_fails_and_keeps_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("intent.db")).await.unwrap();
        let (_other, _, target) = leased_machine().await;
        let connections = InProcessNodeConnections::default();
        assert!(connections
            .start(store, target, Tuning::default(), None, None)
            .await
            .is_err());
        assert!(connections.all().is_empty());
    }

    #[tokio::test]
    async fn an_unknown_machine_has_no_connector() {
        let connections = InProcessNodeConnections::default();
        assert!(connections.running("m1").is_none());
        assert!(connections.all().is_empty());
        assert_eq!(connections.connected_machines(), 0);
        assert!(connections.take("m1").is_none());
    }
}
