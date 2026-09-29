//! Provider boundary only. Static hosts belong to their operator: release stops
//! Intent agents through an authenticated control adapter, never the host.
use intent_core::{nodes::LeaseOwner, Error, Result};
use std::{future::Future, pin::Pin};

pub type ProviderFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeCapability {
    Provision,
    Pause,
    Resume,
    Snapshot,
    Fork,
}

/// The provider does not authorize principals. The composition root supplies a
/// lease-bound control adapter only after normal admission/ownership checks.
pub trait NodeProvider: Send + Sync {
    fn id(&self) -> &'static str;
    fn supports(&self, capability: NodeCapability) -> bool;
    fn perform(&self, capability: NodeCapability) -> ProviderFuture<'_, ()>;
    /// Return only after all managed processes for this lease have stopped.
    /// Persistence must keep releaseRequested set until this succeeds or a
    /// separately verified offline-budget fence is recorded.
    fn release<'a>(&'a self, owner: &'a LeaseOwner) -> ProviderFuture<'a, ()>;
}

pub trait StaticNodeControl: Send + Sync {
    fn stop_managed_agents<'a>(&'a self, owner: &'a LeaseOwner) -> ProviderFuture<'a, ()>;
}

pub struct StaticNodeProvider<C> {
    control: C,
}
impl<C> StaticNodeProvider<C> {
    #[must_use]
    pub fn new(control: C) -> Self {
        Self { control }
    }
}
impl<C: StaticNodeControl> NodeProvider for StaticNodeProvider<C> {
    fn id(&self) -> &'static str {
        "static"
    }
    fn supports(&self, _: NodeCapability) -> bool {
        false
    }
    fn perform(&self, capability: NodeCapability) -> ProviderFuture<'_, ()> {
        Box::pin(async move { Err(Error::Unsupported(format!("static node {capability:?}"))) })
    }
    fn release<'a>(&'a self, owner: &'a LeaseOwner) -> ProviderFuture<'a, ()> {
        self.control.stop_managed_agents(owner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Control {
        stopped: Mutex<Vec<LeaseOwner>>,
        fail: bool,
    }
    impl StaticNodeControl for Control {
        fn stop_managed_agents<'a>(&'a self, owner: &'a LeaseOwner) -> ProviderFuture<'a, ()> {
            Box::pin(async move {
                if self.fail {
                    return Err(Error::Internal("offline".into()));
                }
                let mut stopped = self.stopped.lock().unwrap();
                if !stopped.contains(owner) {
                    stopped.push(owner.clone());
                }
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn static_release_only_stops_managed_agents_and_propagates_failure() {
        let provider = StaticNodeProvider::new(Control {
            stopped: Mutex::new(vec![]),
            fail: false,
        });
        let owner = LeaseOwner {
            head_id: "head".into(),
            node_id: "node".into(),
            node_identity: "identity".into(),
            lease_id: "lease".into(),
            incarnation: "incarnation".into(),
        };
        provider.release(&owner).await.unwrap();
        provider.release(&owner).await.unwrap();
        assert_eq!(
            *provider.control.stopped.lock().unwrap(),
            vec![owner.clone()]
        );
        for capability in [
            NodeCapability::Provision,
            NodeCapability::Pause,
            NodeCapability::Resume,
            NodeCapability::Snapshot,
            NodeCapability::Fork,
        ] {
            assert!(!provider.supports(capability));
            assert!(matches!(
                provider.perform(capability).await,
                Err(Error::Unsupported(_))
            ));
        }
        let offline = StaticNodeProvider::new(Control {
            stopped: Mutex::new(vec![]),
            fail: true,
        });
        assert!(offline.release(&owner).await.is_err());
    }
}
