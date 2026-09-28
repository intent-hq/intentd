//! Original Services retention beside one physical request, never read admission.
//!
//! The Services-only constructor lives in installation.rs. Shared request code
//! retains its opaque allocation without depending on Services or reconstructing
//! an owner from a caller, row, path or later Store.

use std::any::Any;
use std::sync::Arc;

use intent_acp::mcp_server::private_results::McpPrivatePolicy;
use intent_core::caller::current_caller;
use intent_store::{RepositoryLifecycleKey, RepositoryLifecycleObserver, Store};

use super::lifecycle::{
    RepositoryLifecycleRegistry, RepositorySourceLifetime, RepositorySubscription,
};
use super::request_context::RepositoryCapturedRequest;
use super::{AdmissionError, AdmissionResult, RepositoryRetirement};

pub(crate) type ReadPolicyFactory =
    dyn Fn(Arc<RepositoryReadRequest>) -> Arc<dyn McpPrivatePolicy> + Send + Sync;

pub(crate) struct RepositoryReadOwner {
    // Storage only: downcasting this allocation cannot supply authority.
    original: Arc<dyn Any + Send + Sync>,
    store: Store,
    registry: Arc<RepositoryLifecycleRegistry>,
    policy: Option<Arc<ReadPolicyFactory>>,
}

impl RepositoryReadOwner {
    /// Called only by the typed Services constructor with that same instance's
    /// fields. This neither installs an observer nor captures a physical leaf.
    pub(crate) fn retain_original(
        original: Arc<dyn Any + Send + Sync>,
        store: Store,
        registry: Arc<RepositoryLifecycleRegistry>,
    ) -> AdmissionResult<Arc<Self>> {
        let observer: Arc<dyn RepositoryLifecycleObserver> = registry.clone();
        if !store.has_repository_lifecycle_observer(&observer) {
            return Err(AdmissionError::Unavailable);
        }
        Ok(Arc::new(Self {
            original,
            store,
            registry,
            policy: None,
        }))
    }

    pub(crate) fn with_policy(mut self: Arc<Self>, factory: Arc<ReadPolicyFactory>) -> Arc<Self> {
        // The typed factory calls this before exposing the retained owner.
        Arc::get_mut(&mut self)
            .expect("unpublished read owner")
            .policy = Some(factory);
        self
    }
}

/// The sidecar retains the SAME request captured by the ordinary ACP scope.
/// It owns no physical token and cannot extend the physical handle's lifetime.
pub(crate) struct RepositoryReadRequest {
    owner: Arc<RepositoryReadOwner>,
    original: Arc<RepositoryCapturedRequest>,
    correlation: String,
}

impl RepositoryReadRequest {
    /// Prequeue capture uses the authentic physical caller already captured by
    /// the registry. The ambient caller is restored later by the existing ACP
    /// scope and must match before any future consumer may use this evidence.
    pub(super) fn capture(
        owner: Arc<RepositoryReadOwner>,
        original: Arc<RepositoryCapturedRequest>,
    ) -> AdmissionResult<Arc<Self>> {
        original.check_read_owner(&owner.store, &owner.registry)?;
        Ok(Arc::new(Self {
            owner,
            original,
            correlation: uuid::Uuid::new_v4().to_string(),
        }))
    }

    pub(crate) fn check_current(&self) -> AdmissionResult<()> {
        self.original
            .check_read_owner(&self.owner.store, &self.owner.registry)?;
        self.original.check_read_caller()
    }

    pub(crate) fn policy(self: &Arc<Self>) -> Option<Arc<dyn McpPrivatePolicy>> {
        self.owner
            .policy
            .as_ref()
            .map(|factory| factory(self.clone()))
    }

    pub(crate) fn correlation(&self) -> &str {
        &self.correlation
    }

    pub(crate) fn retains<T: Any + Send + Sync>(&self, original: &T) -> bool {
        self.owner
            .original
            .downcast_ref::<T>()
            .is_some_and(|retained| std::ptr::eq(retained, original))
    }

    /// One fresh source child of this same original request, before any await.
    pub(crate) fn child(self: &Arc<Self>) -> AdmissionResult<RepositoryReadChild> {
        self.check_current()?;
        Ok(RepositoryReadChild {
            request: self.clone(),
            lifetime: self.original.source_lifetime()?,
            subscriptions: Vec::new(),
        })
    }
}

/// Owns cleanup independently of escaped metadata and authority handles.
pub(crate) struct RepositoryReadChild {
    request: Arc<RepositoryReadRequest>,
    lifetime: RepositorySourceLifetime,
    subscriptions: Vec<RepositorySubscription>,
}

impl RepositoryReadChild {
    pub(crate) fn retirement(&self) -> RepositoryRetirement {
        self.lifetime.retirement()
    }

    pub(crate) fn subscribe(&mut self, keys: &[RepositoryLifecycleKey]) -> AdmissionResult<()> {
        self.request.check_current()?;
        let caller = current_caller().ok_or(AdmissionError::Denied)?;
        self.subscriptions.push(self.lifetime.subscribe(
            &self.request.owner.store,
            &caller,
            keys,
        )?);
        Ok(())
    }

    pub(crate) fn transfer<T>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        self.request.check_current()?;
        self.lifetime.retirement().dispatch(action)
    }
}

impl Drop for RepositoryReadChild {
    fn drop(&mut self) {
        self.lifetime.retirement().end_scope();
    }
}

#[cfg(test)]
#[path = "read_request/tests.rs"]
mod tests;
