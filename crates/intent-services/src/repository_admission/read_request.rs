//! Original Services retention beside one physical request, never read admission.
//!
//! The Services-only constructor lives in installation.rs. Shared request code
//! retains its opaque allocation without depending on Services or reconstructing
//! an owner from a caller, row, path or later Store.

use std::any::Any;
use std::sync::Arc;

use intent_store::{RepositoryLifecycleObserver, Store};

use super::lifecycle::RepositoryLifecycleRegistry;
use super::request_context::RepositoryCapturedRequest;
use super::{AdmissionError, AdmissionResult};

pub(crate) struct RepositoryReadOwner {
    // Storage only: downcasting this allocation cannot supply authority.
    _original: Arc<dyn Any + Send + Sync>,
    store: Store,
    registry: Arc<RepositoryLifecycleRegistry>,
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
            _original: original,
            store,
            registry,
        }))
    }
}

/// The sidecar retains the SAME request captured by the ordinary ACP scope.
/// It owns no physical token and cannot extend the physical handle's lifetime.
pub(crate) struct RepositoryReadRequest {
    owner: Arc<RepositoryReadOwner>,
    original: Arc<RepositoryCapturedRequest>,
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
        Ok(Arc::new(Self { owner, original }))
    }

    pub(crate) fn check_current(&self) -> AdmissionResult<()> {
        self.original
            .check_read_owner(&self.owner.store, &self.owner.registry)?;
        self.original.check_read_caller()
    }
}
