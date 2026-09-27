//! Installation evidence for the original Services instance, never permission.

use std::sync::Arc;

use intent_store::RepositoryLifecycleObserver;

use crate::repository_admission::lifecycle::RepositoryLifecycleRegistry;
use crate::repository_admission::{AdmissionError, AdmissionResult};
use crate::Services;

impl Services {
    /// Return this instance's original registry only after its Store accepts
    /// the same observer. Retained mutation barriers remain owned by their
    /// original writers; installation cannot settle or replace them.
    pub(crate) async fn repository_lifecycle_registry(
        &self,
    ) -> AdmissionResult<Arc<RepositoryLifecycleRegistry>> {
        let registry = self.repository_lifecycle_registry.clone();
        registry.install(self.store()).await?;
        let observer: Arc<dyn RepositoryLifecycleObserver> = registry.clone();
        if !self.store().has_repository_lifecycle_observer(&observer) {
            return Err(AdmissionError::Unavailable);
        }
        Ok(registry)
    }
}

#[cfg(all(test, unix))]
#[path = "installation/tests.rs"]
mod tests;
