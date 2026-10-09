//! Original Services retention beside one physical request, never read admission.
//!
//! The Services-only constructor lives in installation.rs. Shared request code
//! retains its opaque allocation without depending on Services or reconstructing
//! an owner from a caller, row, path or later Store.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_acp::mcp_server::private_results::McpPrivatePolicy;
use intent_core::caller::current_caller;
use intent_store::{RepositoryLifecycleKey, RepositoryLifecycleObserver, Store};

use super::lifecycle::{
    RepositoryLifecycleRegistry, RepositorySourceLifetime, RepositorySubscription,
};
use super::request_context::{
    require_mandatory_execution, restore_optional, RepositoryCapturedRequest,
};
use super::{AdmissionError, AdmissionResult, RepositoryRetirement};

pub(crate) type ReadPolicyFactory =
    dyn Fn(Arc<RepositoryReadRequest>) -> Arc<dyn McpPrivatePolicy> + Send + Sync;

enum OriginalReadOwner {
    Request(Arc<dyn Any + Send + Sync>),
    Monitor(Weak<dyn Any + Send + Sync>),
}

pub(crate) struct RepositoryReadOwner {
    // Storage only: downcasting this allocation cannot supply authority.
    original: OriginalReadOwner,
    store: Store,
    registry: Arc<RepositoryLifecycleRegistry>,
    policy: Option<Arc<ReadPolicyFactory>>,
    #[cfg(test)]
    review_monitoring: bool,
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
            original: OriginalReadOwner::Request(original),
            store,
            registry,
            policy: None,
            #[cfg(test)]
            review_monitoring: false,
        }))
    }

    pub(crate) fn retains<T: Any + Send + Sync>(&self, original: &T) -> bool {
        let retained = match &self.original {
            OriginalReadOwner::Request(value) => Some(value.clone()),
            OriginalReadOwner::Monitor(value) => value.upgrade(),
        };
        retained
            .as_ref()
            .and_then(|value| value.downcast_ref::<T>())
            .is_some_and(|retained| std::ptr::eq(retained, original))
    }

    pub(crate) fn with_policy(mut self: Arc<Self>, factory: Arc<ReadPolicyFactory>) -> Arc<Self> {
        // The typed factory calls this before exposing the retained owner.
        Arc::get_mut(&mut self)
            .expect("unpublished read owner")
            .policy = Some(factory);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_review_monitoring(mut self: Arc<Self>) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("unpublished read owner")
            .review_monitoring = true;
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
    pub(crate) fn retain_monitor(&self) -> AdmissionResult<Arc<Self>> {
        self.check_current()?;
        let original = match &self.owner.original {
            OriginalReadOwner::Request(value) => Arc::downgrade(value),
            OriginalReadOwner::Monitor(_) => return Err(AdmissionError::Denied),
        };
        // No Services -> monitor -> Services cycle and no retained short-call
        // output policy. Physical retirement remains an independent obligation.
        let owner = Arc::new(RepositoryReadOwner {
            original: OriginalReadOwner::Monitor(original),
            store: self.owner.store.clone(),
            registry: self.owner.registry.clone(),
            policy: None,
            #[cfg(test)]
            review_monitoring: self.owner.review_monitoring,
        });
        Self::capture(owner, self.original.retain_monitor()?)
    }

    /// No production harness is qualified for monitor delivery yet. The later
    /// lifecycle activation must install a receipt at original session creation;
    /// request arguments, persisted metadata and ambient caller IDs cannot opt in.
    pub(crate) fn supports_review_monitoring(&self) -> bool {
        if self.check_current().is_err() {
            return false;
        }
        #[cfg(test)]
        return self.owner.review_monitoring;
        #[cfg(not(test))]
        false
    }

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

    pub(crate) fn check_optional_current(&self) -> AdmissionResult<()> {
        if !self.original.original_caller_matches() {
            return Err(AdmissionError::Denied);
        }
        self.original.retirement().with_optional(|current| {
            if current {
                Ok(())
            } else {
                Err(AdmissionError::Retired)
            }
        })
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
        self.owner.retains(original)
    }

    pub(crate) fn capture_optional(self: &Arc<Self>) -> AdmissionResult<RepositoryOptionalScope> {
        require_mandatory_execution()?;
        self.check_current()?;
        let (lifetime, subscription) = self.original.optional_lifetime()?;
        let scope = RepositoryOptionalScope {
            metadata: RepositoryOptionalMetadata(Arc::new(OptionalState {
                request: self.clone(),
                lifetime,
                subscriptions: Mutex::new(vec![subscription]),
                prepared: AtomicBool::new(false),
            })),
        };
        scope.metadata.check_current()?;
        Ok(scope)
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

/// Service-owned account fence for one already prepared database mutation.
/// Shared request code does not import the concrete Services credential owner.
pub(crate) trait MonitorCommitAuthority {
    fn commit_prepared(
        &self,
        write: intent_store::PreparedMonitorWrite,
    ) -> AdmissionResult<intent_core::Result<bool>>;
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

    /// Finish only a prepared monitor transaction on its blocking worker.
    /// The original async caller was validated before dispatch; this retained
    /// leaf joins physical retirement through the database commit itself.
    pub(crate) fn commit_monitor_write(
        &self,
        eligibility: &impl MonitorCommitAuthority,
        write: intent_store::PreparedMonitorWrite,
    ) -> AdmissionResult<intent_core::Result<bool>> {
        self.lifetime
            .retirement()
            .dispatch(|| eligibility.commit_prepared(write))
    }

    pub(crate) fn transfer<T>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        require_mandatory_execution()?;
        self.request.check_current()?;
        self.lifetime.retirement().dispatch(action)
    }
    pub(crate) fn transfer_with_optional<T>(
        &self,
        optional: Option<&RepositoryOptionalMetadata>,
        action: impl FnOnce(bool) -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        require_mandatory_execution()?;
        self.request.check_current()?;
        if optional.is_some_and(|local| !Arc::ptr_eq(&local.0.request, &self.request)) {
            return Err(AdmissionError::Denied);
        }
        self.lifetime.retirement().dispatch(|| match optional {
            Some(local) => local.0.lifetime.retirement().with_optional(|include| {
                action(include && local.0.prepared.load(Ordering::Acquire))
            }),
            None => action(false),
        })
    }
}

impl Drop for RepositoryReadChild {
    fn drop(&mut self) {
        self.lifetime.retirement().end_scope();
    }
}

struct OptionalState {
    request: Arc<RepositoryReadRequest>,
    lifetime: RepositorySourceLifetime,
    subscriptions: Mutex<Vec<RepositorySubscription>>,
    prepared: AtomicBool,
}

/// Identity metadata alone cannot keep the nonclone owner's scope alive.
#[derive(Clone)]
pub(crate) struct RepositoryOptionalMetadata(Arc<OptionalState>);

impl RepositoryOptionalMetadata {
    pub(crate) fn check_current(&self) -> AdmissionResult<()> {
        self.0.request.check_current()?;
        self.0.lifetime.retirement().check_current()
    }

    pub(crate) fn cancel(&self) {
        self.0.lifetime.retirement().end_scope();
    }

    pub(crate) fn request(&self) -> &Arc<RepositoryReadRequest> {
        &self.0.request
    }

    /// Optional-only entry: acquire the original parent once, nonblocking. No
    /// `check_current` call may wait on that same fence before this try operation.
    pub(crate) fn transfer_optional<T>(&self, action: impl FnOnce(bool) -> T) -> T {
        if !self.0.request.original.original_caller_matches() {
            return action(false);
        }
        self.0
            .request
            .original
            .retirement()
            .with_optional(|parent| {
                if !parent {
                    return action(false);
                }
                self.0
                    .lifetime
                    .retirement()
                    .with_optional(|local| action(local && self.0.prepared.load(Ordering::Acquire)))
            })
    }

    pub(crate) fn subscribe_metadata(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> AdmissionResult<()> {
        self.check_current()?;
        let caller = current_caller().ok_or(AdmissionError::Denied)?;
        let subscription = self
            .0
            .lifetime
            .subscribe(&self.0.request.owner.store, &caller, keys)?;
        let mut subscriptions = self
            .0
            .subscriptions
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        if self.0.lifetime.retirement().is_closed()
            || self.0.request.original.retirement().is_closed()
        {
            return Err(AdmissionError::Retired);
        }
        subscriptions.push(subscription);
        Ok(())
    }
}

pub(crate) struct RepositoryOptionalScope {
    metadata: RepositoryOptionalMetadata,
}

impl RepositoryOptionalScope {
    pub(crate) fn metadata(&self) -> RepositoryOptionalMetadata {
        self.metadata.clone()
    }

    /// Capture entry authority now; invoke the constructor only inside the
    /// restored optional scope. Self already owns cleanup for an unpolled drop.
    pub(crate) fn run_optional<'a, T, F, Fut>(
        self,
        make: F,
    ) -> AdmissionResult<intent_core::BoxFuture<'a, AdmissionResult<PreparedRepositoryOptional<T>>>>
    where
        T: Send + 'a,
        F: FnOnce(RepositoryOptionalMetadata) -> Fut + Send + 'a,
        Fut: std::future::Future<Output = AdmissionResult<T>> + Send + 'a,
    {
        self.metadata.check_current()?;
        let request = self.metadata.0.request.clone();
        let original = request.original.clone();
        let local = self.metadata.0.lifetime.retirement();
        let parent = original.retirement();
        restore_optional(
            original,
            request,
            local.clone(),
            Box::pin(async move {
                let value = tokio::select! {
                    biased;
                    () = local.cancelled() => return Err(AdmissionError::Retired),
                    () = parent.cancelled() => return Err(AdmissionError::Retired),
                    result = async { make(self.metadata.clone()).await } => result?,
                };
                self.metadata.check_current()?;
                self.metadata.0.prepared.store(true, Ordering::Release);
                Ok(PreparedRepositoryOptional { scope: self, value })
            }),
        )
    }
}

impl Drop for RepositoryOptionalScope {
    fn drop(&mut self) {
        let local = self.metadata.0.lifetime.retirement();
        local.end_scope();
        let subscriptions = std::mem::take(
            &mut *self
                .metadata
                .0
                .subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        drop(subscriptions);
        self.metadata
            .0
            .request
            .original
            .retirement()
            .unlink_optional(&local);
    }
}

pub(crate) struct PreparedRepositoryOptional<T> {
    scope: RepositoryOptionalScope,
    value: T,
}

impl<T> PreparedRepositoryOptional<T> {
    pub(crate) fn metadata(&self) -> &RepositoryOptionalMetadata {
        &self.scope.metadata
    }

    pub(crate) fn map<U>(self, map: impl FnOnce(T) -> U) -> PreparedRepositoryOptional<U> {
        PreparedRepositoryOptional {
            scope: self.scope,
            value: map(self.value),
        }
    }

    pub(crate) fn value(&self) -> &T {
        &self.value
    }
}

#[cfg(test)]
#[path = "read_request/tests.rs"]
mod tests;
