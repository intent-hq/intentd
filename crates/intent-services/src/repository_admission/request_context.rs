//! Original physical callback capture, before any queue or operation await.
//!
//! A callback projection is immutable. A pending/absent projection cannot gain
//! an origin after initialization. Existing caller and Store gates still own
//! permission; this context only retains and retires the original request leaf.

use std::sync::{Arc, Mutex, Weak};

use intent_acp::mcp_server::request_context::{
    McpContextFuture, McpRequestContext, McpRequestScope,
};
use intent_core::caller::{current_caller, current_wire_credential, Caller};
use intent_store::{RepositoryLifecycleObserver, Store};

use super::lifecycle::physical_owner::RepositoryPhysicalOwner;
use super::lifecycle::{
    RepositoryLifecycleRegistry, RepositoryPhysicalOrigin, RepositorySourceLifetime,
    RepositorySubscription,
};
use super::read_request::{RepositoryReadOwner, RepositoryReadRequest};
use super::{AdmissionError, AdmissionResult, RepositoryRetirement};

/// A distinct callback allocation may receive a confirmed origin. The physical
/// owner constructor is deliberately the only future production producer.
pub(crate) struct RepositoryCallbackContext {
    registry: Weak<RepositoryLifecycleRegistry>,
    origin: Option<RepositoryPhysicalOrigin>,
    store: Option<Store>,
    read_owner: Option<AdmissionResult<Arc<RepositoryReadOwner>>>,
}

impl RepositoryCallbackContext {
    pub(crate) fn new(
        registry: &Arc<RepositoryLifecycleRegistry>,
        origin: Option<RepositoryPhysicalOrigin>,
    ) -> Self {
        Self {
            registry: Arc::downgrade(registry),
            origin,
            store: None,
            read_owner: None,
        }
    }

    pub(super) fn for_physical_owner(owner: &RepositoryPhysicalOwner) -> Self {
        let (registry, origin, store) = owner.callback_binding();
        Self {
            registry: Arc::downgrade(registry),
            origin: Some(origin),
            store: Some(store.clone()),
            read_owner: None,
        }
    }

    /// Attach once, retaining success or failure. A later Services lookup must
    /// never repair this callback's original unavailable read anchor.
    pub(crate) fn with_read_owner(
        mut self,
        original: AdmissionResult<Arc<RepositoryReadOwner>>,
    ) -> Self {
        if self.read_owner.is_none() {
            self.read_owner = Some(original);
        }
        self
    }

    /// Capture even an unavailable result. No later scope may fill a missing
    /// origin from the current agent, callback, task or session row.
    pub(crate) fn capture(&self) -> Arc<RepositoryCapturedRequest> {
        let retirement = RepositoryRetirement::default();
        let captured = (|| {
            let registry = self.registry.upgrade().ok_or(AdmissionError::Retired)?;
            let origin = self.origin.clone().ok_or(AdmissionError::Unavailable)?;
            let (caller, subscription) = registry.capture_request(&origin, retirement.clone())?;
            Ok(Captured {
                registry,
                origin,
                caller,
                store: self.store.clone(),
                subscriptions: Arc::new(Mutex::new(vec![subscription])),
            })
        })();
        Arc::new(RepositoryCapturedRequest {
            captured,
            retirement,
        })
    }
}

impl McpRequestContext for RepositoryCallbackContext {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        let original = self.capture();
        let read = self
            .read_owner
            .clone()
            .unwrap_or(Err(AdmissionError::Unavailable))
            .and_then(|owner| RepositoryReadRequest::capture(owner, original.clone()));
        Arc::new(RepositoryRequestScope { original, read })
    }
}

tokio::task_local! {
    static CAPTURED_REQUEST: Arc<RepositoryCapturedRequest>;
    static CAPTURED_READ_REQUEST: AdmissionResult<Arc<RepositoryReadRequest>>;
}

struct RepositoryRequestScope {
    original: Arc<RepositoryCapturedRequest>,
    read: AdmissionResult<Arc<RepositoryReadRequest>>,
}

impl Drop for RepositoryRequestScope {
    fn drop(&mut self) {
        // Metadata may retain the request allocation, but only the original
        // MCP scope owners may keep it live through body and preparation.
        self.original.retirement.retire();
    }
}

struct RetireCancelledScope {
    retirement: RepositoryRetirement,
    completed: bool,
}

impl RetireCancelledScope {
    fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for RetireCancelledScope {
    fn drop(&mut self) {
        if !self.completed {
            self.retirement.retire();
        }
    }
}

impl McpRequestScope for RepositoryRequestScope {
    fn private_result_policy(
        &self,
    ) -> Option<Arc<dyn intent_acp::mcp_server::private_results::McpPrivatePolicy>> {
        self.read
            .as_ref()
            .ok()
            .and_then(RepositoryReadRequest::policy)
    }

    fn scope<'a>(&'a self, request: McpContextFuture<'a>) -> McpContextFuture<'a> {
        // Construct the guard before the future: even an unpolled cancelled
        // scope must retire this request, without retiring the physical owner.
        let guard = RetireCancelledScope {
            retirement: self.original.retirement.clone(),
            completed: false,
        };
        let captured = self.original.clone();
        let read = self.read.clone();
        Box::pin(async move {
            CAPTURED_REQUEST
                .scope(captured, CAPTURED_READ_REQUEST.scope(read, request))
                .await;
            // The same scope also surrounds separately bounded preparation.
            // Normal completion keeps it live until the final scope Arc drops.
            guard.complete();
        })
    }
}

/// Only the original ACP capture can supply this task-local lifetime. Missing
/// or unavailable scope is never repaired from the currently running agent.
pub(crate) fn current_source_lifetime() -> AdmissionResult<RepositorySourceLifetime> {
    CAPTURED_REQUEST
        .try_with(|request| request.source_lifetime())
        .map_err(|_| AdmissionError::Unavailable)?
}

/// Ownership evidence only, under the restored original caller. A successful
/// result supplies no read permission, source facts or response admission.
pub(crate) fn current_read_request() -> AdmissionResult<Arc<RepositoryReadRequest>> {
    let read = CAPTURED_READ_REQUEST
        .try_with(Clone::clone)
        .map_err(|_| AdmissionError::Unavailable)??;
    read.check_current()?;
    Ok(read)
}

/// Permanent observed denial ends this original request, not just one lock
/// session. Transient source unavailability leaves later preparation possible.
pub(crate) fn retire_current_request_on_denial(error: AdmissionError) {
    if matches!(
        error,
        AdmissionError::Denied | AdmissionError::Retired | AdmissionError::BindingChanged
    ) {
        let _ = CAPTURED_REQUEST.try_with(|request| request.retirement.retire());
    }
}

struct Captured {
    registry: Arc<RepositoryLifecycleRegistry>,
    origin: RepositoryPhysicalOrigin,
    caller: Caller,
    store: Option<Store>,
    subscriptions: Arc<Mutex<Vec<RepositorySubscription>>>,
}

pub(crate) struct RepositoryCapturedRequest {
    captured: AdmissionResult<Captured>,
    retirement: RepositoryRetirement,
}

impl RepositoryCapturedRequest {
    pub(super) fn check_read_owner(
        &self,
        store: &Store,
        registry: &Arc<RepositoryLifecycleRegistry>,
    ) -> AdmissionResult<()> {
        let captured = self.captured.as_ref().map_err(|error| *error)?;
        let original_store = captured.store.as_ref().ok_or(AdmissionError::Unavailable)?;
        if !Arc::ptr_eq(registry, &captured.registry)
            || !original_store.shares_repository_lifecycle_domain(store)
        {
            return Err(AdmissionError::Denied);
        }
        let observer: Arc<dyn RepositoryLifecycleObserver> = captured.registry.clone();
        if !original_store.has_repository_lifecycle_observer(&observer)
            || !store.has_repository_lifecycle_observer(&observer)
        {
            return Err(AdmissionError::Unavailable);
        }
        self.retirement.check_current()
    }

    pub(super) fn check_read_caller(&self) -> AdmissionResult<()> {
        let captured = self.captured.as_ref().map_err(|error| *error)?;
        if current_caller().as_ref() != Some(&captured.caller)
            || current_wire_credential().is_some()
        {
            return Err(AdmissionError::Denied);
        }
        self.retirement.check_current()
    }

    /// Invoke under the authentic transport caller scope. Neither a callback
    /// projection nor an installed invalidation observer supplies permission.
    pub(crate) fn source_lifetime(&self) -> AdmissionResult<RepositorySourceLifetime> {
        let captured = self.captured.as_ref().map_err(|error| *error)?;
        if current_caller().as_ref() != Some(&captured.caller)
            || current_wire_credential().is_some()
        {
            return Err(AdmissionError::Denied);
        }
        self.retirement.check_current()?;
        Ok(RepositorySourceLifetime::for_captured_request(
            captured.registry.clone(),
            captured.origin.clone(),
            self.retirement.clone(),
            captured.subscriptions.clone(),
        ))
    }
}

impl Drop for RepositoryCapturedRequest {
    fn drop(&mut self) {
        self.retirement.retire();
    }
}

#[cfg(test)]
#[path = "request_context/tests.rs"]
mod tests;
