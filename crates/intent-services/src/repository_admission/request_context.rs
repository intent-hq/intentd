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

use super::lifecycle::{
    RepositoryLifecycleRegistry, RepositoryPhysicalOrigin, RepositorySourceLifetime,
    RepositorySubscription,
};
use super::{AdmissionError, AdmissionResult, RepositoryRetirement};

/// A distinct callback allocation may receive a confirmed origin. The physical
/// owner constructor is deliberately the only future production producer.
pub(crate) struct RepositoryCallbackContext {
    registry: Weak<RepositoryLifecycleRegistry>,
    origin: Option<RepositoryPhysicalOrigin>,
}

impl RepositoryCallbackContext {
    pub(crate) fn new(
        registry: &Arc<RepositoryLifecycleRegistry>,
        origin: Option<RepositoryPhysicalOrigin>,
    ) -> Self {
        Self {
            registry: Arc::downgrade(registry),
            origin,
        }
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
        Arc::new(RepositoryRequestScope(self.capture()))
    }
}

tokio::task_local! {
    static CAPTURED_REQUEST: Arc<RepositoryCapturedRequest>;
}

struct RepositoryRequestScope(Arc<RepositoryCapturedRequest>);

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
    fn scope<'a>(&'a self, request: McpContextFuture<'a>) -> McpContextFuture<'a> {
        // Construct the guard before the future: even an unpolled cancelled
        // scope must retire this request, without retiring the physical owner.
        let guard = RetireCancelledScope {
            retirement: self.0.retirement.clone(),
            completed: false,
        };
        let captured = self.0.clone();
        Box::pin(async move {
            CAPTURED_REQUEST.scope(captured, request).await;
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
    subscriptions: Arc<Mutex<Vec<RepositorySubscription>>>,
}

pub(crate) struct RepositoryCapturedRequest {
    captured: AdmissionResult<Captured>,
    retirement: RepositoryRetirement,
}

impl RepositoryCapturedRequest {
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
