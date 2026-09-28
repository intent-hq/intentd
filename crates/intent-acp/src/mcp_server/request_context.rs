//! In-process request context captured by an original MCP endpoint.
//!
//! This transport seam carries context, not authority. Its trusted owner must
//! keep unavailable/retired captures unavailable and authorize each operation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::private_results::{McpPrivateInvocation, McpPrivatePolicy};
use intent_core::Caller;

/// A context wrapper's body. Unit output keeps transport response types private.
pub type McpContextFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Installed once on the original endpoint by its trusted owner.
pub trait McpRequestContext: Send + Sync {
    /// Snapshot the original request identity synchronously, before queuing.
    ///
    /// This must be bounded, must not start an operation, and must not resolve a
    /// replacement owner by agent ID. A pending/absent/retired origin produces a
    /// permanently non-admitting snapshot, even if a new owner becomes ready.
    fn capture(&self) -> Arc<dyn McpRequestScope>;
}

/// One captured request, shared by the operation and its optional guidance.
pub trait McpRequestScope: Send + Sync {
    /// Optional original-request private-result policy. No existing endpoint
    /// enables it. A qualified producer must require a reservation from this
    /// carrier before acquiring private data; absence is never an ordinary
    /// fallback for a qualified read.
    fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
        None
    }

    /// Apply only the captured context while awaiting `request` exactly once.
    ///
    /// Do not replace, skip, detach or retry the body, and do not recapture the
    /// current owner after an await. Retirement is observed by the service's
    /// admission checks; it must not discard an ordinary completed result.
    /// Dropping this future must drop its body without leaving detached work.
    fn scope<'a>(&'a self, request: McpContextFuture<'a>) -> McpContextFuture<'a>;
}

#[derive(Clone)]
pub(crate) struct CapturedRequestContext {
    caller: Option<Caller>,
    scope: Option<Arc<dyn McpRequestScope>>,
    pub(crate) private_invocation: Option<McpPrivateInvocation>,
}

impl CapturedRequestContext {
    pub(crate) fn capture(caller: Option<Caller>, context: Option<&dyn McpRequestContext>) -> Self {
        Self::capture_with_budget(caller, context, std::time::Duration::from_secs(120))
    }

    pub(crate) fn capture_with_budget(
        caller: Option<Caller>,
        context: Option<&dyn McpRequestContext>,
        budget: std::time::Duration,
    ) -> Self {
        let scope = context.map(McpRequestContext::capture);
        let private_invocation = scope
            .as_ref()
            .and_then(|scope| scope.private_result_policy())
            .map(|policy| McpPrivateInvocation::new(policy, budget));
        Self {
            caller,
            scope,
            private_invocation,
        }
    }

    pub(crate) async fn run<T: Send>(&self, request: impl Future<Output = T> + Send) -> T {
        let scoped = McpPrivateInvocation::scope(self.private_invocation.clone(), async {
            if let Some(scope) = &self.scope {
                let mut result = None;
                scope
                    .scope(Box::pin(async { result = Some(request.await) }))
                    .await;
                result.expect("MCP request scope must await its body exactly once")
            } else {
                request.await
            }
        });
        match &self.caller {
            Some(caller) => intent_core::with_caller(caller.clone(), scoped).await,
            None => scoped.await,
        }
    }
}

#[cfg(test)]
mod tests;
