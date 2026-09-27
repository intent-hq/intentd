//! In-process request context captured by an original MCP endpoint.
//!
//! This transport seam carries context, not authority. Its trusted owner must
//! keep unavailable/retired captures unavailable and authorize each operation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

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
}

impl CapturedRequestContext {
    pub(crate) fn capture(caller: Option<Caller>, context: Option<&dyn McpRequestContext>) -> Self {
        Self {
            caller,
            scope: context.map(McpRequestContext::capture),
        }
    }

    pub(crate) async fn run<T: Send>(&self, request: impl Future<Output = T> + Send) -> T {
        let scoped = async {
            if let Some(scope) = &self.scope {
                let mut result = None;
                scope
                    .scope(Box::pin(async { result = Some(request.await) }))
                    .await;
                result.expect("MCP request scope must await its body exactly once")
            } else {
                request.await
            }
        };
        match &self.caller {
            Some(caller) => intent_core::with_caller(caller.clone(), scoped).await,
            None => scoped.await,
        }
    }
}

#[cfg(test)]
mod tests;
