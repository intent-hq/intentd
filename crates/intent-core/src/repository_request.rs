//! Private, transport-neutral ownership of an original repository read request.
//!
//! These interfaces carry an existing service owner's scope. They neither
//! authenticate a caller nor grant repository access. They are deliberately
//! independent of transport frames, Store, provider credentials and Serde.

use std::sync::Arc;

use crate::{BoxFuture, Result};

/// Selected by the original transport admission, never by request parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryWireEntry {
    /// The actual authenticated connection's bearer binding.
    Bearer,
    /// A connection admitted by the local control transport.
    AdmittedLocal,
}

/// The router's typed service outcome, before JSON envelope construction.
///
/// Neither variant establishes whether the payload is private. In particular,
/// service errors can contain private data, and a successful value can itself
/// contain an `error` field. The original scope owns that classification and
/// retains actual provider error/quota evidence independently of delivery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepositoryReadReplyKind {
    Result,
    ServiceError,
}

/// One original authenticated connection, independent of client and RPC ids.
pub trait RepositoryReadConnection: Send + Sync {
    /// Capture synchronously before the request's first await or spawn.
    /// Each call returns a distinct original request, including equal RPC ids.
    fn capture(&self) -> Arc<dyn RepositoryReadRequestScope>;

    /// Retire only this connection's original request cohort. Idempotent.
    /// Concrete owners must join admitted leaves without holding cohort maps.
    fn retire(&self);
}

/// An original request's scope, kept until its final response handling finishes.
pub trait RepositoryReadRequestScope: Send + Sync {
    /// Restore the concrete owner's private context and run `body` exactly once.
    /// This must not turn an absent or retired capture into a later owner.
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()>;

    /// Complete/cancel the original request even if clones of this scope escape.
    fn retire(&self);

    /// Validate and admit one already prepared response using original evidence.
    ///
    /// An unused ordinary request may transfer directly. A qualified request
    /// must freshly revalidate within a new child of the SAME original request
    /// and consume its authority fence through `transfer`. The synchronous
    /// action may only move a prebuilt packet into a pre-reserved slot: no
    /// serialization, reservation, cache lock, I/O, await or spawn inside.
    ///
    /// Service errors require explicit outcome policy; they are not public
    /// transport failures. Preserve actual provider errors/quota independently
    /// of private-payload eligibility. An error returned here must be safe to
    /// map to the existing local error policy without disclosing the withheld
    /// payload. Never retry an action that already transferred its packet.
    fn deliver<'a>(
        &'a self,
        kind: RepositoryReadReplyKind,
        transfer: &'a mut (dyn FnMut() -> Result<()> + Send),
    ) -> BoxFuture<'a, Result<()>>;
}

#[cfg(test)]
mod tests;
