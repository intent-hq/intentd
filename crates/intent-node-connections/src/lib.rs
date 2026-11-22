//! The node-connection service: everything that holds a link to a machine.
//!
//! Every intentd uses this one component, so desktop and orchestrator mode
//! keep feature parity. The desktop links it in-process; orchestrator mode
//! will reach the same traits over RPC. The crate sits below
//! `intent-services` and may never depend on it
//! (`scripts/check-feature-crate-deps.py`): the head's domain reaches the
//! service through its traits, and the service calls back through traits.
//!
//! [`LinkOwnership`] decides which process owns a machine's link and fences
//! every durable link write. On the desktop one process always owns every
//! link ([`LocalOwnership`]), but the claim and fence calls still run, so
//! one code path serves both deployments.

mod inbound;
pub mod ingest;
pub mod lease;
mod ownership;

pub use inbound::{LinkView, NodeInbound};
pub use ownership::{process_head_session, Fenced, LinkClaim, LinkOwnership, LocalOwnership};
