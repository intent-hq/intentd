//! intent-services — the shared business-logic surface (§3.1).
//!
//! Depends on core, store, git, sourcecontrol, acp, context, providers, pty,
//! and search (§3.2). Sibling feature modules never import each other; they
//! communicate through the store and the event bus (§3.2 rule 4). Stub only.

pub use intent_core::{Error, Result, WorkspaceApi};

/// Aggregate service handle wired by the binary composition root. It implements
/// `WorkspaceApi` so it can be handed to `intent-acp` as `Arc<dyn WorkspaceApi>`
/// (§6.8).
#[derive(Default)]
pub struct Services;

impl WorkspaceApi for Services {}

// Core domain service modules (§3.1).
pub mod notes {}
pub mod tasks {}
pub mod comments {}
pub mod workspace {}
pub mod agent {}
pub mod git {}
pub mod pr {}
pub mod script {}
pub mod file {}
pub mod event {}
pub mod drafts {} // §9.10

// Agent-Ecosystem modules (§18).
pub mod rules {}
pub mod specialists {}
pub mod mcp_servers {}
pub mod memories {}

// Code Changes Review modules (§17).
pub mod file_tracking {}
pub mod diffs {}
pub mod accept_changes {}
pub mod metrics {}

// Integrations & Ops modules (§19).
pub mod token_usage {}
pub mod session_stats {}
pub mod setup_scripts {}
