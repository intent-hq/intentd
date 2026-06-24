//! intent-context — context-engine integration (§3.1, §8).
//!
//! Depends on `intent-core` only (§3.2); it implements the `ContextEngine`
//! trait defined in core with an `auggie`-backed engine ([`AuggieContextEngine`])
//! plus binary [`discovery`]. Construction never fails the daemon and an absent
//! engine surfaces as a first-class `Unavailable` state, never an error (§8.3).

pub mod auggie;
pub mod discovery;

pub use auggie::AuggieContextEngine;
pub use intent_core::{
    ContextEngine, ContextError, EngineAvailability, RetrieveRequest, RetrieveResult, RetrievedItem,
};
