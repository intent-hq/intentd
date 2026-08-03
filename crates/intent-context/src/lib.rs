//! intent-context — context-engine integration (§3.1, §8).
//!
//! Depends on `intent-core` only (§3.2); it implements the `ContextEngine`
//! trait defined in core with an `auggie`-backed engine ([`AuggieContextEngine`])
//! plus binary [`discovery`]. Construction never fails the daemon and an absent
//! engine surfaces as a first-class `Unavailable` state, never an error (§8.3).

/// Test-only process-global env setup. Runs before `main()` — and therefore
/// before any test threads exist, making `set_var` race-free. Node children
/// spawned by lib tests (e.g. the real `auggie` CLI in availability probes)
/// inherit this and skip `module.enableCompileCache()`, which would otherwise
/// leave a `node-compile-cache/` residue at the TMPDIR root after the suite.
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn disable_node_compile_cache() {
    std::env::set_var("NODE_DISABLE_COMPILE_CACHE", "1");
}

pub mod auggie;
pub mod discovery;

pub use auggie::AuggieContextEngine;
pub use intent_core::{
    ContextEngine, ContextError, EngineAvailability, RetrieveRequest, RetrieveResult, RetrievedItem,
};
