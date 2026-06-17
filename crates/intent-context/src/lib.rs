//! intent-context — context-engine integration (§3.1).
//!
//! Depends on `intent-core` only (§3.2); it implements the `ContextEngine`
//! trait defined in core. Stub only.

pub use intent_core::{ContextEngine, Result};

pub mod auggie {
    //! `AuggieContextEngine` — the `auggie`-backed context engine — stub.

    /// Context engine backed by the `auggie` CLI.
    #[derive(Debug, Default)]
    pub struct AuggieContextEngine;

    impl intent_core::ContextEngine for AuggieContextEngine {}
}

pub mod discovery {
    //! Context-engine discovery / availability probing — stub.
}

pub use auggie::AuggieContextEngine;
