//! intent-providers — provider registry + launch arg/env assembly (§3.1, §6.9).
//!
//! Depends on `intent-core` only (§3.2). Stub only.

pub use intent_core::Result;

pub mod registry {
    //! `ProviderConfig` registry — provider quirks are data, not code (§6.9).

    /// Table of known providers. Entries are added as features land.
    #[derive(Debug, Default)]
    pub struct ProviderRegistry;
}

pub mod args {
    //! Launch argument / environment builder — stub.
}

pub mod models {
    //! Model-tier table — stub.
}

pub mod capabilities {
    //! Capability / quirks descriptors — stub.
}

pub use registry::ProviderRegistry;
