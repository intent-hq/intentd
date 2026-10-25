//! Composition root for intentd.
//!
//! `intent-services` (core) owns the [`Services`] state and the feature ports;
//! the feature crates (`intent-svc-*`) implement those ports above it. This
//! crate is the one place that wires them together: [`compose`] registers
//! every feature on a core [`Services`], and [`StartupExt`] carries the
//! feature-owned entry points the daemon calls once at boot.
//!
//! intentd builds its `Services` through [`compose`] and reaches the feature
//! crates only through this crate. Tests that need several features at once
//! (or a feature plus `intent-transport`) live in this crate's `tests/`, so
//! core and `intent-transport` never depend on a feature crate, not even as
//! a dev-dependency.

use std::future::Future;

use intent_services::Services;

/// Workspace transfer (plan, export, import), re-exported for the daemon and
/// its integration tests.
pub use intent_svc_transfer as transfer;

/// Register every feature crate on `services`.
///
/// Call this on the freshly built core services, before the daemon hands
/// them out: an unregistered feature port answers every call with its
/// "unavailable" error.
#[must_use]
pub fn compose(services: Services) -> Services {
    intent_svc_transfer::register(services)
}

/// Feature-owned entry points the daemon calls on [`Services`] at startup.
pub trait StartupExt {
    /// Delete export staging directories left behind by a previous daemon
    /// run. Called once at boot.
    fn sweep_stale_export_staging(&self) -> impl Future<Output = ()> + Send;
}

impl StartupExt for Services {
    fn sweep_stale_export_staging(&self) -> impl Future<Output = ()> + Send {
        intent_svc_transfer::TransferServicesExt::sweep_stale_export_staging(self)
    }
}
