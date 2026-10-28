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

/// Remote agents on a configured execution node, re-exported for the daemon
/// and its integration tests.
pub use intent_svc_remote as remote;

/// Runtime (scripts, hooks, terminals, browser tabs, sandboxes) entry points
/// the daemon calls on [`Services`] at startup.
pub use intent_services::feature_api::RuntimeServicesExt;

/// Register every feature crate on `services`, then run the startup check.
///
/// Call this on the freshly built core services, before the daemon hands
/// them out.
///
/// # Panics
///
/// If a feature port declared in core is still unregistered (see
/// [`assert_feature_ports_registered`]): the daemon fails at boot instead of
/// answering that feature's calls with its "unavailable" error.
#[must_use]
pub fn compose(services: Services) -> Services {
    let services = intent_svc_transfer::register(services);
    let services = intent_svc_remote::register(services);
    assert_feature_ports_registered(&services);
    services
}

/// The startup check [`compose`] runs: every feature port core declares has
/// a registered implementation.
///
/// # Panics
///
/// Naming the unregistered ports, if any.
pub fn assert_feature_ports_registered(services: &Services) {
    let missing = services.unregistered_feature_ports();
    assert!(
        missing.is_empty(),
        "intent_app::compose: feature ports not registered: {}",
        missing.join(", ")
    );
}

/// Feature-owned entry points the daemon calls on [`Services`] at startup.
pub trait StartupExt {
    /// Delete export staging directories left behind by a previous daemon
    /// run. Called once at boot.
    fn sweep_stale_export_staging(&self) -> impl Future<Output = ()> + Send;

    /// Start the connector to the execution node configured in `data_dir`
    /// (`remote-node.json`). Returns `false` when no node is configured.
    fn start_remote_node(
        &self,
        data_dir: &std::path::Path,
    ) -> impl Future<Output = intent_services::Result<bool>> + Send;
}

impl StartupExt for Services {
    fn sweep_stale_export_staging(&self) -> impl Future<Output = ()> + Send {
        intent_svc_transfer::TransferServicesExt::sweep_stale_export_staging(self)
    }

    fn start_remote_node(
        &self,
        data_dir: &std::path::Path,
    ) -> impl Future<Output = intent_services::Result<bool>> + Send {
        intent_svc_remote::RemoteServicesExt::start_remote_node(self, data_dir)
    }
}
