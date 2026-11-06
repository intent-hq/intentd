//! Workspace transfer for intentd: `workspace.transferPlan`,
//! `workspace.export.*` and `workspace.import.*` (PROTOCOL §5.1).
//!
//! The feature sits above `intent-services`: it builds against
//! [`intent_services::feature_api`], and core reaches it only through the
//! `TransferPort` it implements. The composition root registers it with
//! [`register`]; a `Services` without it fails every transfer RPC with the
//! `WorkspaceApi` default error.
//!
//! The former `impl Services` blocks are crate-private extension traits
//! (`TransferPlanExt`, `TransferExportExt`, `TransferImportExt`, ...), so
//! their call sites read as before. [`TransferServicesExt`] is the part the
//! daemon calls directly.

use std::future::Future;
use std::sync::Arc;

use intent_services::feature_api::Services;

mod transfer;
mod transfer_authorship;
mod transfer_export;
pub mod transfer_git;
mod transfer_import;
pub(crate) mod transfer_materialize;
mod transfer_model_selection;
mod transfer_remotes;
#[cfg(test)]
mod transfer_roundtrip;
#[cfg(all(test, unix))]
mod transfer_selection_contract;
mod transfer_submodules;

#[cfg(test)]
mod authorization_tests;
#[cfg(all(test, unix))]
mod fixture_cleanup_tests;

/// Register the transfer feature on `services`. Clones made from the result
/// share one feature instance, so chunks arriving over any connection reach
/// the same session.
#[must_use]
pub fn register(services: Services) -> Services {
    let feature = Arc::new(transfer::TransferFeature::default());
    services
        .with_feature(Arc::clone(&feature))
        .with_transfer_port(feature)
}

/// Transfer entry points the daemon calls on [`Services`] directly.
pub trait TransferServicesExt {
    /// Delete export staging directories left behind by a previous daemon
    /// run. Called once at boot.
    fn sweep_stale_export_staging(&self) -> impl Future<Output = ()> + Send;
}

impl TransferServicesExt for Services {
    fn sweep_stale_export_staging(&self) -> impl Future<Output = ()> + Send {
        transfer_export::TransferExportExt::sweep_stale_export_staging(self)
    }
}
