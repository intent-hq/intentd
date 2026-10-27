//! The runtime feature for intentd: the `script.*`, `hook.*`, `terminal.*`,
//! `browser.*` and `sandbox.*` RPCs, the setup-script terminal, and the
//! script/hook registries they share.
//!
//! The feature sits above `intent-services`: it builds against
//! [`intent_services::feature_api`], and core reaches it only through the
//! `RuntimePort` it implements. The composition root registers it with
//! [`register`]; a `Services` without it fails every runtime RPC with the
//! `WorkspaceApi` default error.
//!
//! The former `impl Services` blocks are crate-private extension traits
//! (`RuntimeStateExt`, `HookServicesExt`, `ScriptMonitorExt`, ...), so their
//! call sites read as before. [`RuntimeServicesExt`] is the part the daemon
//! calls directly.

mod browser_tabs;
mod hook_manager;
mod host_execution;
mod runtime;
mod sandbox_ops;
mod script_monitor;
mod script_ops;
mod script_output_cleanup;
mod script_output_store;
mod setup_runner;
mod terminal_ops;

/// Test-only composition: every runtime-crate test that builds `Services`
/// registers this crate's runtime explicitly through [`test_runtime::with_runtime`]
/// (core's `Services::new` no longer does).
#[cfg(test)]
pub(crate) mod test_runtime {
    use intent_services::Services;

    /// Register the runtime feature on a freshly built `Services`, before
    /// any clone is taken (registration is copy-on-write).
    pub(crate) fn with_runtime(services: Services) -> Services {
        crate::register(services)
    }

    use intent_core::WorkspaceId;
    #[cfg(unix)]
    use intent_services::feature_api::test_support::setup_lifecycle;
    use intent_services::feature_api::test_support::{
        capability_fixtures, fixtures::TempDb, goldens, retire_workspace,
    };
    #[cfg(unix)]
    use intent_services::feature_api::EventBus;

    /// Core's [`goldens::setup`] with the runtime registered.
    pub(crate) async fn goldens_setup() -> (goldens::TempDb, Services, WorkspaceId) {
        let (tmp, services, ws) = goldens::setup().await;
        (tmp, with_runtime(services), ws)
    }

    /// Core's [`capability_fixtures::fixture`] with the runtime registered.
    pub(crate) async fn capability_fixture(tmp: &TempDb) -> capability_fixtures::Fixture {
        let mut fixture = capability_fixtures::fixture(tmp).await;
        fixture.services = with_runtime(fixture.services.clone());
        fixture
    }

    /// Core's [`retire_workspace::fixture`] with the runtime registered.
    pub(crate) async fn retire_fixture() -> retire_workspace::Fixture {
        let mut fixture = retire_workspace::fixture().await;
        fixture.svc = with_runtime(fixture.svc.clone());
        fixture
    }

    /// Core's [`setup_lifecycle::services`] with the runtime registered.
    #[cfg(unix)]
    pub(crate) async fn setup_lifecycle_services(
        root: std::path::PathBuf,
    ) -> (Services, EventBus, TempDb) {
        let (services, bus, tmp) = setup_lifecycle::services(root).await;
        (with_runtime(services), bus, tmp)
    }
}

pub use runtime::{register, RuntimeServicesExt};

#[cfg(test)]
mod registration_tests {
    use intent_services::feature_api::test_support::fixtures::TempDb;
    use intent_services::Services;

    use crate::runtime::{RuntimeFeature, RuntimeStateExt};
    use crate::test_runtime::with_runtime;

    /// Core's `Services::new` leaves the runtime port unregistered;
    /// registering this crate's runtime fills it with one feature that every
    /// later clone shares.
    #[tokio::test]
    async fn register_installs_one_runtime_feature_shared_by_clones() {
        let tmp = TempDb::new();
        let bare = Services::new(intent_store::Store::open(&tmp.path).await.unwrap());
        assert!(bare.unregistered_feature_ports().contains(&"runtime"));
        let svc = with_runtime(bare);
        assert!(!svc.unregistered_feature_ports().contains(&"runtime"));
        let clone = svc.clone();
        assert!(std::ptr::eq(svc.runtime(), clone.runtime()));
        assert!(std::ptr::eq(
            svc.runtime(),
            svc.feature::<RuntimeFeature>().expect("registered")
        ));
    }
}
