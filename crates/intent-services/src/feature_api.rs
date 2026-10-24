//! The core surface that feature crates build against.
//!
//! A feature domain that lives in its own crate (`intent-svc-*`) reaches the
//! core items it needs through this module instead of `crate::` paths. The
//! submodules mirror the core module names, so code moving out of this crate
//! rewrites `crate::<module>::<item>` to
//! `intent_services::feature_api::<module>::<item>` and nothing else.
//!
//! The module is `#[doc(hidden)]`: it is an internal contract between the
//! intentd crates, not a stable API. It lists what the transfer feature uses
//! today and is narrowed again once the feature crates settle.
//!
//! Items exported from here must be `pub` at their definition. A `pub use`
//! of a `pub(crate)` item fails to compile, so this list also keeps their
//! visibility from regressing.
//!
//! The items behind `cfg(any(test, feature = "test-hooks"))` are test seams
//! and fixtures for feature-crate tests. Only dev-dependencies may enable
//! `test-hooks`; scripts/check-test-hooks-feature.sh keeps it out of the
//! production build.

use std::path::Path;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::Arc;

use crate::delivery_tasks::DeliveryTasks;
use crate::feature_ports::TransferPort;
#[cfg(any(test, feature = "test-hooks"))]
use crate::model_catalog::ModelCatalogCache;
use crate::WorkspaceSetupStates;

pub use crate::{
    default_workspaces_root, is_stale_in_flight_status, known_repo_name, publish_event,
    record_setup_skipped, system_actor, workspace_created_event, workspace_setup_completed_event,
    worktree_folder_slug, EventBus, Services, SettingsRegistry, SubscriptionFilter,
};

#[cfg(any(test, feature = "test-hooks"))]
pub use crate::test_support;

/// Agent turn and recovery helpers.
pub mod agent_ops {
    pub use crate::agent_ops::{
        ensure_effort_supported_by_model, is_qualified_monitor_envelope,
        resolve_agent_default_model_with_source, resolve_settings_default_reasoning_effort,
        DefaultModelSource, QueuedMessage, RecoverySource,
    };
}

/// The agent manager and its teardown fence.
pub mod agent_manager {
    pub use crate::agent_manager::{AgentManager, TeardownFence};

    #[cfg(any(test, feature = "test-hooks"))]
    pub use crate::agent_manager::imported_spawn_selection_for_test;

    /// Test fixtures the agent manager tests share.
    #[cfg(any(test, feature = "test-hooks"))]
    pub mod tests {
        pub use crate::test_support::fixtures::EnvGuard;
    }
}

/// Provider resolution for agent sessions.
pub mod agent_session {
    pub use crate::agent_session::{derived_default_provider, resolve_provider_id};
}

/// Caller gating.
pub mod capability {
    pub use crate::capability::gated_collaborator_caller;
}

/// Background task tracking that drains on shutdown.
pub mod delivery_tasks {
    pub use crate::delivery_tasks::DeliveryTasks;
}

/// The feature ports core calls features through.
pub mod feature_ports {
    pub use crate::feature_ports::{TransferPort, IMPORT_MAX_CHUNK_BYTES};
}

/// Workspace file locations.
pub mod file_ops {
    pub use crate::file_ops::{resolve_attachment_source, workspace_root};
}

/// Workspace git locations.
pub mod git_ops {
    pub use crate::git_ops::worktree_path;
}

/// The cached model catalog.
pub mod model_catalog {
    pub use crate::model_catalog::ModelCatalogReader;

    #[cfg(any(test, feature = "test-hooks"))]
    pub use crate::model_catalog::{
        resolve_with_cache, source_for, ModelCatalogCache, ModelFetchResult,
    };
}

/// Provider auth test seams.
#[cfg(any(test, feature = "test-hooks"))]
pub mod provider_auth {
    pub use crate::provider_auth::seed_auth_verdict_for_tests;
}

/// Legacy message-metadata values for attribution tests.
#[cfg(any(test, feature = "test-hooks"))]
pub mod human_attribution_tests {
    pub use crate::test_support::fixtures::legacy_metadata_values;
}

/// The crate-wide test fixtures (`crate::tests` in this crate).
#[cfg(any(test, feature = "test-hooks"))]
pub mod tests {
    pub use crate::test_support::fixtures::{workspace, TempDb};
}

/// Nested repository discovery and staging.
pub mod nested_repos {
    pub use crate::nested_repos::{is_dirty, nested_repo_dirs, stage_all_skipping_nested};
}

/// The script manager.
pub mod script_ops {
    pub use crate::script_ops::ScriptManager;
}

/// Private `Services` state a feature reads, exposed as accessors so the
/// fields themselves stay private.
impl Services {
    /// The asset root, when one is configured.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_assets_root(&self) -> Option<&Path> {
        self.assets_root.as_deref()
    }

    /// The configured workspaces root. `None` means
    /// [`default_workspaces_root`].
    #[doc(hidden)]
    #[must_use]
    pub fn feature_workspaces_root(&self) -> Option<&Path> {
        self.workspaces_root.as_deref()
    }

    /// The event bus, when one is attached.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_event_bus(&self) -> Option<&EventBus> {
        self.event_bus.as_ref()
    }

    /// Background tasks that drain on shutdown.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_store_tasks(&self) -> &DeliveryTasks {
        &self.store_tasks
    }

    /// The lane that serializes script-monitor delivery with exports.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_script_monitor_lane(&self) -> &tokio::sync::Mutex<()> {
        &self.script_locks.monitor_lane
    }

    /// Per-workspace setup state.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_workspace_setup_states(&self) -> &WorkspaceSetupStates {
        &self.workspace_setup_states
    }

    /// The registered transfer feature.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_transfer_port(&self) -> &dyn TransferPort {
        self.ports.transfer()
    }

    /// Test seam: the model catalog cache, for seeding entries.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    #[must_use]
    pub fn models_catalog_for_test(&self) -> &Arc<ModelCatalogCache> {
        &self.models_catalog
    }

    /// Test seam: replace the model catalog cache.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn set_models_catalog_for_test(&mut self, cache: Arc<ModelCatalogCache>) {
        self.models_catalog = cache;
    }
}

#[cfg(test)]
mod unit_tests {
    use intent_core::{PrincipalId, WorkspaceId};
    use intent_store::Store;

    use crate::test_support::fixtures::TempDb;
    use crate::test_support::test_tempdir;
    use crate::{EventBus, Services};

    async fn services(tmp: &TempDb) -> Services {
        Services::new(Store::open(&tmp.path).await.expect("open store"))
    }

    #[tokio::test]
    async fn accessors_read_the_configured_state() {
        let tmp = TempDb::new();
        let bare = services(&tmp).await;
        assert!(bare.feature_assets_root().is_none());
        assert!(bare.feature_workspaces_root().is_none());
        assert!(bare.feature_event_bus().is_none());
        assert!(!bare.feature_store_tasks().is_closed());
        assert!(bare.feature_script_monitor_lane().try_lock().is_ok());

        let roots = test_tempdir("feature-api-");
        let store = Store::open(&tmp.path).await.expect("open store");
        let configured = Services::new(store.clone())
            .with_assets_root(roots.path().join("assets"))
            .with_workspaces_root(roots.path().join("workspaces"))
            .with_event_bus(EventBus::new(store));
        assert_eq!(
            configured.feature_assets_root(),
            Some(roots.path().join("assets").as_path())
        );
        assert_eq!(
            configured.feature_workspaces_root(),
            Some(roots.path().join("workspaces").as_path())
        );
        assert!(configured.feature_event_bus().is_some());
    }

    #[tokio::test]
    async fn transfer_port_test_seams_track_parked_exports() {
        let tmp = TempDb::new();
        let svc = services(&tmp).await;
        let port = svc.feature_transfer_port();
        let ws = WorkspaceId::new();
        assert_eq!(port.export_count_for_test(), 0);
        assert!(!port.export_in_progress(ws.as_str()));

        port.park_building_export_for_test(
            "parked",
            &ws,
            Some(PrincipalId::new()),
            std::env::temp_dir(),
        );
        assert_eq!(port.export_count_for_test(), 1);
        assert!(port.export_in_progress(ws.as_str()));
        assert!(!port.export_in_progress(WorkspaceId::new().as_str()));

        port.remove_export_for_test("parked");
        port.remove_export_for_test("parked");
        assert_eq!(port.export_count_for_test(), 0);
        assert!(!port.export_in_progress(ws.as_str()));
    }
}
