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

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use intent_core::{AgentId, AgentReverseDispatch};
use intent_sourcecontrol::SourceControl;

use crate::delivery_tasks::DeliveryTasks;
use crate::feature_ports::{AgentWakePort, RemoteLaunchPort, RuntimePort, TransferPort};
#[cfg(any(test, feature = "test-hooks"))]
use crate::model_catalog::ModelCatalogCache;
use crate::runtime_config::{ScriptOutputConfig, ScriptParks};
use crate::WorkspaceSetupStates;

pub use crate::runtime::RuntimeServicesExt;
pub use crate::{
    daemon_exe_path, default_workspaces_root, is_stale_in_flight_status, known_repo_name,
    publish_event, publish_event_transient, record_setup_running, record_setup_skipped,
    system_actor, workspace_created_event, workspace_setup_completed_event, worktree_folder_slug,
    write_private_script, ArchiveFence, ArchiveFenceGuard, CompletionClassifyPark, EventBus,
    Services, SettingsRegistry, SubscriptionFilter, SETUP_SCRIPT_WRAPPER, SETUP_TERMINAL_NAME,
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
    pub use crate::feature_ports::{
        monitor_id, wake_id, AgentWakePort, CancelSettlement, CommandScriptActivity, MergeOutcome,
        ProvisionOutcome, RemoteLaunchPort, RemoteRuntime, RuntimePort, ScriptsClosing,
        SetupScriptRun, TransferPort, UnavailableAgentWake, UnavailableRemoteLaunch,
        IMPORT_MAX_CHUNK_BYTES,
    };
}

/// The desktop-client presence and browser assignment runtime.
pub mod desktop {
    pub use crate::desktop::Runtime;
}

/// Script output configuration and test parks that core owns.
pub mod runtime_config {
    pub use crate::runtime_config::{ScriptOutputConfig, ScriptParks, SupervisePark};

    #[cfg(any(test, feature = "test-hooks"))]
    pub use crate::runtime_config::HookStoreFault;
}

/// The async secret store.
pub mod settings {
    pub use crate::settings::AsyncSecretStore;
}

/// Caller classification for event-producing operations.
pub mod event_ops {
    pub use crate::event_ops::is_collaborator_caller;
}

/// The task-local execution scope and its credential policy.
pub mod execution_call {
    pub use crate::execution_call::{expose_git_credential, github_execution_host};
}

/// Workspace file locations.
pub mod file_ops {
    pub use crate::file_ops::{resolve_attachment_source, workspace_root};
}

/// Workspace git locations.
pub mod git_ops {
    pub use crate::git_ops::worktree_path;
}

/// The hook wake and notice framing of the latest harness.
pub mod harness {
    pub use crate::harness::{latest_hook_framing, HookFraming};

    /// Harness v1 constants.
    pub mod v1 {
        pub use crate::harness::v1::HOOK_WAKE_LOGS_CAP;
    }
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

/// Cursor pagination over lists and text.
pub mod pagination {
    pub use crate::pagination::{page_window, paginate_text_lines};
}

/// The refusal for operations on a retired workspace.
pub mod retire {
    pub use crate::retire::retired_error;
}

/// Shell selection and environment scrubbing.
pub mod shell {
    pub use crate::shell::{
        default_shell, scrubbed_env_vars_except, shell_args, shell_true_invocation,
    };
}

/// Source-control credential identity.
pub mod source_control_auth_ops {
    /// Repository-owned credential settings.
    pub mod repository_owner {
        pub use crate::source_control_auth_ops::repository_owner::logical_instance;
    }
}

/// Per-workspace mutation admission.
pub mod workspace_mutations {
    pub use crate::workspace_mutations::{boxed, Mutation, WorkspaceMutations};
}

/// Nested repository discovery and staging.
pub mod nested_repos {
    pub use crate::nested_repos::{is_dirty, nested_repo_dirs, stage_all_skipping_nested};
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
    pub fn feature_store_tasks(&self) -> &Arc<DeliveryTasks> {
        &self.store_tasks
    }

    /// The lane that serializes script-monitor delivery with exports.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_script_monitor_lane(&self) -> &tokio::sync::Mutex<()> {
        self.ports.runtime().script_monitor_lane(self)
    }

    /// Per-workspace setup state.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_workspace_setup_states(&self) -> &WorkspaceSetupStates {
        &self.workspace_setup_states
    }

    /// Background tasks spawned by delivery paths; drained on shutdown.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_delivery_tasks(&self) -> &DeliveryTasks {
        &self.delivery_tasks
    }

    /// Background tasks owned by settings watchers.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_settings_tasks(&self) -> &DeliveryTasks {
        &self.settings_tasks
    }

    /// Background tasks owned by terminal sessions.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_terminal_tasks(&self) -> &Arc<DeliveryTasks> {
        &self.terminal_tasks
    }

    /// The desktop-client presence and browser assignment runtime.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_desktop(&self) -> &crate::desktop::Runtime {
        &self.desktop
    }

    /// The reverse dispatcher to connected clients, when one is attached.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_reverse_dispatch(&self) -> Option<&Arc<dyn AgentReverseDispatch>> {
        self.reverse_dispatch.as_ref()
    }

    /// The lane that serializes browser-tab mutations.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_browser_tab_gate(&self) -> &tokio::sync::Mutex<()> {
        &self.browser_tab_gate
    }

    /// The wired settings registry, if any, borrowed.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_settings_registry(&self) -> Option<&SettingsRegistry> {
        self.settings_registry.as_deref()
    }

    /// The secret store.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_secrets(&self) -> &crate::settings::AsyncSecretStore {
        &self.secrets
    }

    /// The configured source-control provider, if any.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_source_control(&self) -> Option<&Arc<dyn SourceControl>> {
        self.source_control.as_ref()
    }

    /// The in-memory per-agent message queues.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_agent_queues(
        &self,
    ) -> &std::sync::Mutex<HashMap<AgentId, Vec<crate::agent_ops::QueuedMessage>>> {
        &self.agent_queues
    }

    /// The per-workspace archive fence.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_archive_fence(&self) -> &Arc<crate::ArchiveFence> {
        &self.archive_fence
    }

    /// The minimum run time below which a script exit counts as too fast.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_script_too_fast_ms(&self) -> u128 {
        self.script_too_fast_ms
    }

    /// Script lifecycle test parks (all unset in production).
    #[doc(hidden)]
    #[must_use]
    pub fn feature_script_parks(&self) -> &ScriptParks {
        &self.script_parks
    }

    /// Where script output is kept and how much each run may write.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_script_output_config(&self) -> &ScriptOutputConfig {
        &self.script_output_config
    }

    /// The hook-evaluation test park (unset in production).
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    #[must_use]
    pub fn feature_hook_eval_park(&self) -> Option<&Arc<crate::CompletionClassifyPark>> {
        self.hook_eval_park.as_ref()
    }

    /// The hook-wake test park (unset in production).
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    #[must_use]
    pub fn feature_hook_wake_park(&self) -> Option<&Arc<crate::CompletionClassifyPark>> {
        self.hook_wake_park.as_ref()
    }

    /// The registered transfer feature.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_transfer_port(&self) -> &dyn TransferPort {
        self.ports.transfer()
    }

    /// The registered remote-launch feature.
    #[must_use]
    pub fn feature_remote_launch_port(&self) -> &dyn RemoteLaunchPort {
        self.ports.remote_launch()
    }

    /// Test seam: the model catalog cache, for seeding entries.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    #[must_use]
    pub fn models_catalog_for_test(&self) -> &Arc<ModelCatalogCache> {
        &self.models_catalog
    }

    /// The registered runtime feature.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_runtime(&self) -> &dyn RuntimePort {
        self.ports.runtime()
    }

    /// The registered agent-wake feature.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_agent_wake(&self) -> &dyn AgentWakePort {
        self.ports.agent_wake()
    }

    /// A shared handle on the agent-wake port, for holders that outlive the
    /// borrow of `Services`.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_agent_wake_port(&self) -> Arc<dyn AgentWakePort> {
        self.ports.agent_wake_port()
    }

    /// The per-workspace mutation admission gates.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_workspace_mutations(&self) -> &crate::workspace_mutations::WorkspaceMutations {
        &self.workspace_mutations
    }

    /// The per-agent cap on active hooks.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_hooks_max_per_agent(&self) -> u32 {
        self.hooks_max_per_agent
    }

    /// The hook evaluation timeout.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_hook_eval_timeout(&self) -> std::time::Duration {
        self.hook_eval_timeout
    }

    /// The base backoff for hook-run persistence retries.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_hook_store_retry_base(&self) -> std::time::Duration {
        self.hook_store_retry_base
    }

    /// Notified when cached execution contexts must be recomputed.
    #[doc(hidden)]
    #[must_use]
    pub fn feature_execution_invalidation(&self) -> &tokio::sync::Notify {
        &self.execution_invalidation
    }

    /// The hook-run persistence fault injector (test wiring only).
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    #[must_use]
    pub fn feature_hook_store_fault(&self) -> Option<&crate::runtime_config::HookStoreFault> {
        self.hook_store_fault.as_ref()
    }

    /// Test seam: park hook wakes inside their classify window.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn set_hook_wake_park_for_test(&mut self, park: Arc<crate::CompletionClassifyPark>) {
        self.hook_wake_park = Some(park);
    }

    /// Test seam: park hook evaluations inside their window.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn set_hook_eval_park_for_test(&mut self, park: Arc<crate::CompletionClassifyPark>) {
        self.hook_eval_park = Some(park);
    }

    /// Test seam: the script lifecycle parks, for arming one.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn script_parks_mut_for_test(&mut self) -> &mut ScriptParks {
        &mut self.script_parks
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
