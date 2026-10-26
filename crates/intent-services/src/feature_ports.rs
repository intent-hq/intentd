//! Feature ports: the seams through which core code (`WorkspaceApi`
//! forwarders, shared helpers) reaches a feature domain without naming its
//! types.
//!
//! A port is a `Send + Sync` trait defined here and implemented by the
//! feature. Its methods take `&Services` as an explicit context argument, so
//! the feature object never holds a `Services` handle (no `Arc` cycle).
//! Feature state (for example in-flight transfer sessions) lives in the
//! feature's own object, which the feature reads back through the typed
//! [`FeatureSet`] ([`Services::feature`]), not in `Services` fields.
//!
//! The ports are declared once, in the `feature_port_registry!` call below.
//! [`Services::new`] starts each with its "unavailable" default (for
//! transfer, [`UnavailableTransfer`], which fails every RPC with the same
//! error as the `WorkspaceApi` default). The composition root
//! (`intent_app::compose`) registers the real implementations
//! ([`Services::with_transfer_port`]) and then runs the startup check
//! [`Services::ensure_feature_ports_registered`], so a missing registration
//! fails at boot rather than at the first call. `port_forwarders!` writes
//! the one-line `WorkspaceApi` forwarders.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::Mutex;

use intent_core::transfer::TransferPlan;

use intent_core::execution::HostExecutionContext;
use intent_core::{
    AgentId, AgentStatus, BoxFuture, BrowserTab, BrowserTabInput, ClientId, Error, HookId, Result,
    ScriptCreateParams, WorkspaceApi, WorkspaceId,
};

use crate::Services;

/// Maximum DECODED bytes per transfer chunk (`workspace.import.chunk`, and
/// `workspace.export.read`). Base64 inflates this by 4/3 on the wire
/// (~21.4 MiB), keeping the full JSON-RPC frame comfortably under the
/// 40 MiB inbound cap (PROTOCOL §1.3). Core's file reads share the cap.
pub const IMPORT_MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// The workspace transfer feature (`workspace.transferPlan`,
/// `workspace.import.*`, `workspace.export.*`). Each method performs the
/// full RPC, authorization included, exactly as the `WorkspaceApi` method it
/// backs.
pub trait TransferPort: Send + Sync + 'static {
    fn transfer_plan<'a>(
        &'a self,
        svc: &'a Services,
        id: WorkspaceId,
    ) -> BoxFuture<'a, Result<TransferPlan>>;

    fn import_begin<'a>(
        &'a self,
        svc: &'a Services,
        manifest: serde_json::Value,
        archive_size_bytes: u64,
        archive_sha256: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn import_chunk<'a>(
        &'a self,
        svc: &'a Services,
        import_id: String,
        seq: u64,
        data: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn import_commit<'a>(
        &'a self,
        svc: &'a Services,
        import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn import_abort<'a>(
        &'a self,
        svc: &'a Services,
        import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_start<'a>(
        &'a self,
        svc: &'a Services,
        id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_read<'a>(
        &'a self,
        svc: &'a Services,
        export_id: String,
        seq: u64,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_finalize<'a>(
        &'a self,
        svc: &'a Services,
        export_id: String,
        archive_source: bool,
        final_status_message: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn export_abort<'a>(
        &'a self,
        svc: &'a Services,
        export_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// True while an export of `workspace_id` is in flight (between
    /// `workspace.export.start` and `finalize`/`abort`/a failed build).
    fn export_in_progress(&self, workspace_id: &str) -> bool;

    /// Test seam: register an export of `workspace_id` whose archive is
    /// still building, owned by `initiator`, under `export_id`. Core tests
    /// use it to hold an export open without running a build.
    ///
    /// The test seams have panicking defaults: whether they exist depends on
    /// this crate's `test-hooks` feature, which an implementing crate cannot
    /// observe, so it implements them only under its own `cfg(test)`.
    #[cfg(any(test, feature = "test-hooks"))]
    fn park_building_export_for_test(
        &self,
        export_id: &str,
        workspace_id: &WorkspaceId,
        initiator: Option<intent_core::PrincipalId>,
        staging_dir: std::path::PathBuf,
    ) {
        let _ = (export_id, workspace_id, initiator, staging_dir);
        unimplemented!("this transfer port has no test seams");
    }

    /// Test seam: drop the export registered under `export_id`, if any.
    #[cfg(any(test, feature = "test-hooks"))]
    fn remove_export_for_test(&self, export_id: &str) {
        let _ = export_id;
        unimplemented!("this transfer port has no test seams");
    }

    /// Test seam: how many exports are registered.
    #[cfg(any(test, feature = "test-hooks"))]
    fn export_count_for_test(&self) -> usize {
        unimplemented!("this transfer port has no test seams");
    }
}

/// The transfer port of a `Services` no transfer feature was registered on:
/// every RPC fails with the `WorkspaceApi` default "not implemented" error,
/// and no export is ever in flight.
///
/// Under `test-hooks` it records the exports parked through the test seams,
/// so core tests can hold an export open without the transfer feature.
#[derive(Default)]
pub(crate) struct UnavailableTransfer {
    /// Parked exports: `export_id` to workspace id.
    #[cfg(any(test, feature = "test-hooks"))]
    parked_exports: Mutex<HashMap<String, String>>,
}

fn not_implemented<'a, T: Send + 'a>(method: &str) -> BoxFuture<'a, Result<T>> {
    let error = Error::Internal(format!("WorkspaceApi::{method} not implemented"));
    Box::pin(async move { Err(error) })
}

impl TransferPort for UnavailableTransfer {
    fn transfer_plan<'a>(
        &'a self,
        _svc: &'a Services,
        _id: WorkspaceId,
    ) -> BoxFuture<'a, Result<TransferPlan>> {
        not_implemented("workspace_transfer_plan")
    }

    fn import_begin<'a>(
        &'a self,
        _svc: &'a Services,
        _manifest: serde_json::Value,
        _archive_size_bytes: u64,
        _archive_sha256: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_begin")
    }

    fn import_chunk<'a>(
        &'a self,
        _svc: &'a Services,
        _import_id: String,
        _seq: u64,
        _data: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_chunk")
    }

    fn import_commit<'a>(
        &'a self,
        _svc: &'a Services,
        _import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_commit")
    }

    fn import_abort<'a>(
        &'a self,
        _svc: &'a Services,
        _import_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_import_abort")
    }

    fn export_start<'a>(
        &'a self,
        _svc: &'a Services,
        _id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_start")
    }

    fn export_read<'a>(
        &'a self,
        _svc: &'a Services,
        _export_id: String,
        _seq: u64,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_read")
    }

    fn export_finalize<'a>(
        &'a self,
        _svc: &'a Services,
        _export_id: String,
        _archive_source: bool,
        _final_status_message: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_finalize")
    }

    fn export_abort<'a>(
        &'a self,
        _svc: &'a Services,
        _export_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        not_implemented("workspace_export_abort")
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn export_in_progress(&self, _workspace_id: &str) -> bool {
        false
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn export_in_progress(&self, workspace_id: &str) -> bool {
        self.parked_exports
            .lock()
            .expect("parked exports")
            .values()
            .any(|parked| parked == workspace_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn park_building_export_for_test(
        &self,
        export_id: &str,
        workspace_id: &WorkspaceId,
        _initiator: Option<intent_core::PrincipalId>,
        _staging_dir: std::path::PathBuf,
    ) {
        self.parked_exports
            .lock()
            .expect("parked exports")
            .insert(export_id.to_string(), workspace_id.as_str().to_string());
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn remove_export_for_test(&self, export_id: &str) {
        self.parked_exports
            .lock()
            .expect("parked exports")
            .remove(export_id);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn export_count_for_test(&self) -> usize {
        self.parked_exports.lock().expect("parked exports").len()
    }
}

/// The workspace runtime feature: saved scripts (`script.*`, including
/// script monitors), background hooks (`hook.*`), terminals (`terminal.*`),
/// the browser tab registry (`browser.*`), agent sandboxes (`sandbox.*`)
/// and workspace setup scripts. Each RPC method performs the full RPC,
/// authorization included, exactly as the `WorkspaceApi` method of the same
/// name it backs; the remaining methods are the entry points core code
/// (sandbox merge-back, provisioning, the browser-client pin, workspace
/// creation) reaches the runtime through.
pub trait RuntimePort: Send + Sync + 'static {
    fn script_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_list_filtered<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        archive: intent_core::ScriptArchiveFilter,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_list_view<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_get<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_monitor_list_view<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_monitor_get<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        monitor_id: String,
        owner: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_archive<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_restore<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_create<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_remove<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_monitor<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        script_id: String,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_monitor_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_monitor_cancel<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        monitor_id: String,
        owner: Option<AgentId>,
        stop_run: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_start<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_start_monitor<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        script_id: String,
        run_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_stop<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_restart<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_output<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_status<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    fn script_run<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `hook.schedule`: validate + immediate real run + persist + spawn the
    /// per-hook scheduler task (see `hook_manager`).
    fn hook_schedule<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        params: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `hook.list`: hooks in a workspace, optionally one agent's; active
    /// only by default, retired rows as a light projection on request.
    fn hook_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        include_retired: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `hook.get` (MCP-only): one hook row (including `code`) by id, active
    /// or terminal; hooks from other workspaces read as `NotFound`.
    fn hook_get<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `hook.cancel`: stop an active hook; an agent caller may only cancel
    /// its own hooks, while the FE path (`caller = None`) cancels any hook
    /// and wakes the owning agent with a notice.
    fn hook_cancel<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
        caller: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `hook.runNow`: trigger an immediate run of an active hook.
    fn hook_run_now<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `terminal.create`.
    #[expect(
        clippy::too_many_arguments,
        reason = "the terminal.create RPC shape plus svc"
    )]
    fn terminal_create<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
        env: Option<std::collections::BTreeMap<String, String>>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `terminal.write`.
    fn terminal_write<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
        data: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `terminal.resize`.
    fn terminal_resize<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
        cols: u16,
        rows: u16,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `terminal.kill`.
    fn terminal_kill<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `terminal.getBuffer`.
    fn terminal_get_buffer<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `terminal.list`.
    fn terminal_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `terminal.readOutput`.
    fn terminal_read_output<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        terminal_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `browser.listTabs`.
    fn browser_list_tabs<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<Vec<BrowserTab>>>;

    /// `browser.upsertTab`.
    fn browser_upsert_tab<'a>(
        &'a self,
        svc: &'a Services,
        host: ClientId,
        tab: BrowserTabInput,
    ) -> BoxFuture<'a, Result<BrowserTab>>;

    /// `browser.removeTab`.
    fn browser_remove_tab<'a>(
        &'a self,
        svc: &'a Services,
        host: ClientId,
        tab_id: String,
    ) -> BoxFuture<'a, Result<()>>;

    /// `browser.syncTabs`.
    fn browser_sync_tabs<'a>(
        &'a self,
        svc: &'a Services,
        host: ClientId,
        tabs: Vec<BrowserTabInput>,
    ) -> BoxFuture<'a, Result<Vec<String>>>;

    /// `browser.exec` (agent-initiated CDP forward).
    fn browser_exec<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        actions: Vec<serde_json::Value>,
        tab_id: Option<String>,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `browser.navigateTab`.
    fn browser_navigate_tab<'a>(
        &'a self,
        svc: &'a Services,
        tab_id: String,
        url: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `browser.closeTab`.
    fn browser_close_tab<'a>(
        &'a self,
        svc: &'a Services,
        tab_id: String,
        force: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `sandbox.cow.merge`.
    fn sandbox_merge<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// `sandbox.cow.discard`.
    fn sandbox_discard<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// Re-home every claimed browser tab of `workspace_id` to `new_host`,
    /// the workspace's new driving-client pin (REV-2 Model 10).
    fn migrate_claimed_browser_tabs<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        new_host: &'a ClientId,
    ) -> BoxFuture<'a, Result<()>>;

    /// Provision a `CoW` sandbox for `agent_id` under `workspaces_root`.
    fn provision_sandbox<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
        workspaces_root: PathBuf,
    ) -> BoxFuture<'a, Result<ProvisionOutcome>>;

    /// Attempt the merge-back of `agent_id`'s sandbox into canonical. The
    /// caller owns the sandbox status claim and the bookkeeping.
    fn merge_sandbox<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<MergeOutcome>>;

    /// Remove `agent_id`'s sandbox directory and record.
    fn discard_sandbox<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<()>>;

    /// Run a new workspace's effective setup script in its "Setup"
    /// terminal and wait for it to exit. Resolves to `(ran_script,
    /// exit_code)`: whether the script was spawned, and its observed exit
    /// code. The caller owns the `workspace:setup:*` lifecycle around it.
    fn run_setup_script(
        &self,
        svc: &Services,
        run: SetupScriptRun,
    ) -> BoxFuture<'static, (bool, Option<u32>)>;

    // Hook and script-monitor state agents read and settle (agent views,
    // waiting annotations, retire/delete sweeps, turn admission). Each
    // behaves exactly as the runtime `Services` method of the same name; the
    // async ones box that method's future, adding no await point.

    /// The active hooks `agent_id` owns, as hook rows.
    fn active_hooks_for_agent<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<serde_json::Value>>;

    /// The active hooks of `workspace_id`, keyed by owning agent id.
    fn active_hooks_by_agent<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, HashMap<String, Vec<serde_json::Value>>>;

    /// Add `waitingOnHooks` to an idle payload when `agent_id` owns active
    /// hooks; resolves to the hook rows.
    fn annotate_waiting_on_hooks<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        data: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, Vec<serde_json::Value>>;

    /// Cancel every active hook `agent_id` owns (retire sweep).
    fn cancel_agent_hooks<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()>;

    /// The active script monitors `agent_id` owns, as waiting rows.
    fn active_script_monitors_for_agent<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<serde_json::Value>>;

    /// Add `waitingOnScriptMonitors` to an idle payload when `agent_id` owns
    /// active script monitors.
    fn annotate_waiting_on_script_monitors<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        data: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, ()>;

    /// Cancel the active script monitors of `workspace_id` (only `agent_id`'s
    /// when given), recording `reason`.
    fn cancel_script_monitors<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: Option<&'a AgentId>,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<()>>;

    /// Whether `metadata` is a script-monitor wake whose workspace is being
    /// exported (its delivery must wait).
    fn script_monitor_export_blocked(
        &self,
        svc: &Services,
        metadata: Option<&serde_json::Value>,
    ) -> bool;

    /// Re-queue an export-blocked script-monitor wake for `agent_id`;
    /// `false` (nothing queued) when it is not blocked.
    fn defer_script_monitor_for_export(
        &self,
        svc: &Services,
        agent_id: &AgentId,
        content: &str,
        metadata: Option<&serde_json::Value>,
    ) -> bool;

    /// Record which script-monitor wake (if any) `agent_id`'s starting turn
    /// delivers.
    fn track_script_monitor_worker(
        &self,
        svc: &Services,
        agent_id: &AgentId,
        metadata: Option<&serde_json::Value>,
    );

    /// Complete the script-monitor delivery `agent_id`'s finished turn
    /// carried, if any.
    fn settle_script_monitor_worker<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()>;

    /// Admit a turn carrying `metadata` under the monitor lane: `true` for a
    /// non-monitor turn or an accepted monitor delivery.
    fn admit_script_monitor_turn<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<bool>>;

    /// Whether a turn carrying `metadata` may still be delivered to
    /// `agent_id` (`true` for a non-monitor turn).
    fn script_monitor_delivery_allowed<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<bool>>;

    /// Record script-monitor delivery `monitor_id` to `agent_id` as finished.
    fn complete_script_monitor_delivery<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        monitor_id: &'a str,
    ) -> BoxFuture<'a, ()>;

    /// The lane that serializes script-monitor delivery with queue
    /// suppression and exports.
    fn script_monitor_lane<'a>(&'a self, svc: &'a Services) -> &'a tokio::sync::Mutex<()>;

    /// Stop every script of `workspace_id` and refuse new launches while the
    /// returned guard lives (workspace retire).
    fn close_workspace_scripts<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, Box<dyn ScriptsClosing>>;

    // Daemon and workspace lifecycle entry points core reaches the runtime
    // through (boot hydration, shutdown, workspace delete / archive, the
    // host execution context, and agents' ACP terminal host). Each behaves
    // exactly as the runtime code it forwards to; the async ones box that
    // future, adding no await point.

    /// Recover pending script monitors, start their maintenance loop and
    /// hydrate the script registry from the store; resolves to the number of
    /// scripts loaded.
    fn hydrate_scripts<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<usize>>;

    /// After a workspace import commits: recover script monitors, start
    /// their maintenance loop, and refresh the live definitions of the
    /// imported `script_ids` of `workspace_id`. Best-effort: failures are
    /// logged, never returned.
    fn recover_imported_scripts<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        script_ids: &'a [String],
    ) -> BoxFuture<'a, ()>;

    /// Re-arm the active hooks from the store; resolves to the number of
    /// hooks resumed.
    fn rehydrate_hooks<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<usize>>;

    /// Command-mode script runs in flight and the last settlement time.
    fn command_script_activity(&self, svc: &Services) -> CommandScriptActivity;

    /// Whether `workspace_id` has a command-mode script run in flight.
    fn workspace_scripts_running(&self, svc: &Services, workspace_id: &WorkspaceId) -> bool;

    /// Stop every managed script and kill every PTY session for daemon
    /// shutdown; resolves to `(scripts_stopped, ptys_killed)`.
    fn stop_all_scripts<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, (usize, usize)>;

    /// Drop every live hook scheduler task handle (agent-delivery shutdown).
    fn clear_hook_tasks(&self, svc: &Services);

    /// Abort `workspace_id`'s live hook scheduler tasks (workspace delete).
    fn abort_workspace_hook_tasks<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, ()>;

    /// Cancel every active hook of `workspace_id` (archive sweep); resolves
    /// to the cancelled `(name, id)` pairs per owning agent.
    fn cancel_workspace_hooks<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, BTreeMap<AgentId, Vec<(String, HookId)>>>;

    /// Delete `workspace_id`'s on-disk script output; `false` when the
    /// best-effort delete failed.
    fn delete_workspace_script_output<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, bool>;

    /// The host execution context snapshot (`host.executionContext`).
    fn execution_context_snapshot<'a>(
        &'a self,
        svc: &'a Services,
    ) -> BoxFuture<'a, Result<HostExecutionContext>>;

    /// The ACP terminal host an agent session's provider drives: PTY
    /// terminals on the shared host, spawned through a shell when
    /// `terminal_requires_shell`, defaulting to `session_cwd`.
    fn agent_terminal_host(
        &self,
        svc: &Services,
        terminal_requires_shell: bool,
        session_cwd: Option<PathBuf>,
    ) -> Arc<dyn intent_acp::TerminalHost>;
}

/// A workspace's script closing mark ([`RuntimePort::close_workspace_scripts`]):
/// dropping it reopens the workspace for script launches.
pub trait ScriptsClosing: Send {
    /// Script ids the close could not fully stop and settle.
    fn unsettled(&self) -> &[String];
}

/// The core-internal entry points of an unregistered runtime port fail
/// with this error (they have no `WorkspaceApi` default to mirror).
fn runtime_unavailable() -> Error {
    Error::Internal("runtime feature not registered".to_string())
}

/// Outcome of sandbox provisioning.
#[derive(Debug, Clone)]
pub enum ProvisionOutcome {
    /// `CoW` is supported; sandbox was created.
    Supported {
        path: PathBuf,
        branch: String,
        base_commit_sha: String,
        snapshot_commit_sha: Option<String>,
    },
    /// `CoW` is not supported; fallback to shared mode (no bytes copied).
    Unsupported,
}

/// Outcome of a merge-back attempt.
#[derive(Debug, Clone)]
pub enum MergeOutcome {
    /// Clean merge; sandbox commits applied to canonical.
    Merged {
        commit_range: String,
        canonical_head: String,
    },
    /// Conflicts detected; user's repo left pristine.
    Conflict {
        conflicting_paths: Vec<String>,
        canonical_head: String,
    },
    /// Blocked: the merge could not proceed but stays retryable — canonical
    /// has uncommitted user edits overlapping merge paths (`overlapping_paths`
    /// populated), or the sandbox branch is missing/unborn (`overlapping_paths`
    /// empty; see `reason`).
    Blocked {
        reason: String,
        overlapping_paths: Vec<String>,
    },
}

/// One workspace-creation setup-script run ([`RuntimePort::run_setup_script`]).
pub struct SetupScriptRun {
    pub workspace_id: WorkspaceId,
    /// The provisioned worktree: the script's cwd and `.intent/` parent.
    pub worktree: PathBuf,
    /// The effective setup script body.
    pub script: String,
    /// `MAIN_CHECKOUT`.
    pub repo_path: String,
    /// `BRANCH_NAME`.
    pub branch_name: String,
    /// `SOURCE_BRANCH`.
    pub source_branch: String,
}

/// Command-mode run activity for the idle update handshake (see
/// [`RuntimePort::command_script_activity`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommandScriptActivity {
    /// Command runs in flight: admitted and not yet durably settled, or with
    /// a live process. A run stays in flight after its process exits until
    /// its result is persisted, so a restart never races the settlement.
    pub in_flight: usize,
    /// When the most recent command run settled, so a run that starts and
    /// ends between two samples still restarts an idle grace.
    pub last_settled_at: Option<std::time::Instant>,
}

/// The queued-message id of a script-monitor wake (`script-monitor:<id>`),
/// if `metadata` describes one. Shared by runtime and agents.
#[expect(
    clippy::must_use_candidate,
    reason = "internal doc-hidden feature API, not a public library API"
)]
pub fn wake_id(metadata: Option<&serde_json::Value>) -> Option<String> {
    let md = metadata?;
    (md["type"] == "script_monitor_wake")
        .then(|| {
            md["monitorId"]
                .as_str()
                .map(|id| format!("script-monitor:{id}"))
        })
        .flatten()
}

/// The monitor id of a script-monitor wake, if `metadata` describes one.
#[expect(
    clippy::must_use_candidate,
    reason = "internal doc-hidden feature API, not a public library API"
)]
pub fn monitor_id(metadata: Option<&serde_json::Value>) -> Option<&str> {
    let md = metadata?;
    (md["type"] == "script_monitor_wake")
        .then(|| md["monitorId"].as_str())
        .flatten()
}

/// How a cancel transition ([`Services::cancel_active_hook`](crate::hook_manager::HookServicesExt::cancel_active_hook),
/// [`Services::cancel_active_pr_monitor`]) settles the owner's deferred
/// completion watches. A completion watch on an idle owner defers while the
/// owner has active hooks / PR monitors, so cancelling the LAST one must
/// re-run the deferral backstop
/// ([`Services::redeliver_completion_after_queue_mutation`]) — either
/// through a wake or directly — or the watch strands forever.
#[derive(Debug, Clone, Copy)]
pub enum CancelSettlement<'a> {
    /// Wake the owner with this notice; the wake runs the backstop itself
    /// after the delivery attempt (FE/app cancel).
    Notify(&'a str),
    /// No wake; run the backstop directly (owner-side cancel, retire sweep).
    Resettle,
    /// Neither: the caller sweeps several items and runs the backstop ONCE
    /// per owner after queueing its consolidated wake (archive sweep). The
    /// per-item backstop would otherwise see an empty queue and no remaining
    /// watches on the final item and synthesize a genuine completion,
    /// consuming a parent's watch BEFORE the consolidated notice is queued.
    Deferred,
}

/// The agent-wake feature: what the runtime (script monitors, hooks, script
/// ownership checks) and host execution need from agents, so they never name
/// agent types. Implemented on the agents side (`crate::agent_wake`).
///
/// Each method behaves exactly as the agent call it replaces; the async ones
/// box the same future, adding no await point.
pub trait AgentWakePort: Send + Sync + 'static {
    /// Deliver `content` to `agent_id` as a wake message, starting a turn or
    /// queueing it (`Services::deliver_wake_message`).
    fn deliver_wake_message<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
        content: &'a str,
        metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<serde_json::Value>>;

    /// Queue an automatic message for `agent_id` (no attachments, prepend or
    /// interrupt) under `message_id`, without draining. Returns the queue
    /// row describing the queued entry.
    ///
    /// # Errors
    ///
    /// [`Error::Internal`] if the queued entry cannot be serialized; the
    /// message is queued either way.
    fn enqueue_automatic_message(
        &self,
        svc: &Services,
        agent_id: &AgentId,
        message_id: Option<String>,
        content: String,
        metadata: Option<serde_json::Value>,
    ) -> Result<intent_store::AgentQueueRow>;

    /// Persist `agent_id`'s queue snapshot and publish `agent:queue_updated`.
    fn publish_queue_updated<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()>;

    /// The live queue drainer (the agent manager), if one is attached. The
    /// caller holds the handle across the steps that need it.
    fn queue_drainer(&self, svc: &Services) -> Option<Arc<dyn AgentQueueDrainer>>;

    /// The per-agent gate that serializes retirement with restore; hold its
    /// lock to keep `agent_id` from retiring meanwhile.
    fn retirement_gate(&self, svc: &Services, agent_id: &AgentId) -> Arc<tokio::sync::Mutex<()>>;

    /// Whether `status` is terminal: the agent can no longer receive wakes.
    fn is_terminal_status(&self, status: AgentStatus) -> bool;

    /// Whether settings disable `provider_id` (`providers.enabled`).
    fn provider_is_disabled(
        &self,
        provider_id: &str,
        enabled: Option<&std::collections::BTreeMap<String, bool>>,
    ) -> bool;
}

/// Drains an agent's message queue: starts the next queued turn if the agent
/// is idle ([`AgentWakePort::queue_drainer`]).
pub trait AgentQueueDrainer: Send + Sync {
    fn try_drain_queue(
        self: Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'static, ()>;
}

/// The agent-wake port of a `Services` no agents feature was registered on:
/// there is no agent to wake. Deliveries and enqueues fail, there is no
/// drainer, every status is terminal and no provider is disabled.
#[derive(Default)]
pub struct UnavailableAgentWake;

fn agent_wake_unavailable() -> Error {
    Error::Internal("agent wake port not registered".into())
}

impl AgentWakePort for UnavailableAgentWake {
    fn deliver_wake_message<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _agent_id: &'a AgentId,
        _content: &'a str,
        _metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async { Err(agent_wake_unavailable()) })
    }

    fn enqueue_automatic_message(
        &self,
        _svc: &Services,
        _agent_id: &AgentId,
        _message_id: Option<String>,
        _content: String,
        _metadata: Option<serde_json::Value>,
    ) -> Result<intent_store::AgentQueueRow> {
        Err(agent_wake_unavailable())
    }

    fn publish_queue_updated<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn queue_drainer(&self, _svc: &Services) -> Option<Arc<dyn AgentQueueDrainer>> {
        None
    }

    fn retirement_gate(&self, _svc: &Services, _agent_id: &AgentId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::new(tokio::sync::Mutex::new(()))
    }

    fn is_terminal_status(&self, _status: AgentStatus) -> bool {
        true
    }

    fn provider_is_disabled(
        &self,
        _provider_id: &str,
        _enabled: Option<&std::collections::BTreeMap<String, bool>>,
    ) -> bool {
        false
    }
}

/// A `WorkspaceApi` with no overrides: every method is the trait default.
struct DefaultWorkspaceApi;

impl WorkspaceApi for DefaultWorkspaceApi {}

static DEFAULT_WORKSPACE_API: DefaultWorkspaceApi = DefaultWorkspaceApi;

/// The runtime port of a `Services` no runtime feature was registered on:
/// every RPC answers exactly as the `WorkspaceApi` default does.
#[derive(Default)]
pub(crate) struct UnavailableRuntime;

impl RuntimePort for UnavailableRuntime {
    fn script_list<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_list(workspace_id)
    }

    fn script_list_filtered<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        archive: intent_core::ScriptArchiveFilter,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_list_filtered(workspace_id, archive)
    }

    fn script_list_view<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_list_view(workspace_id, options)
    }

    fn script_get<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_get(workspace_id, script_id)
    }

    fn script_monitor_list_view<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_monitor_list_view(workspace_id, agent_id, options)
    }

    fn script_monitor_get<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        monitor_id: String,
        owner: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_monitor_get(workspace_id, monitor_id, owner)
    }

    fn script_archive<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_archive(workspace_id, script_ids)
    }

    fn script_restore<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_restore(workspace_id, script_ids)
    }

    fn script_create<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_create(workspace_id, params)
    }

    fn script_remove<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_remove(workspace_id, script_id)
    }

    fn script_monitor<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        script_id: String,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_monitor(workspace_id, agent_id, script_id, options)
    }

    fn script_monitor_list<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_monitor_list(workspace_id, agent_id)
    }

    fn script_monitor_cancel<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        monitor_id: String,
        owner: Option<AgentId>,
        stop_run: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_monitor_cancel(workspace_id, monitor_id, owner, stop_run)
    }

    fn script_start<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_start(workspace_id, script_id)
    }

    fn script_start_monitor<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        script_id: String,
        run_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_start_monitor(workspace_id, agent_id, script_id, run_id)
    }

    fn script_stop<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_stop(workspace_id, script_id)
    }

    fn script_restart<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_restart(workspace_id, script_id)
    }

    fn script_output<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_output(
            workspace_id,
            script_id,
            max_lines,
            paginate,
            page_token,
        )
    }

    fn script_status<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_status(workspace_id, script_id)
    }

    fn script_run<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.script_run(workspace_id, script_id, max_lines, timeout_seconds)
    }

    fn hook_schedule<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        params: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.hook_schedule(workspace_id, agent_id, params)
    }

    fn hook_list<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        include_retired: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.hook_list(workspace_id, agent_id, include_retired)
    }

    fn hook_get<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.hook_get(workspace_id, hook_id)
    }

    fn hook_cancel<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
        caller: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.hook_cancel(workspace_id, hook_id, caller)
    }

    fn hook_run_now<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.hook_run_now(workspace_id, hook_id)
    }

    fn terminal_create<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
        env: Option<std::collections::BTreeMap<String, String>>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.terminal_create(workspace_id, cols, rows, cwd, command, env)
    }

    fn terminal_write<'a>(
        &'a self,
        _svc: &'a Services,
        terminal_id: String,
        data: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.terminal_write(terminal_id, data)
    }

    fn terminal_resize<'a>(
        &'a self,
        _svc: &'a Services,
        terminal_id: String,
        cols: u16,
        rows: u16,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.terminal_resize(terminal_id, cols, rows)
    }

    fn terminal_kill<'a>(
        &'a self,
        _svc: &'a Services,
        terminal_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.terminal_kill(terminal_id)
    }

    fn terminal_get_buffer<'a>(
        &'a self,
        _svc: &'a Services,
        terminal_id: String,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.terminal_get_buffer(terminal_id, max_bytes)
    }

    fn terminal_list<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.terminal_list(workspace_id)
    }

    fn terminal_read_output<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        terminal_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.terminal_read_output(
            workspace_id,
            terminal_id,
            max_lines,
            paginate,
            page_token,
        )
    }

    fn browser_list_tabs<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<Vec<BrowserTab>>> {
        DEFAULT_WORKSPACE_API.browser_list_tabs(workspace_id)
    }

    fn browser_upsert_tab<'a>(
        &'a self,
        _svc: &'a Services,
        host: ClientId,
        tab: BrowserTabInput,
    ) -> BoxFuture<'a, Result<BrowserTab>> {
        DEFAULT_WORKSPACE_API.browser_upsert_tab(host, tab)
    }

    fn browser_remove_tab<'a>(
        &'a self,
        _svc: &'a Services,
        host: ClientId,
        tab_id: String,
    ) -> BoxFuture<'a, Result<()>> {
        DEFAULT_WORKSPACE_API.browser_remove_tab(host, tab_id)
    }

    fn browser_sync_tabs<'a>(
        &'a self,
        _svc: &'a Services,
        host: ClientId,
        tabs: Vec<BrowserTabInput>,
    ) -> BoxFuture<'a, Result<Vec<String>>> {
        DEFAULT_WORKSPACE_API.browser_sync_tabs(host, tabs)
    }

    fn browser_exec<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        actions: Vec<serde_json::Value>,
        tab_id: Option<String>,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.browser_exec(workspace_id, actions, tab_id, agent_id)
    }

    fn browser_navigate_tab<'a>(
        &'a self,
        _svc: &'a Services,
        tab_id: String,
        url: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.browser_navigate_tab(tab_id, url)
    }

    fn browser_close_tab<'a>(
        &'a self,
        _svc: &'a Services,
        tab_id: String,
        force: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.browser_close_tab(tab_id, force)
    }

    fn sandbox_merge<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.sandbox_merge(workspace_id, sandbox_id)
    }

    fn sandbox_discard<'a>(
        &'a self,
        _svc: &'a Services,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        DEFAULT_WORKSPACE_API.sandbox_discard(workspace_id, sandbox_id)
    }

    fn migrate_claimed_browser_tabs<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _new_host: &'a ClientId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(runtime_unavailable()) })
    }

    fn provision_sandbox<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _agent_id: &'a AgentId,
        _workspaces_root: PathBuf,
    ) -> BoxFuture<'a, Result<ProvisionOutcome>> {
        Box::pin(async { Err(runtime_unavailable()) })
    }

    fn merge_sandbox<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<MergeOutcome>> {
        Box::pin(async { Err(runtime_unavailable()) })
    }

    fn discard_sandbox<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Err(runtime_unavailable()) })
    }

    fn run_setup_script(
        &self,
        _svc: &Services,
        _run: SetupScriptRun,
    ) -> BoxFuture<'static, (bool, Option<u32>)> {
        Box::pin(async { (false, None) })
    }

    fn active_hooks_for_agent<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<serde_json::Value>> {
        Box::pin(async { Vec::new() })
    }

    fn active_hooks_by_agent<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, HashMap<String, Vec<serde_json::Value>>> {
        Box::pin(async { HashMap::new() })
    }

    fn annotate_waiting_on_hooks<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
        _data: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, Vec<serde_json::Value>> {
        Box::pin(async { Vec::new() })
    }

    fn cancel_agent_hooks<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn active_script_monitors_for_agent<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<serde_json::Value>> {
        Box::pin(async { Vec::new() })
    }

    fn annotate_waiting_on_script_monitors<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
        _data: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn cancel_script_monitors<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _agent_id: Option<&'a AgentId>,
        _reason: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn script_monitor_export_blocked(
        &self,
        _svc: &Services,
        _metadata: Option<&serde_json::Value>,
    ) -> bool {
        false
    }

    fn defer_script_monitor_for_export(
        &self,
        _svc: &Services,
        _agent_id: &AgentId,
        _content: &str,
        _metadata: Option<&serde_json::Value>,
    ) -> bool {
        false
    }

    fn track_script_monitor_worker(
        &self,
        _svc: &Services,
        _agent_id: &AgentId,
        _metadata: Option<&serde_json::Value>,
    ) {
    }

    fn settle_script_monitor_worker<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn admit_script_monitor_turn<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
        metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move { Ok(monitor_id(metadata).is_none()) })
    }

    fn script_monitor_delivery_allowed<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
        metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move { Ok(monitor_id(metadata).is_none()) })
    }

    fn complete_script_monitor_delivery<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
        _monitor_id: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn script_monitor_lane<'a>(&'a self, _svc: &'a Services) -> &'a tokio::sync::Mutex<()> {
        static LANE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        &LANE
    }

    fn close_workspace_scripts<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, Box<dyn ScriptsClosing>> {
        Box::pin(async { Box::new(NoScriptsClosing) as Box<dyn ScriptsClosing> })
    }

    fn hydrate_scripts<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async { Ok(0) })
    }

    fn recover_imported_scripts<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _script_ids: &'a [String],
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn rehydrate_hooks<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async { Ok(0) })
    }

    fn command_script_activity(&self, _svc: &Services) -> CommandScriptActivity {
        CommandScriptActivity::default()
    }

    fn workspace_scripts_running(&self, _svc: &Services, _workspace_id: &WorkspaceId) -> bool {
        false
    }

    fn stop_all_scripts<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, (usize, usize)> {
        Box::pin(async { (0, 0) })
    }

    fn clear_hook_tasks(&self, _svc: &Services) {}

    fn abort_workspace_hook_tasks<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn cancel_workspace_hooks<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, BTreeMap<AgentId, Vec<(String, HookId)>>> {
        Box::pin(async { BTreeMap::new() })
    }

    fn delete_workspace_script_output<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, bool> {
        Box::pin(async { true })
    }

    fn execution_context_snapshot<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, Result<HostExecutionContext>> {
        Box::pin(async { Err(runtime_unavailable()) })
    }

    fn agent_terminal_host(
        &self,
        _svc: &Services,
        _terminal_requires_shell: bool,
        _session_cwd: Option<PathBuf>,
    ) -> Arc<dyn intent_acp::TerminalHost> {
        Arc::new(NoTerminalHost)
    }
}

/// The ACP terminal host of an unregistered runtime: every request fails.
struct NoTerminalHost;

fn no_terminal_host() -> intent_acp::AcpError {
    intent_acp::AcpError::Terminal("runtime feature not registered".into())
}

impl intent_acp::TerminalHost for NoTerminalHost {
    fn create(
        &self,
        _params: intent_acp::TerminalCreateParams,
    ) -> BoxFuture<'_, intent_acp::AcpResult<String>> {
        Box::pin(async { Err(no_terminal_host()) })
    }

    fn output(
        &self,
        _terminal_id: String,
    ) -> BoxFuture<'_, intent_acp::AcpResult<intent_acp::TerminalOutputInfo>> {
        Box::pin(async { Err(no_terminal_host()) })
    }

    fn wait_for_exit(
        &self,
        _terminal_id: String,
    ) -> BoxFuture<'_, intent_acp::AcpResult<intent_acp::TerminalExitInfo>> {
        Box::pin(async { Err(no_terminal_host()) })
    }

    fn release(&self, _terminal_id: String) -> BoxFuture<'_, intent_acp::AcpResult<()>> {
        Box::pin(async { Err(no_terminal_host()) })
    }

    fn kill(&self, _terminal_id: String) -> BoxFuture<'_, intent_acp::AcpResult<()>> {
        Box::pin(async { Err(no_terminal_host()) })
    }
}

/// The closing mark of an unregistered runtime: there are no scripts.
struct NoScriptsClosing;

impl ScriptsClosing for NoScriptsClosing {
    fn unsettled(&self) -> &[String] {
        &[]
    }
}

/// Typed feature state on [`Services`], keyed by type.
///
/// A feature crate stores its state object here once, when the composition
/// root registers it ([`Services::with_feature`]), and its extension-trait
/// code reads it back with [`Services::feature`]. The set is frozen once the
/// `Services` is shared: it changes only through the owned builder methods,
/// and clones share the same objects.
#[derive(Clone, Default)]
pub(crate) struct FeatureSet {
    features: Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl FeatureSet {
    fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.features
            .get(&TypeId::of::<T>())
            .and_then(|feature| feature.downcast_ref::<T>())
    }

    fn insert<T: Any + Send + Sync>(&mut self, feature: Arc<T>) {
        let previous = Arc::make_mut(&mut self.features).insert(TypeId::of::<T>(), feature);
        assert!(
            previous.is_none(),
            "feature {} registered twice",
            std::any::type_name::<T>()
        );
    }
}

impl Services {
    /// Store a feature's state object. Every clone made afterwards shares
    /// it; clones made before do not see it.
    ///
    /// # Panics
    ///
    /// If a feature of the same type is already registered: each feature is
    /// registered once, by the composition root.
    #[must_use]
    pub fn with_feature<T: Any + Send + Sync>(mut self, feature: Arc<T>) -> Self {
        self.features.insert(feature);
        self
    }

    /// The registered feature state of type `T`, if any.
    #[must_use]
    pub fn feature<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.features.get::<T>()
    }
}

/// A registered port and whether the composition root registered it (as
/// opposed to the "unavailable" default [`Services::new`] starts with).
struct PortSlot<P: ?Sized> {
    port: Arc<P>,
    registered: bool,
}

impl<P: ?Sized> Clone for PortSlot<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            registered: self.registered,
        }
    }
}

/// Declares the feature port registry: the [`FeaturePorts`] fields, their
/// accessors, the `Services::with_*_port` setters, and the list the startup
/// check walks.
///
/// Adding a port is one line here: `name: Trait = UnavailableDefault,
/// setter;`. Every port declared here is a production port, so
/// [`Services::ensure_feature_ports_registered`] fails until the composition
/// root registers it.
macro_rules! feature_port_registry {
    ($(
        $(#[$setter_doc:meta])*
        $name:ident: $port:ident = $default:ty, $setter:ident;
    )*) => {
        /// The registered ports. Cloned with `Services`; every clone shares
        /// the same feature objects (and so the same feature state).
        #[derive(Clone)]
        pub(crate) struct FeaturePorts {
            $($name: PortSlot<dyn $port>,)*
        }

        impl Default for FeaturePorts {
            fn default() -> Self {
                Self {
                    $($name: PortSlot {
                        port: Arc::new(<$default>::default()),
                        registered: false,
                    },)*
                }
            }
        }

        impl FeaturePorts {
            /// Every declared port, by name.
            pub(crate) const NAMES: &'static [&'static str] = &[$(stringify!($name)),*];

            $(
                pub(crate) fn $name(&self) -> &dyn $port {
                    self.$name.port.as_ref()
                }
            )*

            /// The declared ports no feature registered, in declaration
            /// order.
            pub(crate) fn unregistered(&self) -> Vec<&'static str> {
                let registered = [$(self.$name.registered),*];
                Self::NAMES
                    .iter()
                    .zip(registered)
                    .filter(|(_, registered)| !registered)
                    .map(|(name, _)| *name)
                    .collect()
            }
        }

        impl Services {
            $(
                $(#[$setter_doc])*
                #[must_use]
                pub fn $setter(mut self, port: Arc<dyn $port>) -> Self {
                    self.ports.$name = PortSlot {
                        port,
                        registered: true,
                    };
                    self
                }
            )*
        }
    };
}

feature_port_registry! {
    /// Register the workspace transfer feature. Every clone made afterwards
    /// shares it; clones made before keep the port they had.
    transfer: TransferPort = UnavailableTransfer, with_transfer_port;
    /// Register the workspace runtime feature (scripts and hooks).
    /// [`Services::new`] registers the in-crate one.
    runtime: RuntimePort = UnavailableRuntime, with_runtime_port;
    /// Register the agent-wake feature runtime code wakes agents through.
    /// [`Services::new`] registers the in-crate one.
    agent_wake: AgentWakePort = UnavailableAgentWake, with_agent_wake_port;
}

/// Writes `WorkspaceApi` methods that forward to a feature port: each
/// `fn method(&self, args..) -> Ret => port_method;` line expands to
/// `self.ports.<port>().port_method(self, args..)`. Use it inside
/// `impl WorkspaceApi for Services`.
macro_rules! port_forwarders {
    ($port:ident {
        $(
            fn $method:ident(&self $(, $arg:ident: $ty:ty)* $(,)?) -> $ret:ty => $target:ident;
        )*
    }) => {
        $(
            fn $method(&self $(, $arg: $ty)*) -> $ret {
                self.ports.$port().$target(self $(, $arg)*)
            }
        )*
    };
}
pub(crate) use port_forwarders;

impl FeaturePorts {
    /// A shared handle on the agent-wake port, for holders that outlive the
    /// borrow of `Services` (the script manager).
    pub(crate) fn agent_wake_port(&self) -> Arc<dyn AgentWakePort> {
        Arc::clone(&self.agent_wake.port)
    }
}

impl Services {
    /// The declared feature ports that no feature registered. Empty once the
    /// composition root has run.
    #[must_use]
    pub fn unregistered_feature_ports(&self) -> Vec<&'static str> {
        self.ports.unregistered()
    }

    /// The startup check: fails, naming every missing port, unless each
    /// declared feature port was registered. An unregistered port would
    /// otherwise surface only at its first call, as an "unavailable" error.
    ///
    /// # Errors
    ///
    /// [`Error::Internal`] listing the unregistered ports.
    pub fn ensure_feature_ports_registered(&self) -> Result<()> {
        let missing = self.unregistered_feature_ports();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(Error::Internal(format!(
                "feature ports not registered: {}",
                missing.join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::RuntimeStateExt;
    use std::sync::Arc;

    use intent_core::{Error, WorkspaceApi, WorkspaceId};

    use super::{
        AgentWakePort, FeaturePorts, RuntimePort, TransferPort, UnavailableAgentWake,
        UnavailableRuntime, UnavailableTransfer, DEFAULT_WORKSPACE_API,
    };

    use crate::test_support::fixtures::TempDb;
    use crate::Services;

    #[tokio::test]
    async fn unregistered_transfer_port_fails_like_the_workspace_api_default() {
        let tmp = TempDb::new();
        let svc = Services::new(intent_store::Store::open(&tmp.path).await.unwrap());
        let ws = WorkspaceId::new();
        let expect = |result: intent_core::Result<serde_json::Value>, method: &str| {
            assert!(
                matches!(&result, Err(Error::Internal(m)) if m == &format!("WorkspaceApi::{method} not implemented")),
                "{method}: {result:?}"
            );
        };
        assert!(matches!(
            svc.workspace_transfer_plan(ws.clone()).await,
            Err(Error::Internal(m)) if m == "WorkspaceApi::workspace_transfer_plan not implemented"
        ));
        expect(
            svc.workspace_import_begin(serde_json::json!({}), 0, String::new())
                .await,
            "workspace_import_begin",
        );
        expect(
            svc.workspace_import_chunk("i".into(), 0, String::new())
                .await,
            "workspace_import_chunk",
        );
        expect(
            svc.workspace_import_commit("i".into()).await,
            "workspace_import_commit",
        );
        expect(
            svc.workspace_import_abort("i".into()).await,
            "workspace_import_abort",
        );
        expect(
            svc.workspace_export_start(ws.clone()).await,
            "workspace_export_start",
        );
        expect(
            svc.workspace_export_read("e".into(), 0).await,
            "workspace_export_read",
        );
        expect(
            svc.workspace_export_finalize("e".into(), false, None).await,
            "workspace_export_finalize",
        );
        expect(
            svc.workspace_export_abort("e".into()).await,
            "workspace_export_abort",
        );
        assert!(!svc.feature_transfer_port().export_in_progress(ws.as_str()));
    }

    async fn bare_services(tmp: &TempDb) -> Services {
        Services::new(intent_store::Store::open(&tmp.path).await.unwrap())
    }

    #[tokio::test]
    async fn the_startup_check_names_every_unregistered_port() {
        let tmp = TempDb::new();
        let svc = bare_services(&tmp).await;
        assert_eq!(FeaturePorts::NAMES, ["transfer", "runtime", "agent_wake"]);
        assert_eq!(
            FeaturePorts::default().unregistered(),
            FeaturePorts::NAMES,
            "every declared port, the in-crate ones included, starts unregistered"
        );
        assert_eq!(
            svc.unregistered_feature_ports(),
            ["transfer"],
            "Services::new registers the in-crate runtime and agent-wake features"
        );
        assert!(matches!(
            svc.ensure_feature_ports_registered(),
            Err(Error::Internal(m)) if m == "feature ports not registered: transfer"
        ));
    }

    #[tokio::test]
    async fn a_registered_port_passes_the_startup_check_and_receives_calls() {
        let tmp = TempDb::new();
        let port = Arc::new(UnavailableTransfer::default());
        let svc = bare_services(&tmp)
            .await
            .with_transfer_port(Arc::clone(&port) as Arc<dyn TransferPort>);
        assert!(svc.unregistered_feature_ports().is_empty());
        svc.ensure_feature_ports_registered()
            .expect("every port registered");
        let ws = WorkspaceId::new();
        port.park_building_export_for_test("e1", &ws, None, std::path::PathBuf::new());
        assert!(
            svc.clone()
                .feature_transfer_port()
                .export_in_progress(ws.as_str()),
            "the forwarders and clones reach the registered object"
        );
    }

    #[tokio::test]
    async fn services_new_registers_one_runtime_feature_shared_by_clones() {
        let tmp = TempDb::new();
        let svc = bare_services(&tmp).await;
        assert!(!svc.unregistered_feature_ports().contains(&"runtime"));
        let clone = svc.clone();
        assert!(std::ptr::eq(svc.runtime(), clone.runtime()));
        assert!(std::ptr::eq(
            svc.runtime(),
            svc.feature::<crate::runtime::RuntimeFeature>()
                .expect("registered")
        ));
    }

    #[tokio::test]
    async fn unregistered_agent_wake_port_has_no_agent_to_wake() {
        let tmp = TempDb::new();
        let svc = bare_services(&tmp)
            .await
            .with_agent_wake_port(Arc::new(UnavailableAgentWake) as Arc<dyn AgentWakePort>);
        let port = svc.ports.agent_wake();
        let ws = WorkspaceId::new();
        let agent = intent_core::AgentId::new();
        let unavailable =
            |e: &Error| matches!(e, Error::Internal(m) if m == "agent wake port not registered");
        let delivered = port
            .deliver_wake_message(&svc, &ws, &agent, "wake", None)
            .await;
        assert!(delivered.as_ref().is_err_and(unavailable), "{delivered:?}");
        let queued = port.enqueue_automatic_message(&svc, &agent, None, "m".into(), None);
        assert!(queued.as_ref().is_err_and(unavailable), "{queued:?}");
        port.publish_queue_updated(&svc, &agent).await;
        assert!(port.queue_drainer(&svc).is_none());
        let first = port.retirement_gate(&svc, &agent);
        let _held = first.try_lock().expect("a fresh gate");
        assert!(port.is_terminal_status(intent_core::AgentStatus::Idle));
        let disabled = std::collections::BTreeMap::from([("claude".to_owned(), false)]);
        assert!(!port.provider_is_disabled("claude", Some(&disabled)));
    }

    #[tokio::test]
    async fn the_registered_agent_wake_port_answers_like_the_agent_calls() {
        use intent_core::AgentStatus;
        let tmp = TempDb::new();
        let svc = bare_services(&tmp).await;
        let port = svc.ports.agent_wake();
        for status in AgentStatus::ALL {
            assert_eq!(
                port.is_terminal_status(status),
                crate::agent_ops::is_terminal_status(status),
                "{status:?}"
            );
        }
        for enabled in [
            None,
            Some(std::collections::BTreeMap::from([(
                "claude".to_owned(),
                false,
            )])),
            Some(std::collections::BTreeMap::from([(
                "claude".to_owned(),
                true,
            )])),
        ] {
            for provider in ["claude", "codex", "no-such-provider"] {
                assert_eq!(
                    port.provider_is_disabled(provider, enabled.as_ref()),
                    crate::agent_ops::provider_is_disabled(provider, enabled.as_ref()),
                    "{provider} {enabled:?}"
                );
            }
        }
        let agent = intent_core::AgentId::new();
        let gate = port.retirement_gate(&svc, &agent);
        assert!(
            Arc::ptr_eq(&gate, &svc.agent_retirement_gates.for_agent(&agent)),
            "the port hands out the agent's own retirement gate"
        );
        assert!(
            port.queue_drainer(&svc).is_none(),
            "no drainer before an agent manager is attached"
        );
        let receipt = port
            .enqueue_automatic_message(
                &svc,
                &agent,
                Some("wake-1".into()),
                "body".into(),
                Some(serde_json::json!({"type": "script_monitor_wake"})),
            )
            .expect("queued");
        assert_eq!(receipt.id, "wake-1");
        assert_eq!(receipt.agent_id, agent);
        assert_eq!(receipt.position, 0);
        assert_eq!(receipt.payload["content"], "body");
        assert_eq!(receipt.payload["userOrigin"], false);
        assert_eq!(
            receipt.payload["messageMetadata"]["type"],
            "script_monitor_wake"
        );
        assert_eq!(receipt.turn_id, receipt.payload["turnId"]);
        assert_eq!(receipt.created_at, receipt.payload["queuedAt"]);
    }

    #[tokio::test]
    async fn unregistered_runtime_port_answers_like_the_workspace_api_default() {
        let tmp = TempDb::new();
        let svc = bare_services(&tmp)
            .await
            .with_runtime_port(Arc::new(UnavailableRuntime) as Arc<dyn RuntimePort>);
        let api = &DEFAULT_WORKSPACE_API;
        let ws = WorkspaceId::new();
        let agent = intent_core::AgentId::new();
        let hook = intent_core::HookId::new();
        let same = |method: &str,
                    got: intent_core::Result<serde_json::Value>,
                    want: intent_core::Result<serde_json::Value>| {
            assert_eq!(format!("{got:?}"), format!("{want:?}"), "{method}");
        };
        let summary = serde_json::json!({"view": "summary"});
        let full = serde_json::json!({});
        same(
            "script_list",
            svc.script_list(ws.clone()).await,
            api.script_list(ws.clone()).await,
        );
        for archive in [
            intent_core::ScriptArchiveFilter::Active,
            intent_core::ScriptArchiveFilter::Archived,
        ] {
            same(
                "script_list_filtered",
                svc.script_list_filtered(ws.clone(), archive).await,
                api.script_list_filtered(ws.clone(), archive).await,
            );
        }
        for options in [&summary, &full] {
            same(
                "script_list_view",
                svc.script_list_view(ws.clone(), options.clone()).await,
                api.script_list_view(ws.clone(), options.clone()).await,
            );
            same(
                "script_monitor_list_view",
                svc.script_monitor_list_view(ws.clone(), None, options.clone())
                    .await,
                api.script_monitor_list_view(ws.clone(), None, options.clone())
                    .await,
            );
        }
        same(
            "script_get",
            svc.script_get(ws.clone(), "s".into()).await,
            api.script_get(ws.clone(), "s".into()).await,
        );
        same(
            "script_monitor_get",
            svc.script_monitor_get(ws.clone(), "m".into(), None).await,
            api.script_monitor_get(ws.clone(), "m".into(), None).await,
        );
        same(
            "script_archive",
            svc.script_archive(ws.clone(), vec![]).await,
            api.script_archive(ws.clone(), vec![]).await,
        );
        same(
            "script_restore",
            svc.script_restore(ws.clone(), vec![]).await,
            api.script_restore(ws.clone(), vec![]).await,
        );
        let params = intent_core::ScriptCreateParams::default;
        same(
            "script_create",
            svc.script_create(ws.clone(), params()).await,
            api.script_create(ws.clone(), params()).await,
        );
        same(
            "script_remove",
            svc.script_remove(ws.clone(), "s".into()).await,
            api.script_remove(ws.clone(), "s".into()).await,
        );
        same(
            "script_monitor",
            svc.script_monitor(ws.clone(), agent.clone(), "s".into(), full.clone())
                .await,
            api.script_monitor(ws.clone(), agent.clone(), "s".into(), full.clone())
                .await,
        );
        same(
            "script_monitor_list",
            svc.script_monitor_list(ws.clone(), None).await,
            api.script_monitor_list(ws.clone(), None).await,
        );
        same(
            "script_monitor_cancel",
            svc.script_monitor_cancel(ws.clone(), "m".into(), None, false)
                .await,
            api.script_monitor_cancel(ws.clone(), "m".into(), None, false)
                .await,
        );
        same(
            "script_start",
            svc.script_start(ws.clone(), "s".into()).await,
            api.script_start(ws.clone(), "s".into()).await,
        );
        same(
            "script_start_monitor",
            svc.script_start_monitor(ws.clone(), agent.clone(), "s".into(), "r".into())
                .await,
            api.script_start_monitor(ws.clone(), agent.clone(), "s".into(), "r".into())
                .await,
        );
        same(
            "script_stop",
            svc.script_stop(ws.clone(), "s".into()).await,
            api.script_stop(ws.clone(), "s".into()).await,
        );
        same(
            "script_restart",
            svc.script_restart(ws.clone(), "s".into()).await,
            api.script_restart(ws.clone(), "s".into()).await,
        );
        same(
            "script_output",
            svc.script_output(ws.clone(), "s".into(), None, None, None)
                .await,
            api.script_output(ws.clone(), "s".into(), None, None, None)
                .await,
        );
        same(
            "script_status",
            svc.script_status(ws.clone(), "s".into()).await,
            api.script_status(ws.clone(), "s".into()).await,
        );
        same(
            "script_run",
            svc.script_run(ws.clone(), "s".into(), None, None).await,
            api.script_run(ws.clone(), "s".into(), None, None).await,
        );
        same(
            "hook_schedule",
            svc.hook_schedule(ws.clone(), agent.clone(), full.clone())
                .await,
            api.hook_schedule(ws.clone(), agent.clone(), full.clone())
                .await,
        );
        same(
            "hook_list",
            svc.hook_list(ws.clone(), None, false).await,
            api.hook_list(ws.clone(), None, false).await,
        );
        same(
            "hook_get",
            svc.hook_get(ws.clone(), hook.clone()).await,
            api.hook_get(ws.clone(), hook.clone()).await,
        );
        same(
            "hook_cancel",
            svc.hook_cancel(ws.clone(), hook.clone(), None).await,
            api.hook_cancel(ws.clone(), hook.clone(), None).await,
        );
        same(
            "hook_run_now",
            svc.hook_run_now(ws.clone(), hook.clone()).await,
            api.hook_run_now(ws.clone(), hook.clone()).await,
        );
        let client = intent_core::ClientId::from_string("client");
        let tab_input = || -> intent_core::BrowserTabInput {
            serde_json::from_value(
                serde_json::json!({"tabId": "t", "workspaceId": ws, "url": "about:blank"}),
            )
            .expect("tab input")
        };
        same(
            "terminal_create",
            svc.terminal_create(ws.clone(), 80, 24, None, None, None)
                .await,
            api.terminal_create(ws.clone(), 80, 24, None, None, None)
                .await,
        );
        same(
            "terminal_write",
            svc.terminal_write("t".into(), "x".into()).await,
            api.terminal_write("t".into(), "x".into()).await,
        );
        same(
            "terminal_resize",
            svc.terminal_resize("t".into(), 80, 24).await,
            api.terminal_resize("t".into(), 80, 24).await,
        );
        same(
            "terminal_kill",
            svc.terminal_kill("t".into()).await,
            api.terminal_kill("t".into()).await,
        );
        same(
            "terminal_get_buffer",
            svc.terminal_get_buffer("t".into(), None).await,
            api.terminal_get_buffer("t".into(), None).await,
        );
        same(
            "terminal_list",
            svc.terminal_list(ws.clone()).await,
            api.terminal_list(ws.clone()).await,
        );
        same(
            "terminal_read_output",
            svc.terminal_read_output(ws.clone(), "t".into(), None, None, None)
                .await,
            api.terminal_read_output(ws.clone(), "t".into(), None, None, None)
                .await,
        );
        assert_eq!(
            format!("{:?}", svc.browser_list_tabs(ws.clone()).await),
            format!("{:?}", api.browser_list_tabs(ws.clone()).await),
            "browser_list_tabs"
        );
        assert_eq!(
            format!(
                "{:?}",
                svc.browser_upsert_tab(client.clone(), tab_input()).await
            ),
            format!(
                "{:?}",
                api.browser_upsert_tab(client.clone(), tab_input()).await
            ),
            "browser_upsert_tab"
        );
        assert_eq!(
            format!(
                "{:?}",
                svc.browser_remove_tab(client.clone(), "t".into()).await
            ),
            format!(
                "{:?}",
                api.browser_remove_tab(client.clone(), "t".into()).await
            ),
            "browser_remove_tab"
        );
        assert_eq!(
            format!(
                "{:?}",
                svc.browser_sync_tabs(client.clone(), vec![tab_input()])
                    .await
            ),
            format!(
                "{:?}",
                api.browser_sync_tabs(client.clone(), vec![tab_input()])
                    .await
            ),
            "browser_sync_tabs"
        );
        same(
            "browser_exec",
            svc.browser_exec(ws.clone(), vec![full.clone()], None, None)
                .await,
            api.browser_exec(ws.clone(), vec![full.clone()], None, None)
                .await,
        );
        same(
            "browser_navigate_tab",
            svc.browser_navigate_tab("t".into(), "about:blank".into())
                .await,
            api.browser_navigate_tab("t".into(), "about:blank".into())
                .await,
        );
        same(
            "browser_close_tab",
            svc.browser_close_tab("t".into(), true).await,
            api.browser_close_tab("t".into(), true).await,
        );
        same(
            "sandbox_merge",
            svc.sandbox_merge(ws.clone(), agent.clone()).await,
            api.sandbox_merge(ws.clone(), agent.clone()).await,
        );
        same(
            "sandbox_discard",
            svc.sandbox_discard(ws.clone(), agent.clone()).await,
            api.sandbox_discard(ws.clone(), agent.clone()).await,
        );
        assert!(
            matches!(
                svc.script_list(ws.clone()).await,
                Err(Error::Internal(m)) if m == "WorkspaceApi::script_list not implemented"
            ),
            "the default is the trait's own unavailable answer"
        );
    }

    #[tokio::test]
    async fn unregistered_runtime_port_refuses_the_core_entry_points() {
        let tmp = TempDb::new();
        let svc = bare_services(&tmp)
            .await
            .with_runtime_port(Arc::new(UnavailableRuntime) as Arc<dyn RuntimePort>);
        let port = svc.ports.runtime();
        let ws = WorkspaceId::new();
        let agent = intent_core::AgentId::new();
        let refused = |method: &str, got: String| {
            assert_eq!(
                got, r#"Err(Internal("runtime feature not registered"))"#,
                "{method}"
            );
        };
        refused(
            "migrate_claimed_browser_tabs",
            format!(
                "{:?}",
                port.migrate_claimed_browser_tabs(
                    &svc,
                    &ws,
                    &intent_core::ClientId::from_string("client")
                )
                .await
            ),
        );
        refused(
            "provision_sandbox",
            format!(
                "{:?}",
                port.provision_sandbox(&svc, &ws, &agent, tmp.path.clone())
                    .await
            ),
        );
        refused(
            "merge_sandbox",
            format!("{:?}", port.merge_sandbox(&svc, &ws, &agent).await),
        );
        refused(
            "discard_sandbox",
            format!("{:?}", port.discard_sandbox(&svc, &ws, &agent).await),
        );
        let run = super::SetupScriptRun {
            workspace_id: ws.clone(),
            worktree: tmp.path.clone(),
            script: "exit 0".into(),
            repo_path: String::new(),
            branch_name: String::new(),
            source_branch: String::new(),
        };
        assert_eq!(
            port.run_setup_script(&svc, run).await,
            (false, None),
            "an unregistered runtime runs no setup script"
        );
        assert_eq!(
            format!("{:?}", port.rehydrate_hooks(&svc).await),
            "Ok(0)",
            "an unregistered runtime resumes no hooks"
        );
        // Best-effort and infallible: an unregistered runtime has no
        // imported scripts to recover.
        port.recover_imported_scripts(&svc, &ws, &["script".to_owned()])
            .await;
    }

    #[derive(Debug, Default)]
    struct Counter(std::sync::atomic::AtomicUsize);

    #[tokio::test]
    async fn the_feature_set_returns_the_registered_object_to_every_later_clone() {
        let tmp = TempDb::new();
        let before = bare_services(&tmp).await;
        assert!(before.feature::<Counter>().is_none());
        let counter = Arc::new(Counter::default());
        let svc = before.clone().with_feature(Arc::clone(&counter));
        let clone = svc.clone();
        clone
            .feature::<Counter>()
            .expect("registered")
            .0
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(counter.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(std::ptr::eq(
            svc.feature::<Counter>().unwrap(),
            Arc::as_ptr(&counter)
        ));
        assert!(
            before.feature::<Counter>().is_none(),
            "a clone made before registration does not see it"
        );
        assert!(svc.feature::<String>().is_none());
    }

    #[tokio::test]
    #[should_panic(expected = "registered twice")]
    async fn a_feature_registers_once() {
        let tmp = TempDb::new();
        let _ = bare_services(&tmp)
            .await
            .with_feature(Arc::new(Counter::default()))
            .with_feature(Arc::new(Counter::default()));
    }
}
