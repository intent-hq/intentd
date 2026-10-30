//! The in-crate runtime feature: the `script.*`, `hook.*`, `terminal.*`,
//! `browser.*` and `sandbox.*` RPCs behind [`RuntimePort`], the sandbox,
//! browser-client and setup-script entry points core code calls through it,
//! and the shared script/hook registries the feature owns.
//!
//! Core reaches the feature only through the port (the `WorkspaceApi`
//! forwarders) and through [`Services::runtime`](crate::runtime::RuntimeStateExt::runtime) for the registries that
//! core-resident code (the script manager view, script monitors, hook
//! scheduling, shutdown) still reads. [`Services::new`] registers it; the
//! later move into its own crate turns that registration into the
//! composition root's.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_core::execution::HostExecutionContext;
use intent_core::{
    now_iso, AgentId, BoxFuture, BrowserTab, BrowserTabInput, ClientId, Error, HookId, Result,
    ReverseDispatchError, ScriptCreateParams, WorkspaceApi, WorkspaceId,
};

use crate::browser_tabs::BrowserTabOpsExt;
use crate::browser_tabs::BrowserTabRoutingExt;
use crate::hook_manager::HookServicesExt;
use crate::host_execution::HostExecutionExt;
use crate::script_monitor::ScriptMonitorExt;
use crate::script_output_cleanup::ScriptOutputCleanupExt;
use crate::{
    browser_tabs, hook_manager, sandbox_ops, script_ops, script_output_store, setup_runner,
    terminal_ops,
};
use intent_services::feature_api::feature_ports::{
    CommandScriptActivity, MergeOutcome, ProvisionOutcome, RuntimePort, ScriptsClosing,
    SetupScriptRun,
};
use intent_services::feature_api::{
    browser_ops, capability, record_setup_running, write_private_script, Services,
    SETUP_TERMINAL_NAME,
};

/// Runtime-owned state, shared by every clone of the [`Services`] it is
/// registered on.
#[derive(Default)]
pub(crate) struct RuntimeFeature {
    /// The shared `script.*` registry (definitions + runtime + supervisor
    /// tasks), keyed by script id. Scripts run on the same PTY host as
    /// `terminal.*`, so a terminal can attach to a running script (§12.2).
    pub(crate) scripts: script_ops::ScriptRegistry,
    /// Shared across clones and manager views: bootstrap serializes per
    /// workspace; definition replacement serializes per durable script id.
    pub(crate) script_locks: script_ops::ScriptLocks,
    /// The monitor `ws.script.start` armed by default, per (workspace,
    /// script). An explicit `ws.script.monitor` by the same owner replaces it
    /// so the agent's own TTL / output pattern apply. In-memory: after a
    /// restart the default monitor simply behaves like an explicit one.
    auto_start_monitors: Mutex<HashMap<(WorkspaceId, String), String>>,
    /// Live per-hook scheduler tasks for the background hook service
    /// (`hook.*`). Shared across clones so the RPC/MCP front doors and the
    /// tasks themselves observe the same set; rehydrated from the `hook`
    /// table at boot via [`Services::rehydrate_hooks`](crate::hook_manager::HookServicesExt::rehydrate_hooks).
    pub(crate) hook_tasks: hook_manager::HookTasks,
}

/// The runtime feature state and script output store, on [`Services`].
pub(crate) trait RuntimeStateExt {
    fn runtime(&self) -> &RuntimeFeature;
    fn script_output_store(&self) -> script_output_store::ScriptOutputStore;
    fn script_manager(&self) -> script_ops::ScriptManager;
}

impl RuntimeStateExt for Services {
    /// The runtime feature registered by [`Services::new`].
    fn runtime(&self) -> &RuntimeFeature {
        self.feature::<RuntimeFeature>()
            .expect("the in-crate runtime feature is registered")
    }

    /// The on-disk script output store, built from the core
    /// [`ScriptOutputConfig`](intent_services::feature_api::runtime_config::ScriptOutputConfig).
    fn script_output_store(&self) -> script_output_store::ScriptOutputStore {
        script_output_store::ScriptOutputStore::new(
            &self.feature_script_output_config().data_dir,
            self.feature_script_output_config().max_bytes_per_run,
        )
    }

    /// Build a [`ScriptManager`](script_ops::ScriptManager) view over the shared
    /// PTY host, event bus, store, script registry, and bootstrap locks for one `script.*` call.
    fn script_manager(&self) -> script_ops::ScriptManager {
        script_ops::ScriptManager::new(
            self.pty(),
            self.feature_event_bus().cloned(),
            self.store().clone(),
            self.runtime().scripts.clone(),
            self.runtime().script_locks.clone(),
            self.feature_script_too_fast_ms(),
            self.feature_script_parks().clone(),
        )
        .with_settings(self.settings_registry())
        .with_output_store(self.script_output_store())
        .with_tasks(self.feature_store_tasks().clone())
        .with_owner_services(self.clone())
    }
}

/// Runtime entry points the daemon calls on [`Services`] directly, through
/// the composition root's `StartupExt`.
pub trait RuntimeServicesExt {
    /// Re-arm the active hooks from the store at boot; resolves to the
    /// number of hooks resumed.
    fn rehydrate_hooks(&self) -> impl Future<Output = Result<usize>> + Send;

    /// Start the periodic script output retention sweep.
    fn spawn_script_output_sweep_loop(&self) -> tokio::task::JoinHandle<()>;

    /// Start the loop that republishes the host execution context when its
    /// inputs change.
    fn spawn_execution_context_loop(&self) -> tokio::task::JoinHandle<()>;
}

impl RuntimeServicesExt for Services {
    fn rehydrate_hooks(&self) -> impl Future<Output = Result<usize>> + Send {
        HookServicesExt::rehydrate_hooks(self)
    }

    fn spawn_script_output_sweep_loop(&self) -> tokio::task::JoinHandle<()> {
        ScriptOutputCleanupExt::spawn_script_output_sweep_loop(self)
    }

    fn spawn_execution_context_loop(&self) -> tokio::task::JoinHandle<()> {
        HostExecutionExt::spawn_execution_context_loop(self)
    }
}

/// Register the in-crate runtime feature: its state and its port.
#[must_use]
pub fn register(services: Services) -> Services {
    let feature = Arc::new(RuntimeFeature::default());
    services
        .with_feature(Arc::clone(&feature))
        .with_runtime_port(feature)
}

impl RuntimePort for RuntimeFeature {
    fn script_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_workspace_operator(&workspace_id, "script.list")
                .await?;
            mgr.list(&workspace_id).await
        })
    }

    fn script_list_filtered<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        archive: intent_core::ScriptArchiveFilter,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_workspace_operator(&workspace_id, "script.list")
                .await?;
            mgr.list_filtered(&workspace_id, archive).await
        })
    }

    fn script_list_view<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_workspace_operator(&workspace_id, "script.list")
                .await?;
            let options = intent_core::script_lists::ScriptListOptions::parse(&options, false)?;
            if options.summary {
                svc.script_manager()
                    .summary_page(&workspace_id, &options)
                    .await
            } else {
                svc.script_manager().list(&workspace_id).await
            }
        })
    }

    fn script_get<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.get")
                .await?;
            let mgr = svc.script_manager();
            let _read = mgr.scoped_read_guard(&workspace_id, &script_id).await?;
            mgr.get(&workspace_id, &script_id).await
        })
    }

    fn script_monitor_list_view<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_member(&workspace_id).await?;
            let options = intent_core::script_lists::ScriptListOptions::parse(&options, true)?;
            if options.summary {
                svc.store()
                    .script_summary_page(&workspace_id, agent_id.as_ref(), true, &options)
                    .await
            } else {
                svc.script_monitor_list(workspace_id, agent_id).await
            }
        })
    }

    fn script_monitor_get<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        monitor_id: String,
        owner: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_member(&workspace_id).await?;
            let row = svc
                .store()
                .script_monitor(&workspace_id, &monitor_id)
                .await?;
            if owner.is_some_and(|id| id != row.agent_id) {
                return Err(Error::InvalidParams("unknown monitor ID".into()));
            }
            serde_json::to_value(row).map_err(|e| Error::Internal(e.to_string()))
        })
    }

    fn script_archive<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "script.archive")
                .await?;
            svc.require_member(&workspace_id).await?;
            mgr.archive(&workspace_id, script_ids, true).await
        })
    }

    fn script_restore<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "script.restore")
                .await?;
            svc.require_member(&workspace_id).await?;
            mgr.archive(&workspace_id, script_ids, false).await
        })
    }

    fn script_create<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "script.create")
                .await?;
            // Script definitions have no workspace foreign key; refuse member
            // creation for a missing workspace before persisting or registering.
            svc.require_member(&workspace_id).await?;
            if capability::gated_collaborator_caller("script.create")?.is_some() {
                if let Some(id) = params.script_id.as_deref() {
                    if svc
                        .store()
                        .script_workspace(id)
                        .await?
                        .is_some_and(|actual| actual != workspace_id)
                    {
                        return Err(Error::NotFound(format!("script {id}")));
                    }
                }
                return mgr.create_in_workspace(workspace_id, params).await;
            }
            mgr.create(workspace_id, params).await
        })
    }

    fn script_remove<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "script.remove")
                .await?;
            svc.require_member(&workspace_id).await?;
            if capability::gated_collaborator_caller("script.remove")?.is_some() {
                return mgr.remove_in_workspace(&workspace_id, &script_id).await;
            }
            mgr.remove(&workspace_id, &script_id).await
        })
    }

    fn script_monitor<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        script_id: String,
        options: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            let _admission = svc.feature_workspace_mutations().enter(&workspace_id)?;
            svc.require_member(&workspace_id).await?;
            let _archive = svc.feature_archive_fence().acquire(&workspace_id).await;
            let retirement = svc.feature_agent_wake().retirement_gate(svc, &agent_id);
            let _retirement = retirement.lock().await;
            let key = (workspace_id.clone(), script_id.clone());
            let auto = self.auto_start_monitors.lock().unwrap().get(&key).cloned();
            let reply = svc
                .script_manager()
                .monitor_replacing(
                    &workspace_id,
                    &agent_id,
                    &script_id,
                    options,
                    auto.as_deref(),
                )
                .await?;
            // Once replaced, the default is gone; a stale marker (its monitor
            // already settled) never matches an active row and is harmless.
            if let Some(auto) = auto {
                if reply["ok"] == true && reply["monitor"]["monitorId"].as_str() != Some(&auto) {
                    let mut autos = self.auto_start_monitors.lock().unwrap();
                    if autos.get(&key) == Some(&auto) {
                        autos.remove(&key);
                    }
                }
            }
            Ok(reply)
        })
    }

    fn script_monitor_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_member(&workspace_id).await?;
            Ok(
                serde_json::json!({"monitors":svc.store().script_monitors(&workspace_id,agent_id.as_ref()).await?}),
            )
        })
    }

    fn script_monitor_cancel<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        monitor_id: String,
        owner: Option<AgentId>,
        stop_run: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "script.stop")
                .await?;
            svc.require_member(&workspace_id).await?;
            svc.script_manager()
                .cancel_monitor(&workspace_id, &monitor_id, owner.as_ref(), stop_run)
                .await
        })
    }

    fn script_start<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.start")
                .await?;
            mgr.start(&workspace_id, &script_id).await
        })
    }

    fn script_start_monitor<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        script_id: String,
        run_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            match svc
                .store()
                .get_script_in_workspace(&workspace_id, &script_id)
                .await?
            {
                Some(def) if def.mode == intent_core::ScriptMode::Command => {}
                _ => return Ok(serde_json::Value::Null),
            }
            let existing = svc
                .active_script_monitor_ids(&workspace_id, &script_id)
                .await?;
            let reply = svc
                .script_monitor(
                    workspace_id.clone(),
                    agent_id,
                    script_id.clone(),
                    serde_json::json!({"ttlMs": script_ops::AUTO_START_MONITOR_TTL_MS, "runId": run_id}),
                )
                .await?;
            // Remember only a monitor this call created: one the agent armed
            // itself (or another owner's) must never be replaced later.
            if let Some(id) = reply["monitor"]["monitorId"].as_str() {
                if reply["ok"] == true && !existing.iter().any(|e| e == id) {
                    self.auto_start_monitors
                        .lock()
                        .unwrap()
                        .insert((workspace_id, script_id), id.to_owned());
                }
            }
            Ok(reply)
        })
    }

    fn script_stop<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.stop")
                .await?;
            mgr.stop(&workspace_id, &script_id).await
        })
    }

    fn script_restart<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.restart")
                .await?;
            mgr.restart(&workspace_id, &script_id).await
        })
    }

    fn script_output<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.output")
                .await?;
            mgr.ensure_known(&workspace_id, &script_id).await?;
            let _read = mgr.scoped_read_guard(&workspace_id, &script_id).await?;
            mgr.output(
                &workspace_id,
                &script_id,
                max_lines,
                paginate.unwrap_or(false),
                page_token.as_ref(),
            )
            .await
        })
    }

    fn script_status<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.status")
                .await?;
            mgr.ensure_known(&workspace_id, &script_id).await?;
            let _read = mgr.scoped_read_guard(&workspace_id, &script_id).await?;
            mgr.status(&workspace_id, &script_id)
        })
    }

    fn script_run<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let mgr = svc.script_manager();
        Box::pin(async move {
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.run")
                .await?;
            let result = mgr
                .run(&workspace_id, &script_id, max_lines, timeout_seconds)
                .await?;
            // A command may finish after the owner downgraded this open socket.
            svc.require_saved_script_operator(&workspace_id, &script_id, "script.run")
                .await?;
            Ok(result)
        })
    }

    /// `hook.schedule`: validate + immediate real run + persist + spawn the
    /// per-hook scheduler task (see `hook_manager`).
    fn hook_schedule<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        params: serde_json::Value,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_agent_member(&agent_id).await?;
            svc.hook_schedule_op(&workspace_id, &agent_id, &params)
                .await
        })
    }

    /// `hook.list`: hooks in a workspace, optionally one agent's; active
    /// only by default, retired rows as a light projection on request.
    fn hook_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        include_retired: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            // The workspace is the authorization scope; the agent filter is
            // just a filter (the wire arm always passes `None`).
            svc.require_member(&workspace_id).await?;
            svc.hook_list_op(&workspace_id, agent_id.as_ref(), include_retired)
                .await
        })
    }

    /// `hook.get` (MCP-only): one hook row (including `code`) by id, active
    /// or terminal; hooks from other workspaces read as `NotFound`.
    fn hook_get<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_member(&workspace_id).await?;
            svc.hook_get_op(&workspace_id, &hook_id).await
        })
    }

    /// `hook.cancel`: stop an active hook; an agent caller may only cancel
    /// its own hooks, while the FE path (`caller = None`) cancels any hook
    /// and wakes the owning agent with a notice.
    fn hook_cancel<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
        caller: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "hook.cancel")
                .await?;
            svc.hook_cancel_op(&workspace_id, &hook_id, caller.as_ref())
                .await
        })
    }

    /// `hook.runNow`: trigger an immediate run of an active hook.
    fn hook_run_now<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        hook_id: intent_core::HookId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "hook.runNow")
                .await?;
            svc.hook_run_now_op(&workspace_id, &hook_id).await
        })
    }

    fn terminal_create<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
        env: Option<std::collections::BTreeMap<String, String>>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let pty = svc.pty();
        let bus = svc.feature_event_bus().cloned();
        let store = svc.store().clone();
        let settings = svc.settings_registry();
        Box::pin(async move {
            // Workspace terminals use the host owner's execution setup.
            svc.require_workspace_manager(&workspace_id, "terminal.create")
                .await?;
            // Management admission also permits deletion retries for missing
            // workspaces; a member must not spawn an unmanageable terminal there.
            svc.require_member(&workspace_id).await?;
            // Held from the retired check through the spawn: retire flips
            // the row and sweeps the workspace's PTYs under the same fence,
            // so a create admitted before the flip finishes spawning before
            // the sweep lists terminals, and one after it is refused.
            let _archive = svc.feature_archive_fence().acquire(&workspace_id).await;
            svc.refuse_if_retired(&workspace_id).await?;
            terminal_ops::create_owned(
                svc.feature_terminal_tasks(),
                pty,
                bus,
                Some(store),
                settings,
                workspace_id,
                cols,
                rows,
                cwd,
                command,
                env,
            )
            .await
        })
    }

    fn terminal_write<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
        data: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let pty = svc.pty();
        Box::pin(async move {
            // Resolve the terminal's actual workspace before authorizing it.
            svc.require_terminal_manager(&terminal_id, "terminal.write")
                .await?;
            svc.refuse_terminal_if_retired(&terminal_id).await?;
            terminal_ops::write(&pty, &terminal_id, &data)
        })
    }

    fn terminal_resize<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
        cols: u16,
        rows: u16,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let pty = svc.pty();
        Box::pin(async move {
            svc.require_terminal_manager(&terminal_id, "terminal.resize")
                .await?;
            svc.refuse_terminal_if_retired(&terminal_id).await?;
            terminal_ops::resize(&pty, &terminal_id, cols, rows)
        })
    }

    fn terminal_kill<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let pty = svc.pty();
        Box::pin(async move {
            svc.require_terminal_manager(&terminal_id, "terminal.kill")
                .await?;
            svc.refuse_terminal_if_retired(&terminal_id).await?;
            terminal_ops::kill(&pty, &terminal_id).await
        })
    }

    fn terminal_get_buffer<'a>(
        &'a self,
        svc: &'a Services,
        terminal_id: String,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let pty = svc.pty();
        Box::pin(async move {
            svc.require_terminal_manager(&terminal_id, "terminal.getBuffer")
                .await?;
            terminal_ops::get_buffer(&pty, &terminal_id, max_bytes)
        })
    }

    fn terminal_list<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let pty = svc.pty();
        let boot_id = pty.daemon_boot_id().to_string();
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "terminal.list")
                .await?;
            terminal_ops::list(&pty, &workspace_id, &boot_id)
        })
    }

    fn terminal_read_output<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        terminal_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let pty = svc.pty();
        Box::pin(async move {
            svc.require_workspace_manager(&workspace_id, "terminal.readOutput")
                .await?;
            terminal_ops::read_output(
                &pty,
                &workspace_id,
                &terminal_id,
                max_lines,
                paginate.unwrap_or(false),
                page_token.as_ref(),
            )
        })
    }

    fn browser_list_tabs<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'a, Result<Vec<BrowserTab>>> {
        let svc = svc.clone();
        Box::pin(async move {
            svc.require_member(&workspace_id).await?;
            svc.browser_tabs_list(workspace_id).await
        })
    }

    fn browser_upsert_tab<'a>(
        &'a self,
        svc: &'a Services,
        host: ClientId,
        tab: BrowserTabInput,
    ) -> BoxFuture<'a, Result<BrowserTab>> {
        let svc = svc.clone();
        Box::pin(async move { svc.browser_tab_upsert(host, tab).await })
    }

    fn browser_remove_tab<'a>(
        &'a self,
        svc: &'a Services,
        host: ClientId,
        tab_id: String,
    ) -> BoxFuture<'a, Result<()>> {
        let svc = svc.clone();
        Box::pin(async move { svc.browser_tab_remove(host, tab_id).await })
    }

    fn browser_sync_tabs<'a>(
        &'a self,
        svc: &'a Services,
        host: ClientId,
        tabs: Vec<BrowserTabInput>,
    ) -> BoxFuture<'a, Result<Vec<String>>> {
        let svc = svc.clone();
        Box::pin(async move { svc.browser_tabs_sync(host, tabs).await })
    }

    /// `browser.exec` — agent-initiated CDP forward (REV-2, PROTOCOL §5.14/§12.4).
    ///
    /// When an agent triggers this via the MCP `ws.browser.exec` binding
    /// there is no per-connection reverse channel to use (the caller is the
    /// daemon-hosted MCP server, not a client connection), so we route the
    /// batch through the injected [`AgentReverseDispatch`]. The target is
    /// the workspace's **driving client** ([`Services::driving_client_target`],
    /// REV-2 rules 2–5: pin → client of an active desktop session → host of
    /// the workspace's claimed tabs → first-connected eligible client); a
    /// driving client that is offline is
    /// a hard `-32603` ("browser client … for this workspace is not
    /// connected"), never a silent fallback. Two rules run before dispatch:
    /// every `tabId` the batch names (envelope or per action) must be an
    /// open registry tab of this workspace (`-32602` "tab not found"), and
    /// `listTabs` is answered by the daemon from the registry — all hosts
    /// aggregated, no reverse call — so a `listTabs` batch never mixes with
    /// FE-executed actions (`-32602`). After a successful dispatch, a
    /// `claimTab` that succeeded re-homes the row to the driving client as
    /// re-resolved at commit time (`browser:tab-updated { changes: {
    /// hostClientId, ownerAgentId } }`, see
    /// [`Services::browser_tab_claim_migrate`](crate::browser_tabs::BrowserTabRoutingExt::browser_tab_claim_migrate)).
    /// Attribution fields (`workspaceId`, `agentId`,
    /// `tabId`) are threaded into the forwarded params so the FE sees the
    /// same envelope shape the client-triggered path already emits. Result
    /// shaping stays in [`browser_ops`], via the agent-surface variant
    /// [`browser_ops::shape_agent_result`]: structured per-action failures
    /// (`not-owner` / `already-claimed`) are preserved as data instead of
    /// flattening into a top-level error (intent-hq/monorepo#3042).
    /// Remaining failure modes surface as `Error::Internal` with the
    /// underlying reason so the MCP caller can distinguish "no client
    /// connected" from a proxy-side failure. An empty `actions` batch is
    /// rejected with
    /// `Error::InvalidParams` (JSON-RPC `-32602`) before any dispatch — the
    /// FE tool has no meaningful behavior on an empty batch and the
    /// client-triggered path already enforces the same guard. When no
    /// dispatcher is wired (unit tests, read-only wiring) the request
    /// short-circuits with `no client connected`.
    fn browser_exec<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        actions: Vec<serde_json::Value>,
        tab_id: Option<String>,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            if let Some(agent) = agent_id.as_ref() {
                svc.require_agent_member(agent).await?;
            }
            if actions.is_empty() {
                return Err(Error::InvalidParams(
                    "browser.exec: actions must be a non-empty array".to_string(),
                ));
            }
            // Every tab the batch names must be an open registry tab of this
            // workspace — before anything is answered or dispatched; the
            // driving client is the one host, so no per-tab host lookup is
            // involved (Model 5).
            let mut named_tabs: Vec<&str> = actions
                .iter()
                .filter_map(|a| a["tabId"].as_str())
                .chain(tab_id.as_deref())
                .filter(|id| !id.is_empty())
                .collect();
            named_tabs.sort_unstable();
            named_tabs.dedup();
            for id in &named_tabs {
                let known = svc
                    .store()
                    .get_browser_tab(id)
                    .await?
                    .is_some_and(|tab| tab.workspace_id == workspace_id);
                if !known {
                    return Err(Error::InvalidParams(format!(
                        "browser.exec: tab not found: {id}"
                    )));
                }
            }
            let list_tabs_count = actions
                .iter()
                .filter(|a| a["action"] == browser_tabs::LIST_TABS_ACTION)
                .count();
            if list_tabs_count > 0 {
                if list_tabs_count != actions.len() {
                    return Err(Error::InvalidParams(
                        "browser.exec: listTabs is answered by the daemon and cannot be batched \
                         with other actions"
                            .to_string(),
                    ));
                }
                let mut results = Vec::with_capacity(actions.len());
                for action in &actions {
                    let scope = match &action["scope"] {
                        serde_json::Value::Null => None,
                        serde_json::Value::String(scope) => Some(scope.as_str()),
                        other => {
                            return Err(Error::InvalidParams(format!(
                                "browser.exec: listTabs scope must be \"mine\", \"unclaimed\" or \
                                 \"all\", got {other}"
                            )))
                        }
                    };
                    results.push(
                        svc.browser_tabs_list_for_agent(&workspace_id, scope, agent_id.as_ref())
                            .await?,
                    );
                }
                return Ok(if results.len() == 1 {
                    results.remove(0)
                } else {
                    serde_json::json!({ "results": results })
                });
            }
            let Some(dispatch) = svc.feature_reverse_dispatch().cloned() else {
                return Err(Error::Internal(
                    "browser.exec: no client connected".to_string(),
                ));
            };
            let target = svc.driving_client_target(&workspace_id).await?;
            // Claims re-home their tab after the dispatch; record which
            // client executed them (the commit re-validates the host).
            let claims: Vec<String> = actions
                .iter()
                .filter(|a| a["action"] == "claimTab")
                .filter_map(|a| a["tabId"].as_str().or(tab_id.as_deref()))
                .map(str::to_string)
                .collect();
            let executed_on = if claims.is_empty() {
                None
            } else {
                dispatch.resolve(&target).ok().map(|r| r.client_id)
            };
            // Shape by *request* arity: the FE aborts a batch on the first
            // failing action, so the reply's results count can be shorter
            // than the batch (see `shape_agent_result`).
            let requested_actions = actions.len();
            let args = browser_ops::BrowserExecArgs {
                actions,
                tab_id,
                agent_id: agent_id.as_ref().map(|a| a.as_str().to_string()),
                workspace_id: Some(workspace_id.as_str().to_string()),
            };
            let forwarded = browser_ops::build_forward_params(&args);
            let response = match dispatch.dispatch("browser.exec", forwarded, target).await {
                Ok(response) => response,
                Err(ReverseDispatchError::NoClient) => {
                    return Err(Error::Internal(
                        "browser.exec: no client connected".to_string(),
                    ));
                }
                Err(err) => {
                    let message = svc.browser_dispatch_error_message(err).await;
                    return Err(Error::Internal(format!("browser.exec: {message}")));
                }
            };
            if let Some(executed_on) = executed_on {
                for tab_id in browser_ops::successful_claims(&response, &claims) {
                    svc.browser_tab_claim_migrate(&tab_id, &executed_on, agent_id.as_ref())
                        .await?;
                }
            }
            browser_ops::shape_agent_result(&response, requested_actions)
                .map_err(|e| Error::Internal(e.message))
        })
    }

    /// `browser.navigateTab { tabId, url }` (REV-2 Model 4): route a
    /// navigation request to the client that renders the live tab — the
    /// workspace's driving client for a claimed tab, the physical host for
    /// an unclaimed one — as a reverse `browser.exec { action: "navigate"
    /// }` and echo that action's result envelope. The host then reports the
    /// resulting navigation through `browser.upsertTab` and every client
    /// follows the canonical row.
    fn browser_dispatch_error_message<'a>(
        &'a self,
        svc: &'a Services,
        err: ReverseDispatchError,
    ) -> BoxFuture<'a, String> {
        Box::pin(async move { svc.browser_dispatch_error_message(err).await })
    }

    fn browser_navigate_tab<'a>(
        &'a self,
        svc: &'a Services,
        tab_id: String,
        url: String,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            let tab = svc
                .browser_tab_required("browser.navigateTab", &tab_id)
                .await?;
            let response = svc
                .browser_tab_dispatch(
                    "browser.navigateTab",
                    &tab,
                    serde_json::json!({ "action": "navigate", "tabId": tab_id, "url": url }),
                )
                .await?;
            browser_ops::shape_result(&response).map_err(|e| Error::Internal(e.message))
        })
    }

    /// `browser.closeTab { tabId, force? }` (REV-2 Model 6): route the close
    /// to the tab's routing target (as `browser_navigate_tab`) so the host's
    /// own `browser.removeTab` deletes the row. With `force` the row is
    /// tombstoned daemon-side regardless (after a best-effort routed close
    /// when the target is reachable); a target that is offline without
    /// `force` is `Error::Internal`. Unknown tab ⇒ `Error::InvalidParams`.
    fn browser_close_tab<'a>(
        &'a self,
        svc: &'a Services,
        tab_id: String,
        force: bool,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        Box::pin(async move {
            let tab = svc
                .browser_tab_required("browser.closeTab", &tab_id)
                .await?;
            let action = serde_json::json!({ "action": "closeTab", "tabId": tab_id });
            if force {
                let target = svc.browser_tab_route_target(&tab).await?;
                let reachable = svc
                    .feature_reverse_dispatch()
                    .is_some_and(|d| d.resolve(&target).is_ok());
                if reachable {
                    // The host is told directly; failures fall through to
                    // the tombstone, which its next sync reconciles.
                    let _ = svc
                        .browser_tab_dispatch("browser.closeTab", &tab, action)
                        .await;
                }
                svc.browser_tab_force_close(&tab_id).await?;
                return Ok(serde_json::json!({ "ok": true }));
            }
            let response = svc
                .browser_tab_dispatch("browser.closeTab", &tab, action)
                .await?;
            browser_ops::shape_result(&response).map_err(|e| Error::Internal(e.message))?;
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    fn sandbox_merge<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let store = svc.store().clone();
        Box::pin(async move {
            use crate::sandbox_ops::{merge_sandbox, MergeOutcome};
            use intent_store::SandboxStatus;

            svc.require_agent_member(&sandbox_id).await?;

            // Claim the merge atomically (current status → merging) so the RPC
            // never merges concurrently with the completion path or the
            // background retry sweep — every merge path goes through the same
            // compare-and-swap claim protocol.
            let sandbox = store
                .get_sandbox(&workspace_id, &sandbox_id)
                .await?
                .ok_or_else(|| Error::Internal("Sandbox not found".to_string()))?;
            if sandbox.status == SandboxStatus::Merging {
                return Err(Error::Internal(
                    "Sandbox merge already in progress".to_string(),
                ));
            }
            let claimed = store
                .try_transition_sandbox_status(
                    &workspace_id,
                    &sandbox_id,
                    sandbox.status,
                    SandboxStatus::Merging,
                    &now_iso(),
                )
                .await?;
            if !claimed {
                return Err(Error::Internal(
                    "Sandbox merge already in progress".to_string(),
                ));
            }

            // Attempt merge. On a hard error return the sandbox to
            // merge_pending so it stays visible to the retry sweep and
            // retryable via this RPC rather than stranded `merging`.
            let outcome = match merge_sandbox(&store, &workspace_id, &sandbox_id).await {
                Ok(outcome) => outcome,
                Err(e) => {
                    let _ = store
                        .update_sandbox_status(
                            &workspace_id,
                            &sandbox_id,
                            SandboxStatus::MergePending,
                            &now_iso(),
                        )
                        .await;
                    return Err(e);
                }
            };

            match outcome {
                MergeOutcome::Merged {
                    commit_range,
                    canonical_head,
                } => {
                    // Shared bookkeeping: mark merged, discard sandbox, emit
                    // sandbox:cow:merged, clear retry count.
                    svc.finalize_sandbox_merged(
                        &workspace_id,
                        &sandbox_id,
                        &commit_range,
                        &canonical_head,
                    )
                    .await;

                    Ok(serde_json::json!({
                        "ok": true,
                        "status": "merged",
                        "commitRange": commit_range,
                        "canonicalHead": canonical_head,
                    }))
                }
                MergeOutcome::Conflict {
                    conflicting_paths,
                    canonical_head,
                } => {
                    let _ = store
                        .update_sandbox_status(
                            &workspace_id,
                            &sandbox_id,
                            SandboxStatus::ConflictBounced,
                            &now_iso(),
                        )
                        .await;

                    Ok(serde_json::json!({
                        "ok": true,
                        "status": "conflict",
                        "conflictingPaths": conflicting_paths,
                        "canonicalHead": canonical_head,
                    }))
                }
                MergeOutcome::Blocked {
                    reason,
                    overlapping_paths,
                } => {
                    let _ = store
                        .update_sandbox_status(
                            &workspace_id,
                            &sandbox_id,
                            SandboxStatus::MergePending,
                            &now_iso(),
                        )
                        .await;

                    Ok(serde_json::json!({
                        "ok": true,
                        "status": "blocked",
                        "reason": reason,
                        "overlappingPaths": overlapping_paths,
                    }))
                }
            }
        })
    }

    fn sandbox_discard<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'a, Result<serde_json::Value>> {
        let store = svc.store().clone();
        Box::pin(async move {
            svc.require_agent_member(&sandbox_id).await?;
            crate::sandbox_ops::discard_sandbox(&store, &workspace_id, &sandbox_id).await?;
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    fn migrate_claimed_browser_tabs<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        new_host: &'a ClientId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(svc.browser_tabs_migrate_claimed(workspace_id, new_host))
    }

    fn rehome_superseded_browser_tabs<'a>(
        &'a self,
        svc: &'a Services,
        client_id: ClientId,
        superseded: Vec<ClientId>,
    ) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            svc.browser_tabs_rehome_superseded(&client_id, superseded)
                .await
        })
    }

    fn provision_sandbox<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
        workspaces_root: PathBuf,
    ) -> BoxFuture<'a, Result<ProvisionOutcome>> {
        Box::pin(async move {
            let config = sandbox_ops::ProvisionConfig { workspaces_root };
            sandbox_ops::provision_sandbox(svc.store(), workspace_id, agent_id, &config).await
        })
    }

    fn merge_sandbox<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<MergeOutcome>> {
        Box::pin(sandbox_ops::merge_sandbox(
            svc.store(),
            workspace_id,
            agent_id,
        ))
    }

    fn discard_sandbox<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(sandbox_ops::discard_sandbox(
            svc.store(),
            workspace_id,
            agent_id,
        ))
    }

    fn run_setup_script(
        &self,
        svc: &Services,
        run: SetupScriptRun,
    ) -> BoxFuture<'static, (bool, Option<u32>)> {
        let SetupScriptRun {
            workspace_id,
            worktree: worktree_for_read,
            script,
            repo_path,
            branch_name,
            source_branch,
        } = run;
        let worktree_path = worktree_for_read.to_string_lossy().to_string();
        let pty_for_setup = svc.pty();
        let bus_for_setup = svc.feature_event_bus().cloned();
        let setup_states = svc.feature_workspace_setup_states().clone();
        let terminal_tasks = svc.feature_terminal_tasks().clone();
        Box::pin(async move {
            'setup: {
                // Write script to a private file under worktree .intent/ directory
                // (mode 0600 on Unix, safe from other users, isolated from /tmp races).
                let script_id = uuid::Uuid::new_v4();
                let intent_dir = worktree_for_read.join(".intent");
                // Security: refuse if .intent exists as a symlink (prevents writing
                // script outside the worktree via a repo-committed symlink attack).
                if let Ok(meta) = tokio::fs::symlink_metadata(&intent_dir).await {
                    if !meta.is_dir() {
                        tracing::warn!(
                            workspace = %workspace_id.as_str(),
                            "setup script execution skipped: .intent is not a real directory (symlink or file)"
                        );
                        break 'setup (false, None);
                    }
                } else {
                    // .intent doesn't exist, create it with restrictive permissions
                    if let Err(e) = tokio::fs::create_dir_all(&intent_dir).await {
                        tracing::warn!(
                            workspace = %workspace_id.as_str(),
                            error = %e,
                            "failed to create .intent directory for setup script"
                        );
                        break 'setup (false, None);
                    }
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        if let Err(e) = tokio::fs::set_permissions(
                            &intent_dir,
                            std::fs::Permissions::from_mode(0o700),
                        )
                        .await
                        {
                            tracing::warn!(
                                workspace = %workspace_id.as_str(),
                                error = %e,
                                "failed to set .intent directory permissions"
                            );
                        }
                    }
                }
                // Select the platform runner: POSIX always spawns
                // absolute /bin/sh + the inline timing wrapper
                // (byte-identical to the historic spawn); Windows
                // prefers a discovered Git-for-Windows sh.exe
                // running the same wrapper (so bash setup scripts
                // keep working), else falls back to a cmd.exe
                // `.cmd` wrapper with the same summary format.
                let runner = setup_runner::select(
                    cfg!(windows),
                    if cfg!(windows) {
                        setup_runner::find_windows_sh()
                    } else {
                        None
                    },
                );
                let script_path = intent_dir.join(format!(
                    "setup-{}.{}",
                    script_id.simple(),
                    runner.script_extension()
                ));
                if let Err(e) = write_private_script(&script_path, &script).await {
                    tracing::warn!(
                        workspace = %workspace_id.as_str(),
                        error = %e,
                        "failed to write setup script to private file"
                    );
                    break 'setup (false, None);
                }
                // cmd.exe fallback only: the timing wrapper is a
                // sibling .cmd file rather than an inline `-c`
                // string (cmd has no equivalent of `sh -c`).
                let wrapper_path = if runner.needs_cmd_wrapper_file() {
                    let p = intent_dir.join(format!("setup-wrapper-{}.cmd", script_id.simple()));
                    if let Err(e) =
                        write_private_script(&p, setup_runner::SETUP_SCRIPT_WRAPPER_CMD).await
                    {
                        tracing::warn!(
                            workspace = %workspace_id.as_str(),
                            error = %e,
                            "failed to write setup script cmd wrapper"
                        );
                        let _ = tokio::fs::remove_file(&script_path).await;
                        break 'setup (false, None);
                    }
                    Some(p)
                } else {
                    None
                };
                // Spawn through the timing wrapper so the scrollback
                // ends with a completion summary and the script's
                // exit code is preserved. `extra_env` carries the
                // cmd.exe fallback's script path (see
                // `SetupScriptRunner::command`); empty otherwise.
                let (program, args, extra_env) =
                    runner.command(&script_path, wrapper_path.as_deref());
                let mut spec = intent_pty::SpawnSpec::new(workspace_id.as_str(), program);
                spec.args = args;
                spec.size = intent_pty::PtySize { rows: 24, cols: 80 };
                spec.name = Some(SETUP_TERMINAL_NAME.to_string());
                spec.cwd = Some(worktree_for_read.clone());
                spec.env = vec![
                    ("MAIN_CHECKOUT".to_string(), repo_path),
                    ("WORKTREE_PATH".to_string(), worktree_path.clone()),
                    ("BRANCH_NAME".to_string(), branch_name),
                    ("SOURCE_BRANCH".to_string(), source_branch),
                ];
                spec.env.extend(extra_env);
                match pty_for_setup.spawn(spec) {
                    Ok(pty_id) => {
                        let terminal_id = pty_id.to_string();
                        tracing::info!(
                            workspace = %workspace_id.as_str(),
                            terminal_id = %terminal_id,
                            "setup script terminal spawned"
                        );
                        record_setup_running(&setup_states, &workspace_id, &terminal_id);
                        // Spawn output stream to fan setup script output to event bus
                        crate::terminal_ops::spawn_output_stream(
                            &terminal_tasks,
                            pty_for_setup.clone(),
                            bus_for_setup.clone(),
                            workspace_id.clone(),
                            pty_id,
                            terminal_id,
                        );
                        // Wait for the script to actually complete before cleanup
                        let exit = pty_for_setup.wait(pty_id).await;
                        // Best-effort cleanup of the script + wrapper files (after exit)
                        let _ = tokio::fs::remove_file(&script_path).await;
                        if let Some(w) = &wrapper_path {
                            let _ = tokio::fs::remove_file(w).await;
                        }
                        (true, exit.ok().filter(|e| e.observed).map(|e| e.exit_code))
                    }
                    Err(e) => {
                        tracing::warn!(
                            workspace = %workspace_id.as_str(),
                            error = %e,
                            "failed to spawn setup script terminal"
                        );
                        // Cleanup on spawn failure
                        let _ = tokio::fs::remove_file(&script_path).await;
                        if let Some(w) = &wrapper_path {
                            let _ = tokio::fs::remove_file(w).await;
                        }
                        (false, None)
                    }
                }
            }
        })
    }

    fn active_hooks_for_agent<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<serde_json::Value>> {
        Box::pin(svc.active_hooks_for_agent(agent_id))
    }

    fn active_hooks_by_agent<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, HashMap<String, Vec<serde_json::Value>>> {
        Box::pin(svc.active_hooks_by_agent(workspace_id))
    }

    fn annotate_waiting_on_hooks<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        data: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, Vec<serde_json::Value>> {
        Box::pin(svc.annotate_waiting_on_hooks(agent_id, data))
    }

    fn cancel_agent_hooks<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(svc.cancel_agent_hooks(agent_id))
    }

    fn active_script_monitors_for_agent<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<serde_json::Value>> {
        Box::pin(svc.active_script_monitors_for_agent(agent_id))
    }

    fn annotate_waiting_on_script_monitors<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        data: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, ()> {
        Box::pin(svc.annotate_waiting_on_script_monitors(agent_id, data))
    }

    fn cancel_script_monitors<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: Option<&'a AgentId>,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(svc.cancel_script_monitors(workspace_id, agent_id, reason))
    }

    fn script_monitor_export_blocked(
        &self,
        svc: &Services,
        metadata: Option<&serde_json::Value>,
    ) -> bool {
        svc.script_monitor_export_blocked(metadata)
    }

    fn defer_script_monitor_for_export(
        &self,
        svc: &Services,
        agent_id: &AgentId,
        content: &str,
        metadata: Option<&serde_json::Value>,
    ) -> bool {
        svc.defer_script_monitor_for_export(agent_id, content, metadata)
    }

    fn track_script_monitor_worker(
        &self,
        svc: &Services,
        agent_id: &AgentId,
        metadata: Option<&serde_json::Value>,
    ) {
        svc.track_script_monitor_worker(agent_id, metadata);
    }

    fn settle_script_monitor_worker<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(svc.settle_script_monitor_worker(agent_id))
    }

    fn admit_script_monitor_turn<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(svc.admit_script_monitor_turn(agent_id, metadata))
    }

    fn script_monitor_delivery_allowed<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        metadata: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(svc.script_monitor_delivery_allowed(agent_id, metadata))
    }

    fn complete_script_monitor_delivery<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        monitor_id: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(svc.complete_script_monitor_delivery(agent_id, monitor_id))
    }

    fn script_monitor_lane<'a>(&'a self, svc: &'a Services) -> &'a tokio::sync::Mutex<()> {
        &svc.runtime().script_locks.monitor_lane
    }

    fn close_workspace_scripts<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, Box<dyn ScriptsClosing>> {
        Box::pin(async move {
            Box::new(svc.script_manager().close_workspace(workspace_id).await)
                as Box<dyn ScriptsClosing>
        })
    }

    fn hydrate_scripts<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<usize>> {
        Box::pin(async move {
            svc.script_manager().recover_monitors().await?;
            svc.start_script_monitor_maintenance();
            svc.script_manager().hydrate().await
        })
    }

    fn recover_imported_scripts<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        script_ids: &'a [String],
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Resolve imported monitor deadlines/results before compact run
            // recovery manufactures an interruption. No source process is
            // resumed here.
            if let Err(error) = svc.script_manager().recover_monitors().await {
                tracing::warn!(%error, "post-import script monitor recovery failed");
            }
            svc.start_script_monitor_maintenance();
            if !script_ids.is_empty() {
                let manager = svc.script_manager();
                for id in script_ids {
                    if let Err(error) = manager.refresh_imported(workspace_id, id).await {
                        tracing::warn!(script = %id, %error, "post-import script refresh failed");
                    }
                }
            }
        })
    }

    fn rehydrate_hooks<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<usize>> {
        Box::pin(HookServicesExt::rehydrate_hooks(svc))
    }

    fn command_script_activity(&self, svc: &Services) -> CommandScriptActivity {
        script_ops::command_activity(&svc.runtime().scripts, &svc.runtime().script_locks)
    }

    fn workspace_scripts_running(&self, svc: &Services, workspace_id: &WorkspaceId) -> bool {
        script_ops::workspace_command_running(&svc.runtime().scripts, workspace_id)
    }

    fn stop_all_scripts<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, (usize, usize)> {
        Box::pin(async move { svc.script_manager().stop_all().await })
    }

    fn clear_hook_tasks(&self, svc: &Services) {
        svc.runtime().hook_tasks.lock().unwrap().clear();
    }

    fn abort_workspace_hook_tasks<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(svc.abort_workspace_hook_tasks(workspace_id))
    }

    fn cancel_workspace_hooks<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, BTreeMap<AgentId, Vec<(String, HookId)>>> {
        Box::pin(svc.cancel_workspace_hooks(workspace_id))
    }

    fn delete_workspace_script_output<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, bool> {
        Box::pin(svc.delete_workspace_script_output(workspace_id))
    }

    fn execution_context_snapshot<'a>(
        &'a self,
        svc: &'a Services,
    ) -> BoxFuture<'a, Result<HostExecutionContext>> {
        Box::pin(svc.execution_context_snapshot())
    }

    fn agent_terminal_host(
        &self,
        svc: &Services,
        terminal_requires_shell: bool,
        session_cwd: Option<PathBuf>,
    ) -> Arc<dyn intent_acp::TerminalHost> {
        Arc::new(terminal_ops::PtyTerminalHost::with_shell_mode(
            svc.pty(),
            svc.settings_registry(),
            terminal_requires_shell,
            session_cwd,
        ))
    }
}

impl ScriptsClosing for script_ops::WorkspaceClosing {
    fn unsettled(&self) -> &[String] {
        script_ops::WorkspaceClosing::unsettled(self)
    }
}

#[cfg(test)]
mod archive_sweep_golden_tests;

#[cfg(test)]
mod guest_role_tests;

#[cfg(test)]
mod hook_golden_tests;

#[cfg(test)]
mod host_role_tests;

#[cfg(test)]
mod retire_workspace_tests;

#[cfg(test)]
mod sandbox_file_write_tests;

#[cfg(test)]
mod script_monitor_cancel_race_tests;

#[cfg(all(test, unix))]
mod setup_lifecycle_tests;

#[cfg(test)]
mod test_fixtures;

#[cfg(test)]
mod workspace_delete_tests;
