//! The in-crate runtime feature: the `script.*` and `hook.*` RPCs behind
//! [`RuntimePort`], and the shared script/hook registries they own.
//!
//! Core reaches the feature only through the port (the `WorkspaceApi`
//! forwarders) and through [`Services::runtime`] for the registries that
//! core-resident code (the script manager view, script monitors, hook
//! scheduling, shutdown) still reads. [`Services::new`] registers it; the
//! later move into its own crate turns that registration into the
//! composition root's.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use intent_core::{
    AgentId, BoxFuture, Error, Result, ScriptCreateParams, WorkspaceApi, WorkspaceId,
};

use crate::feature_ports::RuntimePort;
use crate::{capability, hook_manager, script_ops, Services};

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
    /// table at boot via [`Services::rehydrate_hooks`].
    pub(crate) hook_tasks: hook_manager::HookTasks,
}

impl Services {
    /// The runtime feature registered by [`Services::new`].
    pub(crate) fn runtime(&self) -> &RuntimeFeature {
        self.feature::<RuntimeFeature>()
            .expect("the in-crate runtime feature is registered")
    }
}

/// Register the in-crate runtime feature: its state and its port.
pub(crate) fn register(services: Services) -> Services {
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
                svc.store
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
            let row = svc.store.script_monitor(&workspace_id, &monitor_id).await?;
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
                        .store
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
            let _admission = svc.workspace_mutations.enter(&workspace_id)?;
            svc.require_member(&workspace_id).await?;
            let _archive = svc.archive_fence.acquire(&workspace_id).await;
            let retirement = svc.agent_retirement_gates.for_agent(&agent_id);
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
                serde_json::json!({"monitors":svc.store.script_monitors(&workspace_id,agent_id.as_ref()).await?}),
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
                .store
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
}
