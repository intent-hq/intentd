//! A test double for the runtime port, for core tests whose subject is agent
//! and watch behaviour that reads runtime state (active hooks, waiting
//! script monitors) but not the runtime itself.
//!
//! It answers the hook and script-monitor questions core asks through
//! [`RuntimePort`] from state the test scripts, and forwards everything else
//! to [`UnavailableRuntime`] — what core has once the runtime feature lives
//! in its own crate. It records the port calls it answers. Real hook and
//! script-monitor behaviour is covered by the runtime's own tests and by the
//! intent-app end-to-end tests.
//!
//! - Active hooks and waiting script monitors are read from the store with
//!   the same queries the runtime uses: tests seed and end hooks by writing
//!   hook rows (or with [`FakeRuntime::cancel_hook`]), and seed monitor rows.
//! - Script-monitor admission, completion markers and notification dispatch
//!   mirror the runtime's store-level contract (the same store calls and
//!   agent-wake port calls); the runtime's in-memory locks are not modelled.
//! - Sandbox merges succeed without touching the checkout; discards remove
//!   the sandbox folder and row.
//! - Cancel sweeps on retire/archive mark the hook rows cancelled and resettle
//!   the owner through core's real completion redelivery.

use std::collections::HashSet;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_core::execution::HostExecutionContext;
use intent_core::{
    AgentId, BoxFuture, BrowserTab, BrowserTabInput, ClientId, Hook, HookId, Result,
    ScriptCreateParams, WorkspaceId,
};
use serde_json::{json, Value};

use crate::feature_ports::{
    monitor_id, wake_id, CommandScriptActivity, MergeOutcome, ProvisionOutcome, RuntimePort,
    ScriptsClosing, SetupScriptRun, UnavailableRuntime,
};
use crate::Services;

/// The runtime test double. Install it with [`FakeRuntime::install`] and
/// reach it from a test with [`FakeRuntime::of`].
#[derive(Default)]
pub(crate) struct FakeRuntime {
    calls: Mutex<Vec<&'static str>>,
    monitor_completions: Mutex<HashSet<(AgentId, String)>>,
    monitor_workers: Mutex<HashMap<AgentId, String>>,
}

static UNAVAILABLE: UnavailableRuntime = UnavailableRuntime;

impl FakeRuntime {
    /// Register a fresh fake as `services`' runtime port (and feature, so
    /// [`FakeRuntime::of`] finds it). Install before cloning `services`.
    pub(crate) fn install(services: Services) -> Services {
        let fake = Arc::new(Self::default());
        services
            .with_feature(Arc::clone(&fake))
            .with_runtime_port(fake)
    }

    /// The fake installed on `svc`.
    pub(crate) fn of(svc: &Services) -> &Self {
        svc.feature::<Self>()
            .expect("FakeRuntime::install was called on this Services")
    }

    /// Script a hook cancel the way the runtime settles one: the hook leaves
    /// the active set; an app cancel (`caller: None`) also wakes the owner;
    /// then core redelivers any completion the hook was holding back.
    pub(crate) async fn cancel_hook(
        &self,
        svc: &Services,
        hook_id: &HookId,
        caller: Option<&AgentId>,
    ) {
        self.record("cancel_hook");
        let hook = svc
            .store()
            .get_hook(hook_id)
            .await
            .expect("cancel_hook: hook row");
        mark_cancelled(svc, &hook).await;
        if caller.is_none() {
            let metadata = json!({ "hookId": hook.hook_id, "reason": "cancelled" });
            svc.feature_agent_wake()
                .deliver_wake_message(
                    svc,
                    &hook.workspace_id,
                    &hook.agent_id,
                    "[fake runtime] hook cancelled",
                    Some(&metadata),
                )
                .await
                .expect("deliver hook-cancel wake");
        }
        svc.redeliver_completion_after_queue_mutation(&hook.agent_id)
            .await;
    }

    /// Deliver the pending notification of a settled script monitor the way
    /// the runtime's dispatcher does: queue its wake for the owner, admit it
    /// in the store, publish the queue and drain it.
    pub(crate) async fn dispatch_script_monitor(
        &self,
        svc: &Services,
        row: &intent_core::ScriptMonitor,
    ) {
        self.record("dispatch_script_monitor");
        let Some(job) = svc
            .store()
            .script_notification_jobs()
            .await
            .expect("notification jobs")
            .into_iter()
            .find(|job| job.wake_id == format!("script-monitor:{}", row.monitor_id))
        else {
            return;
        };
        let Some(id) = monitor_id(Some(&job.metadata)) else {
            return;
        };
        if !svc
            .store()
            .script_monitor_wake_pending(id)
            .await
            .expect("wake pending")
        {
            return;
        }
        if !self
            .script_monitor_delivery_allowed(svc, &job.agent_id, Some(&job.metadata))
            .await
            .expect("delivery allowed")
        {
            svc.store()
                .finish_script_monitor_wake(id, true)
                .await
                .expect("finish wake");
            return;
        }
        let agents = svc.feature_agent_wake();
        let Some(drainer) = agents.queue_drainer(svc) else {
            return;
        };
        let receipt = agents
            .enqueue_automatic_message(
                svc,
                &job.agent_id,
                Some(job.wake_id.clone()),
                job.body.clone(),
                Some(job.metadata.clone()),
            )
            .expect("enqueue notification");
        svc.store()
            .admit_script_notification(&receipt)
            .await
            .expect("admit notification");
        agents.publish_queue_updated(svc, &job.agent_id).await;
        drainer
            .try_drain_queue(job.agent_id.clone(), job.workspace_id.clone())
            .await;
        svc.redeliver_completion_after_queue_mutation(&row.agent_id)
            .await;
    }

    /// Retry a finished monitor delivery's completion marker, as the runtime
    /// does after a failed marker write.
    pub(crate) async fn retry_script_monitor_completion(
        &self,
        svc: &Services,
        agent: &AgentId,
        id: &str,
    ) {
        self.record("retry_script_monitor_completion");
        if svc
            .store()
            .complete_script_notification(agent, id)
            .await
            .is_ok()
        {
            self.monitor_completions
                .lock()
                .unwrap()
                .remove(&(agent.clone(), id.to_owned()));
            svc.redeliver_completion_after_queue_mutation(agent).await;
        }
    }

    /// Whether a finished delivery's completion marker still awaits a retry.
    pub(crate) fn completion_retry_pending(&self, agent: &AgentId, id: &str) -> bool {
        self.monitor_completions
            .lock()
            .unwrap()
            .contains(&(agent.clone(), id.to_owned()))
    }

    /// The port calls the fake answered, in order.
    pub(crate) fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().unwrap().clone()
    }

    fn record(&self, call: &'static str) {
        self.calls.lock().unwrap().push(call);
    }
}

/// Mark a hook row cancelled, as the runtime's cancel transition does.
async fn mark_cancelled(svc: &Services, hook: &Hook) {
    svc.store()
        .update_hook_state(&hook.hook_id, intent_core::HookState::Cancelled)
        .await
        .expect("cancel hook row");
    svc.store()
        .update_hook_next_run(&hook.hook_id, None)
        .await
        .expect("clear hook next run");
}

/// The `waitingOnHooks` entry the runtime reports for an active hook.
fn waiting_entry(h: intent_store::ActiveHookMetadata) -> Value {
    let mut v = json!({ "hookId": h.hook_id, "name": h.name });
    if let Some(next) = h.next_run_at {
        v["nextRunAt"] = Value::String(next);
    }
    if let Some(expires) = h.expires_at {
        v["expiresAt"] = Value::String(expires);
    }
    v
}

fn is_active(hook: &Hook) -> bool {
    matches!(
        hook.state,
        intent_core::HookState::Scheduled | intent_core::HookState::Running
    )
}

/// A scheduled hook row owned by `agent`, for [`FakeRuntime::add_hook`].
pub(crate) fn scheduled_hook(workspace: &WorkspaceId, agent: &AgentId, name: &str) -> Hook {
    Hook {
        hook_id: HookId::new(),
        workspace_id: workspace.clone(),
        agent_id: agent.clone(),
        name: name.to_string(),
        code: "return { dispatch: false };".to_string(),
        delay_ms: 10_000,
        cron: None,
        run_at: None,
        state: intent_core::HookState::Scheduled,
        created_at: intent_core::now_iso(),
        last_run_at: None,
        next_run_at: None,
        run_count: 0,
        last_error: None,
        last_logs: None,
        last_state: None,
        expires_at: None,
        perpetual: false,
        dispatch_count: 0,
    }
}

/// An active script-monitor row owned by `agent`, for seeding with
/// `store().insert_script_monitor`.
pub(crate) fn active_script_monitor(
    workspace: &WorkspaceId,
    agent: &AgentId,
    script_name: &str,
) -> intent_core::ScriptMonitor {
    intent_core::ScriptMonitor {
        monitor_id: uuid::Uuid::new_v4().to_string(),
        workspace_id: workspace.clone(),
        agent_id: agent.clone(),
        script_id: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        script_name: script_name.into(),
        mode: intent_core::ScriptMode::Command,
        state: "active".into(),
        created_at: intent_core::now_iso(),
        expires_at: "2099-01-01T00:00:00Z".into(),
        output_pattern: None,
        line_count: None,
        settled_at: None,
        reason: None,
        result: None,
        trigger: None,
    }
}

/// Forward each listed method to [`UnavailableRuntime`].
macro_rules! forward_to_unavailable {
    ($(fn $name:ident $(<$lt:lifetime>)? (&$($sl:lifetime)? self $(, $arg:ident: $ty:ty)*) $(-> $ret:ty)?;)*) => {
        $(
            fn $name $(<$lt>)? (&$($sl)? self $(, $arg: $ty)*) $(-> $ret)? {
                UNAVAILABLE.$name($($arg),*)
            }
        )*
    };
}

impl RuntimePort for FakeRuntime {
    fn active_hooks_for_agent<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<Value>> {
        self.record("active_hooks_for_agent");
        Box::pin(async move {
            svc.store()
                .active_hook_metadata_by_agent(agent_id)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(waiting_entry)
                .collect()
        })
    }

    fn active_hooks_by_agent<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, HashMap<String, Vec<Value>>> {
        self.record("active_hooks_by_agent");
        Box::pin(async move {
            let mut by_agent: HashMap<String, Vec<Value>> = HashMap::new();
            for h in svc
                .store()
                .active_hook_metadata_by_workspace(workspace_id)
                .await
                .unwrap_or_default()
            {
                by_agent
                    .entry(h.agent_id.0.clone())
                    .or_default()
                    .push(waiting_entry(h));
            }
            by_agent
        })
    }

    fn annotate_waiting_on_hooks<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        data: &'a mut Value,
    ) -> BoxFuture<'a, Vec<Value>> {
        Box::pin(async move {
            self.record("annotate_waiting_on_hooks");
            if let Some(existing) = data.get("waitingOnHooks").and_then(Value::as_array) {
                return existing.clone();
            }
            let hooks = self.active_hooks_for_agent(svc, agent_id).await;
            if !hooks.is_empty() {
                if let Some(obj) = data.as_object_mut() {
                    obj.insert("waitingOnHooks".to_string(), Value::Array(hooks.clone()));
                }
            }
            hooks
        })
    }

    fn cancel_agent_hooks<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.record("cancel_agent_hooks");
            let hooks = svc
                .store()
                .list_hooks_by_agent(agent_id)
                .await
                .unwrap_or_default();
            for hook in hooks.iter().filter(|h| is_active(h)) {
                mark_cancelled(svc, hook).await;
                svc.redeliver_completion_after_queue_mutation(&hook.agent_id)
                    .await;
            }
        })
    }

    fn cancel_workspace_hooks<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
    ) -> BoxFuture<'a, BTreeMap<AgentId, Vec<(String, HookId)>>> {
        Box::pin(async move {
            self.record("cancel_workspace_hooks");
            let mut by_owner: BTreeMap<AgentId, Vec<(String, HookId)>> = BTreeMap::new();
            let hooks = svc
                .store()
                .list_hooks_by_workspace(workspace_id)
                .await
                .unwrap_or_default();
            for hook in hooks.into_iter().filter(is_active) {
                mark_cancelled(svc, &hook).await;
                by_owner
                    .entry(hook.agent_id)
                    .or_default()
                    .push((hook.name, hook.hook_id));
            }
            by_owner
        })
    }

    fn active_script_monitors_for_agent<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Vec<Value>> {
        self.record("active_script_monitors_for_agent");
        Box::pin(async move {
            svc.store()
                .script_monitor_waiting(None, Some(agent_id))
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|(_, row)| row)
                .collect()
        })
    }

    fn annotate_waiting_on_script_monitors<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        data: &'a mut Value,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.record("annotate_waiting_on_script_monitors");
            let rows = self.active_script_monitors_for_agent(svc, agent_id).await;
            if !rows.is_empty() {
                data["waitingOnScriptMonitors"] = json!(rows);
            }
        })
    }

    fn admit_script_monitor_turn<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        metadata: Option<&'a Value>,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            self.record("admit_script_monitor_turn");
            let Some(id) = monitor_id(metadata) else {
                return Ok(true);
            };
            if !self
                .script_monitor_delivery_allowed(svc, agent_id, metadata)
                .await?
            {
                return Ok(false);
            }
            svc.store().accept_script_notification(agent_id, id).await
        })
    }

    fn script_monitor_delivery_allowed<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        metadata: Option<&'a Value>,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            self.record("script_monitor_delivery_allowed");
            let Some(id) = monitor_id(metadata) else {
                return Ok(true);
            };
            if self.completion_retry_pending(agent_id, id) {
                return Ok(false);
            }
            let owner = match svc.store().get_agent_session_summary(agent_id).await {
                Ok(owner) => owner,
                Err(intent_core::Error::NotFound(_)) => return Ok(false),
                Err(error) => return Err(error),
            };
            if metadata.and_then(|md| md["workspaceId"].as_str())
                != Some(owner.workspace_id.as_str())
                || metadata
                    .and_then(|md| md["agentId"].as_str())
                    .is_some_and(|id| id != agent_id.as_str())
                || svc
                    .store()
                    .get_agent_session_retired_at(agent_id)
                    .await?
                    .is_some()
                || (!owner.workspace_id.is_chief()
                    && match svc.store().get_workspace(&owner.workspace_id).await {
                        Ok(workspace) => workspace.archived,
                        Err(intent_core::Error::NotFound(_)) => true,
                        Err(error) => return Err(error),
                    })
            {
                return Ok(false);
            }
            svc.store()
                .script_monitor_wake_allowed_for_agent(id, agent_id, &owner.workspace_id)
                .await
        })
    }

    fn complete_script_monitor_delivery<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
        monitor_id: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.record("complete_script_monitor_delivery");
            {
                let mut workers = self.monitor_workers.lock().unwrap();
                if workers
                    .get(agent_id)
                    .is_some_and(|current| current == monitor_id)
                {
                    workers.remove(agent_id);
                }
            }
            self.monitor_completions
                .lock()
                .unwrap()
                .insert((agent_id.clone(), monitor_id.to_owned()));
            self.retry_script_monitor_completion(svc, agent_id, monitor_id)
                .await;
        })
    }

    fn cancel_script_monitors<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: Option<&'a AgentId>,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.record("cancel_script_monitors");
            let store = svc.store();
            let owners = store
                .script_notification_owners(workspace_id, agent_id)
                .await?;
            if owners.is_empty() {
                return Ok(());
            }
            for mut row in store.script_monitors(workspace_id, agent_id).await? {
                if row.state == "active" {
                    row.state = "cancelled".into();
                    row.reason = Some(reason.into());
                    row.settled_at = Some(intent_core::now_iso());
                    store.settle_script_monitor(&row).await?;
                }
                store
                    .finish_script_monitor_wake(&row.monitor_id, true)
                    .await?;
            }
            store
                .suppress_script_notifications(workspace_id, agent_id)
                .await?;
            {
                let mut queues = svc.feature_agent_queues().lock().unwrap();
                for (id, queue) in queues.iter_mut() {
                    if agent_id.is_some_and(|a| a != id) {
                        continue;
                    }
                    queue.retain(|entry| {
                        entry.message_metadata.as_ref().is_none_or(|md| {
                            md["type"] != "script_monitor_wake"
                                || md["workspaceId"].as_str() != Some(workspace_id.as_str())
                        })
                    });
                }
            }
            for owner in owners {
                svc.redeliver_completion_after_queue_mutation(&owner).await;
            }
            Ok(())
        })
    }

    /// A clean merge: the git merge-back itself is runtime behaviour, so the
    /// fake leaves the checkout untouched and reports success.
    fn merge_sandbox<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<MergeOutcome>> {
        self.record("merge_sandbox");
        Box::pin(async {
            Ok(MergeOutcome::Merged {
                commit_range: "fake..fake".into(),
                canonical_head: "fake".into(),
            })
        })
    }

    /// Remove the sandbox folder and row, as the runtime's discard does.
    /// Mirrors the runtime's claimed-tab migration on a pin change: reassign
    /// the claimed rows in the store, flag each moved tab's desktop
    /// assignment, and wake desktop waiters. The runtime's per-tab
    /// `browser:tab-updated` events are not published here.
    fn migrate_claimed_browser_tabs<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        new_host: &'a ClientId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let moved = svc
                .store
                .reassign_claimed_browser_tabs(workspace_id, new_host)
                .await?;
            for (tab, _) in &moved {
                svc.desktop.assignment_changed(&tab.workspace_id);
            }
            svc.desktop.changed.notify_waiters();
            Ok(())
        })
    }

    fn discard_sandbox<'a>(
        &'a self,
        svc: &'a Services,
        workspace_id: &'a WorkspaceId,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.record("discard_sandbox");
            if let Some(sandbox) = svc.store().get_sandbox(workspace_id, agent_id).await? {
                let path = PathBuf::from(&sandbox.path);
                if path.exists() {
                    std::fs::remove_dir_all(&path)
                        .map_err(|e| intent_core::Error::Internal(e.to_string()))?;
                }
            }
            svc.store().delete_sandbox(workspace_id, agent_id).await
        })
    }

    fn script_monitor_export_blocked(&self, svc: &Services, metadata: Option<&Value>) -> bool {
        let Some(md) = metadata.filter(|md| monitor_id(Some(md)).is_some()) else {
            return false;
        };
        md["workspaceId"]
            .as_str()
            .is_some_and(|workspace| svc.feature_transfer_port().export_in_progress(workspace))
    }

    fn defer_script_monitor_for_export(
        &self,
        svc: &Services,
        agent_id: &AgentId,
        content: &str,
        metadata: Option<&Value>,
    ) -> bool {
        self.record("defer_script_monitor_for_export");
        if !self.script_monitor_export_blocked(svc, metadata) {
            return false;
        }
        let _receipt = svc.feature_agent_wake().enqueue_automatic_message(
            svc,
            agent_id,
            wake_id(metadata),
            content.to_owned(),
            metadata.cloned(),
        );
        true
    }

    fn track_script_monitor_worker(
        &self,
        _svc: &Services,
        agent_id: &AgentId,
        metadata: Option<&Value>,
    ) {
        self.record("track_script_monitor_worker");
        let mut workers = self.monitor_workers.lock().unwrap();
        if let Some(id) = monitor_id(metadata) {
            workers.insert(agent_id.clone(), id.to_owned());
        } else {
            workers.remove(agent_id);
        }
    }

    fn settle_script_monitor_worker<'a>(
        &'a self,
        svc: &'a Services,
        agent_id: &'a AgentId,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.record("settle_script_monitor_worker");
            let id = self.monitor_workers.lock().unwrap().remove(agent_id);
            if let Some(id) = id {
                self.complete_script_monitor_delivery(svc, agent_id, &id)
                    .await;
            }
        })
    }

    forward_to_unavailable! {
        fn execution_context_snapshot<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<HostExecutionContext>>;
        fn script_list<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_list_filtered<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, archive: intent_core::ScriptArchiveFilter) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_list_view<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, options: serde_json::Value) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_get<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_monitor_list_view<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, agent_id: Option<AgentId>, options: serde_json::Value) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_monitor_get<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, monitor_id: String, owner: Option<AgentId>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_archive<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_ids: Vec<String>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_restore<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_ids: Vec<String>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_create<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, params: ScriptCreateParams) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_remove<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_monitor<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, agent_id: AgentId, script_id: String, options: serde_json::Value) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_monitor_list<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, agent_id: Option<AgentId>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_monitor_cancel<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, monitor_id: String, owner: Option<AgentId>, stop_run: bool) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_start<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_start_monitor<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, agent_id: AgentId, script_id: String, run_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_stop<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_restart<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_output<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String, max_lines: Option<i64>, paginate: Option<bool>, page_token: Option<String>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_status<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn script_run<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, script_id: String, max_lines: Option<i64>, timeout_seconds: Option<i64>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn hook_schedule<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, agent_id: AgentId, params: serde_json::Value) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn hook_list<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, agent_id: Option<AgentId>, include_retired: bool) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn hook_get<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, hook_id: intent_core::HookId) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn hook_cancel<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, hook_id: intent_core::HookId, caller: Option<AgentId>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn hook_run_now<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, hook_id: intent_core::HookId) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn terminal_create<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, cols: u16, rows: u16, cwd: Option<String>, command: Option<String>, env: Option<std::collections::BTreeMap<String, String>>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn terminal_write<'a>(&'a self, svc: &'a Services, terminal_id: String, data: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn terminal_resize<'a>(&'a self, svc: &'a Services, terminal_id: String, cols: u16, rows: u16) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn terminal_kill<'a>(&'a self, svc: &'a Services, terminal_id: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn terminal_get_buffer<'a>(&'a self, svc: &'a Services, terminal_id: String, max_bytes: Option<i64>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn terminal_list<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn terminal_read_output<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, terminal_id: String, max_lines: Option<i64>, paginate: Option<bool>, page_token: Option<String>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn browser_list_tabs<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId) -> BoxFuture<'a, Result<Vec<BrowserTab>>>;
        fn browser_upsert_tab<'a>(&'a self, svc: &'a Services, host: ClientId, tab: BrowserTabInput) -> BoxFuture<'a, Result<BrowserTab>>;
        fn browser_remove_tab<'a>(&'a self, svc: &'a Services, host: ClientId, tab_id: String) -> BoxFuture<'a, Result<()>>;
        fn browser_sync_tabs<'a>(&'a self, svc: &'a Services, host: ClientId, tabs: Vec<BrowserTabInput>) -> BoxFuture<'a, Result<Vec<String>>>;
        fn browser_exec<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, actions: Vec<serde_json::Value>, tab_id: Option<String>, agent_id: Option<AgentId>) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn browser_dispatch_error_message<'a>(&'a self, svc: &'a Services, err: intent_core::ReverseDispatchError) -> BoxFuture<'a, String>;
        fn browser_navigate_tab<'a>(&'a self, svc: &'a Services, tab_id: String, url: String) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn browser_close_tab<'a>(&'a self, svc: &'a Services, tab_id: String, force: bool) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn sandbox_merge<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, sandbox_id: AgentId) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn sandbox_discard<'a>(&'a self, svc: &'a Services, workspace_id: WorkspaceId, sandbox_id: AgentId) -> BoxFuture<'a, Result<serde_json::Value>>;
        fn provision_sandbox<'a>(&'a self, svc: &'a Services, workspace_id: &'a WorkspaceId, agent_id: &'a AgentId, workspaces_root: PathBuf) -> BoxFuture<'a, Result<ProvisionOutcome>>;
        fn run_setup_script(&self, svc: &Services, run: SetupScriptRun) -> BoxFuture<'static, (bool, Option<u32>)>;
        fn script_monitor_lane<'a>(&'a self, svc: &'a Services) -> &'a tokio::sync::Mutex<()>;
        fn close_workspace_scripts<'a>(&'a self, svc: &'a Services, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Box<dyn ScriptsClosing>>;
        fn hydrate_scripts<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<usize>>;
        fn recover_imported_scripts<'a>(&'a self, svc: &'a Services, workspace_id: &'a WorkspaceId, script_ids: &'a [String]) -> BoxFuture<'a, ()>;
        fn rehydrate_hooks<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, Result<usize>>;
        fn command_script_activity(&self, svc: &Services) -> CommandScriptActivity;
        fn workspace_scripts_running(&self, svc: &Services, workspace_id: &WorkspaceId) -> bool;
        fn stop_all_scripts<'a>(&'a self, svc: &'a Services) -> BoxFuture<'a, (usize, usize)>;
        fn clear_hook_tasks(&self, svc: &Services);
        fn abort_workspace_hook_tasks<'a>(&'a self, svc: &'a Services, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, ()>;
        fn delete_workspace_script_output<'a>(&'a self, svc: &'a Services, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, bool>;
        fn agent_terminal_host(&self, svc: &Services, terminal_requires_shell: bool, session_cwd: Option<PathBuf>) -> Arc<dyn intent_acp::TerminalHost>;
    }
}
