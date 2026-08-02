//! Background hook scheduler (spec: "JS kernel" watchers). A hook is a small
//! agent-owned JavaScript script the daemon runs periodically (fixed
//! `delayMs` between runs) until it signals a dispatch, fails, or is
//! cancelled. Each active hook owns one tokio task; schedules persist to the
//! `hook` table and rehydrate at boot ([`Services::rehydrate_hooks`]).
//!
//! Scripts evaluate in QuickJS via `intent_js::eval` with the exact same
//! `ws.*` prelude + host dispatch the `workspace_api` MCP tool installs
//! (including `ws.host.exec`), a 60 s wall-clock budget, and the hook's
//! workspace/agent pinned as the caller. The script's return value is the
//! contract: `{ dispatch: true, message }` wakes the owning agent (queued
//! behind an in-flight turn via the automatic-delivery `agent.sendMessage`
//! path) and terminates the hook; `{ dispatch: false }` / `undefined` sleeps
//! `delayMs` and re-runs; a throw or timeout evicts the hook, persists
//! `last_error`, emits `hook:evicted`, and wakes the owner with the reason.
//! Scripts may call `console.log/info/warn/error`; the last run's captured
//! output persists to `last_logs` (overwritten each run, capped) and is
//! appended to dispatch/evict wake messages.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::events::{
    HOOK_CANCELLED, HOOK_DISPATCHED, HOOK_EVICTED, HOOK_RUN_COMPLETED, HOOK_RUN_STARTED,
    HOOK_SCHEDULED,
};
use intent_core::{
    now_iso, AgentId, AgentStatus, Error, Hook, HookId, HookState, Result, WorkspaceApi,
    WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::{publish_event, system_actor, Services};

/// Minimum inter-run delay (spec decision 5): `delayMs` below this floor is
/// rejected at schedule time.
pub(crate) const MIN_HOOK_DELAY_MS: i64 = 10_000;

/// Maximum hook name length (spec: name > 19 chars fails validation).
pub(crate) const MAX_HOOK_NAME_LEN: usize = 19;

/// Wall-clock budget for one hook script run (spec: any run exceeding 60 s is
/// killed and the hook evicted). Tests compress it via the `#[cfg(test)]`-only
/// [`Services::with_hook_eval_timeout`].
pub(crate) const HOOK_EVAL_TIMEOUT: Duration = Duration::from_secs(60);

/// Per-run console-capture caps: the in-envelope `console.*` shim buffers at
/// most this many lines / bytes, dropping from the head (with a marker line)
/// so a hot loop cannot bloat the `last_logs` row.
const HOOK_LOG_MAX_LINES: usize = 100;
const HOOK_LOG_MAX_BYTES: usize = 8 * 1024;

/// Cap (in chars) on the `[hook logs]` section appended to dispatch/evict
/// wake messages; longer captures are head-truncated to the most recent tail.
const HOOK_WAKE_LOGS_CAP: usize = 2048;

/// Control frames the service ops send to a hook's scheduler task.
enum HookControl {
    /// Run the script immediately and reset the inter-run timer.
    RunNow,
}

/// Live handle for one active hook's scheduler task.
pub(crate) struct HookHandle {
    control: mpsc::Sender<HookControl>,
    abort: tokio::task::AbortHandle,
}

/// Registry of active hook tasks, shared across [`Services`] clones so the
/// RPC/MCP front doors and the tasks themselves observe the same set.
pub(crate) type HookTasks = Arc<Mutex<HashMap<HookId, HookHandle>>>;

/// Outcome of one script run. Every variant carries the run's captured
/// console output (`None` when the script logged nothing).
enum RunOutcome {
    /// `{ dispatch: false }` / `undefined` — sleep and re-run.
    Continue { logs: Option<String> },
    /// `{ dispatch: true, message }` — wake the owner, terminate the hook.
    Dispatch {
        message: String,
        logs: Option<String>,
    },
    /// Throw or timeout — evict, persist the error, wake the owner.
    Failed { error: String, logs: RunLogs },
}

/// Console capture attached to a failed run. A script throw is caught inside
/// the eval envelope, so its logs survive; a timeout (or engine failure)
/// kills the whole eval before the capture can be returned, so that run's
/// logs are lost — the previously persisted `last_logs` is left untouched.
enum RunLogs {
    Captured(Option<String>),
    Lost,
}

/// ISO-8601 timestamp `delay_ms` in the future — the persisted/emitted
/// `nextRunAt` clients animate countdown bars from.
fn next_run_at_iso(delay_ms: i64) -> String {
    (OffsetDateTime::now_utc() + time::Duration::milliseconds(delay_ms))
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Evaluate one hook script in QuickJS with the full `ws.*` environment and
/// interpret its return value against the script contract. Never panics; every
/// failure mode folds into [`RunOutcome::Failed`].
async fn run_hook_script(api: Arc<dyn WorkspaceApi>, hook: &Hook, timeout: Duration) -> RunOutcome {
    let host = intent_acp::make_workspace_host(
        api,
        hook.workspace_id.clone(),
        Some(hook.agent_id.clone()),
        None,
    );
    // Same `{__k, __v}` envelope as the `workspace_api` dispatch so an
    // `undefined` return (no dispatch) survives the JSON bridge, extended
    // with a `console.*` shim whose capped line buffer rides back as
    // `__logs`. A user-code throw is caught in-envelope (`__k: 'e'`) so its
    // logs survive; only a timeout/engine failure loses them.
    let prelude = intent_acp::bindings_prelude();
    let code = &hook.code;
    let max_lines = HOOK_LOG_MAX_LINES;
    let max_bytes = HOOK_LOG_MAX_BYTES;
    let full_code = format!(
        "{prelude}\n\
         const __hook_logs = [];\n\
         let __hook_logs_bytes = 0;\n\
         let __hook_logs_dropped = false;\n\
         const __hook_byte_len = (s) => {{\n\
           let n = 0;\n\
           for (let i = 0; i < s.length; i++) {{\n\
             const c = s.codePointAt(i);\n\
             if (c > 0xffff) i++;\n\
             n += c <= 0x7f ? 1 : c <= 0x7ff ? 2 : c <= 0xffff ? 3 : 4;\n\
           }}\n\
           return n;\n\
         }};\n\
         const __hook_log = (...args) => {{\n\
           const line = args.map((a) => {{\n\
             if (typeof a === 'string') return a;\n\
             try {{ const s = JSON.stringify(a); return s === undefined ? String(a) : s; }}\n\
             catch (_e) {{ return String(a); }}\n\
           }}).join(' ');\n\
           __hook_logs.push(line);\n\
           __hook_logs_bytes += __hook_byte_len(line) + 1;\n\
           while (__hook_logs.length > {max_lines} || __hook_logs_bytes > {max_bytes}) {{\n\
             const dropped = __hook_logs.shift();\n\
             if (dropped === undefined) break;\n\
             __hook_logs_bytes -= __hook_byte_len(dropped) + 1;\n\
             __hook_logs_dropped = true;\n\
           }}\n\
         }};\n\
         const console = {{ log: __hook_log, info: __hook_log, warn: __hook_log, error: __hook_log }};\n\
         let __hook_user;\n\
         let __hook_threw = false;\n\
         let __hook_err = '';\n\
         try {{\n\
           __hook_user = await (async () => {{ {code}\n}})();\n\
         }} catch (e) {{\n\
           __hook_threw = true;\n\
           __hook_err = String(e);\n\
         }}\n\
         const __logs = __hook_logs.length === 0 ? '' :\n\
           (__hook_logs_dropped ? '[earlier log lines truncated]\\n' : '') + __hook_logs.join('\\n');\n\
         if (__hook_threw) return {{ __k: 'e', __v: __hook_err, __logs }};\n\
         return {{ __k: __hook_user === undefined ? 'u' : 'v', __v: __hook_user, __logs }};"
    );
    let opts = intent_js::EvalOptions {
        timeout,
        ..intent_js::EvalOptions::default()
    };
    match intent_js::eval(&full_code, &opts, Some(host)).await {
        Ok(v) => {
            let logs = v
                .get("__logs")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            match v.get("__k").and_then(Value::as_str) {
                Some("u") => RunOutcome::Continue { logs },
                Some("v") => parse_outcome(v.get("__v").cloned().unwrap_or(Value::Null), logs),
                Some("e") => RunOutcome::Failed {
                    error: v
                        .get("__v")
                        .and_then(Value::as_str)
                        .unwrap_or("(unknown script error)")
                        .to_string(),
                    logs: RunLogs::Captured(logs),
                },
                _ => RunOutcome::Failed {
                    error: "engine: unexpected hook eval envelope".to_string(),
                    logs: RunLogs::Lost,
                },
            }
        }
        // Timeout or engine failure: the eval died before the envelope (and
        // its console capture) could be returned — the logs are lost.
        Err(e) => RunOutcome::Failed {
            error: e.to_string(),
            logs: RunLogs::Lost,
        },
    }
}

/// Interpret the script's returned value: only `{ dispatch: true }` fires a
/// dispatch; everything else (including `null` and non-objects) re-runs.
fn parse_outcome(v: Value, logs: Option<String>) -> RunOutcome {
    if v.get("dispatch").and_then(Value::as_bool) == Some(true) {
        let message = v
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("(hook dispatched with no message)")
            .to_string();
        return RunOutcome::Dispatch { message, logs };
    }
    RunOutcome::Continue { logs }
}

/// Append a run's captured console output to a wake message as a
/// `[hook logs]` section, head-truncated to [`HOOK_WAKE_LOGS_CAP`] chars so a
/// log-heavy run cannot flood the owner's queue. No-op when the run logged
/// nothing.
fn with_wake_logs(message: &str, logs: Option<&str>) -> String {
    let Some(logs) = logs.filter(|l| !l.is_empty()) else {
        return message.to_string();
    };
    if logs.chars().count() <= HOOK_WAKE_LOGS_CAP {
        return format!("{message}\n\n[hook logs]\n{logs}");
    }
    let start = logs
        .char_indices()
        .rev()
        .nth(HOOK_WAKE_LOGS_CAP - 1)
        .map(|(i, _)| i)
        .unwrap_or(0);
    format!(
        "{message}\n\n[hook logs]\n[earlier log lines truncated]\n{}",
        &logs[start..]
    )
}

impl Services {
    /// `hook.schedule`: validate, run the script once immediately (a real run
    /// — a dispatch wakes the owner and never persists a schedule; a failure
    /// rejects the call), then persist the hook and spawn its scheduler task.
    pub(crate) async fn hook_schedule_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        params: &Value,
    ) -> Result<Value> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::InvalidParams("hook.schedule: `name` is required".into()))?;
        if name.chars().count() > MAX_HOOK_NAME_LEN {
            return Err(Error::InvalidParams(format!(
                "hook.schedule: `name` must be at most {MAX_HOOK_NAME_LEN} characters"
            )));
        }
        let code = params
            .get("code")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| Error::InvalidParams("hook.schedule: `code` is required".into()))?;
        let delay_ms = params
            .get("delayMs")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::InvalidParams("hook.schedule: `delayMs` is required".into()))?;
        if delay_ms < MIN_HOOK_DELAY_MS {
            return Err(Error::InvalidParams(format!(
                "hook.schedule: `delayMs` must be at least {MIN_HOOK_DELAY_MS}"
            )));
        }
        // Per-agent cap on active hooks (`[hooks] maxPerAgent`).
        let cap = self.hooks_max_per_agent as usize;
        let active = self
            .store
            .list_hooks_by_agent(agent_id)
            .await?
            .into_iter()
            .filter(|h| matches!(h.state, HookState::Scheduled | HookState::Running))
            .count();
        if active >= cap {
            return Err(Error::InvalidParams(format!(
                "hook.schedule: agent already has {active} active hooks (max {cap})"
            )));
        }

        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: workspace_id.clone(),
            agent_id: agent_id.clone(),
            name: name.to_string(),
            code: code.to_string(),
            delay_ms,
            state: HookState::Running,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
        };

        // Validation run (spec decision 4): a REAL run, before anything
        // persists. Failure rejects the schedule (nothing is persisted and no
        // lifecycle event fires — the error surfaces on the call itself); a
        // dispatch wakes the owner immediately and the hook never schedules.
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.clone());
        let outcome = run_hook_script(api, &hook, self.hook_eval_timeout).await;
        match outcome {
            RunOutcome::Failed { error, .. } => Err(Error::InvalidParams(format!(
                "hook.schedule: first run failed: {error}"
            ))),
            RunOutcome::Dispatch { message, logs } => {
                let mut hook = hook;
                hook.state = HookState::Dispatched;
                hook.last_run_at = Some(now_iso());
                hook.run_count = 1;
                hook.last_logs = logs;
                self.store.insert_hook(&hook).await?;
                self.emit_hook_event(HOOK_RUN_COMPLETED, &hook, None).await;
                let message = with_wake_logs(&message, hook.last_logs.as_deref());
                self.wake_hook_owner(&hook, &message, "dispatched").await;
                self.emit_hook_event(HOOK_DISPATCHED, &hook, None).await;
                Ok(json!({ "hook": hook, "dispatched": true }))
            }
            RunOutcome::Continue { logs } => {
                let mut hook = hook;
                hook.state = HookState::Scheduled;
                hook.last_run_at = Some(now_iso());
                hook.next_run_at = Some(next_run_at_iso(delay_ms));
                hook.run_count = 1;
                hook.last_logs = logs;
                self.store.insert_hook(&hook).await?;
                self.emit_hook_event(HOOK_RUN_COMPLETED, &hook, hook.next_run_at.clone())
                    .await;
                self.emit_hook_event(HOOK_SCHEDULED, &hook, hook.next_run_at.clone())
                    .await;
                self.spawn_hook_task(hook.clone());
                Ok(json!({ "hook": hook, "dispatched": false }))
            }
        }
    }

    /// `hook.list`: hooks in a workspace (optionally one agent's), oldest
    /// first, as `{ hooks: [Hook] }`.
    pub(crate) async fn hook_list_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: Option<&AgentId>,
    ) -> Result<Value> {
        let hooks = match agent_id {
            Some(a) => self.store.list_hooks_by_agent(a).await?,
            None => self.store.list_hooks_by_workspace(workspace_id).await?,
        };
        let hooks: Vec<Hook> = hooks
            .into_iter()
            .filter(|h| &h.workspace_id == workspace_id)
            .collect();
        Ok(json!({ "hooks": hooks }))
    }

    /// `hook.cancel`: stop an active hook's task, mark it cancelled, and emit
    /// `hook:cancelled`. A non-owner cancel (`by_owner = false`, the FE path)
    /// additionally wakes the owning agent with a notice.
    pub(crate) async fn hook_cancel_op(
        &self,
        workspace_id: &WorkspaceId,
        hook_id: &HookId,
        by_owner: bool,
    ) -> Result<Value> {
        let hook = self.store.get_hook(hook_id).await?;
        if &hook.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("hook {} not found", hook_id.0)));
        }
        if !matches!(hook.state, HookState::Scheduled | HookState::Running) {
            return Err(Error::InvalidParams(format!(
                "hook.cancel: hook {} is not active",
                hook_id.0
            )));
        }
        self.abort_hook_task(hook_id);
        self.store
            .update_hook_state(hook_id, HookState::Cancelled)
            .await?;
        self.store.update_hook_next_run(hook_id, None).await?;
        let mut hook = hook;
        hook.state = HookState::Cancelled;
        hook.next_run_at = None;
        self.emit_hook_event(HOOK_CANCELLED, &hook, None).await;
        if !by_owner {
            self.wake_hook_owner(&hook, "This hook was cancelled from the app.", "cancelled")
                .await;
        }
        Ok(json!({ "ok": true, "hook": hook }))
    }

    /// `hook.runNow`: signal an active hook's task to run immediately (the
    /// inter-run timer resets after the run).
    pub(crate) async fn hook_run_now_op(
        &self,
        workspace_id: &WorkspaceId,
        hook_id: &HookId,
    ) -> Result<Value> {
        let hook = self.store.get_hook(hook_id).await?;
        if &hook.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("hook {} not found", hook_id.0)));
        }
        if !matches!(hook.state, HookState::Scheduled | HookState::Running) {
            return Err(Error::InvalidParams(format!(
                "hook.runNow: hook {} is not active",
                hook_id.0
            )));
        }
        let control = {
            let tasks = self.hook_tasks.lock().unwrap();
            tasks.get(hook_id).map(|h| h.control.clone())
        };
        let Some(control) = control else {
            return Err(Error::Internal(format!(
                "hook.runNow: hook {} has no live scheduler task",
                hook_id.0
            )));
        };
        control
            .send(HookControl::RunNow)
            .await
            .map_err(|_| Error::Internal("hook.runNow: scheduler task is gone".into()))?;
        Ok(json!({ "ok": true, "hookId": hook_id }))
    }

    /// Boot rehydration: reload every active (`scheduled`/`running`) hook row
    /// and respawn its scheduler task. Rows whose owning agent is gone are
    /// cancelled instead of resumed. `running` rows (daemon died mid-run) are
    /// reset to `scheduled`; every resumed hook starts a fresh `delayMs`
    /// countdown. Returns the number of resumed hooks.
    pub async fn rehydrate_hooks(&self) -> Result<usize> {
        let hooks = self.store.load_active_hooks().await?;
        let mut resumed = 0;
        for mut hook in hooks {
            if self.hook_task_alive(&hook.hook_id) {
                continue;
            }
            // Prune hooks whose owner no longer exists (deleted agents keep
            // their session row with status `deleted`).
            let owner_gone = match self.store.get_agent_session_status(&hook.agent_id).await {
                Ok(AgentStatus::Deleted) => true,
                Ok(_) => false,
                Err(Error::NotFound(_)) => true,
                Err(e) => return Err(e),
            };
            if owner_gone {
                let _ = self
                    .store
                    .update_hook_state(&hook.hook_id, HookState::Cancelled)
                    .await;
                let _ = self.store.update_hook_next_run(&hook.hook_id, None).await;
                hook.state = HookState::Cancelled;
                hook.next_run_at = None;
                self.emit_hook_event(HOOK_CANCELLED, &hook, None).await;
                continue;
            }
            let next_run_at = next_run_at_iso(hook.delay_ms);
            if hook.state == HookState::Running {
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Scheduled)
                    .await?;
                hook.state = HookState::Scheduled;
            }
            self.store
                .update_hook_next_run(&hook.hook_id, Some(&next_run_at))
                .await?;
            hook.next_run_at = Some(next_run_at.clone());
            self.emit_hook_event(HOOK_SCHEDULED, &hook, Some(next_run_at))
                .await;
            self.spawn_hook_task(hook);
            resumed += 1;
        }
        Ok(resumed)
    }

    /// Whether a live scheduler task exists for `hook_id`.
    fn hook_task_alive(&self, hook_id: &HookId) -> bool {
        self.hook_tasks.lock().unwrap().contains_key(hook_id)
    }

    /// Abort and deregister a hook's scheduler task (no-op when absent).
    fn abort_hook_task(&self, hook_id: &HookId) {
        if let Some(handle) = self.hook_tasks.lock().unwrap().remove(hook_id) {
            handle.abort.abort();
        }
    }

    /// Spawn the per-hook scheduler task: sleep `delayMs` (or a `runNow`
    /// control frame), run the script, and act on the outcome. The task
    /// deregisters itself from [`Services::hook_tasks`] on every exit path.
    fn spawn_hook_task(&self, hook: Hook) {
        let (control_tx, mut control_rx) = mpsc::channel::<HookControl>(4);
        let services = self.clone();
        let hook_id = hook.hook_id.clone();
        let join = tokio::spawn(async move {
            let mut hook = hook;
            loop {
                let delay = Duration::from_millis(hook.delay_ms.max(0) as u64);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    ctl = control_rx.recv() => {
                        match ctl {
                            Some(HookControl::RunNow) => {}
                            // All senders dropped — the registry entry is
                            // gone (cancel path aborts first, but guard the
                            // race anyway).
                            None => break,
                        }
                    }
                }
                match services.execute_hook_run(&mut hook).await {
                    Ok(true) => {}
                    // Terminal outcome (dispatch/evict): stop the loop.
                    Ok(false) => break,
                    // Unrecoverable store failure: the row may already be
                    // `running` with no task left to drive it — best-effort
                    // evict so it doesn't linger active-looking forever.
                    Err(e) => {
                        services.evict_hook_after_store_error(&mut hook, &e).await;
                        break;
                    }
                }
            }
            services.hook_tasks.lock().unwrap().remove(&hook.hook_id);
        });
        self.hook_tasks.lock().unwrap().insert(
            hook_id,
            HookHandle {
                control: control_tx,
                abort: join.abort_handle(),
            },
        );
    }

    /// One scheduled run of `hook`'s script. Returns `Ok(true)` to keep the
    /// loop alive (script continued), `Ok(false)` on a terminal outcome
    /// (dispatched or evicted).
    async fn execute_hook_run(&self, hook: &mut Hook) -> Result<bool> {
        // A cancel can race the sleep expiry: re-read the persisted state and
        // stop silently if this hook is no longer active.
        match self.store.get_hook(&hook.hook_id).await {
            Ok(h) if matches!(h.state, HookState::Scheduled | HookState::Running) => {}
            Ok(_) => return Ok(false),
            Err(Error::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        }
        self.store
            .update_hook_state(&hook.hook_id, HookState::Running)
            .await?;
        hook.state = HookState::Running;
        self.emit_hook_event(HOOK_RUN_STARTED, hook, None).await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.clone());
        let outcome = run_hook_script(api, hook, self.hook_eval_timeout).await;
        let last_run_at = now_iso();
        match outcome {
            RunOutcome::Continue { logs } => {
                let next_run_at = next_run_at_iso(hook.delay_ms);
                self.store
                    .update_hook_run(&hook.hook_id, &last_run_at, Some(&next_run_at))
                    .await?;
                self.store
                    .update_hook_last_logs(&hook.hook_id, logs.as_deref())
                    .await?;
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Scheduled)
                    .await?;
                hook.state = HookState::Scheduled;
                hook.last_run_at = Some(last_run_at);
                hook.next_run_at = Some(next_run_at.clone());
                hook.run_count += 1;
                hook.last_logs = logs;
                self.emit_hook_event(HOOK_RUN_COMPLETED, hook, Some(next_run_at))
                    .await;
                Ok(true)
            }
            RunOutcome::Dispatch { message, logs } => {
                self.store
                    .update_hook_run(&hook.hook_id, &last_run_at, None)
                    .await?;
                self.store
                    .update_hook_last_logs(&hook.hook_id, logs.as_deref())
                    .await?;
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Dispatched)
                    .await?;
                hook.state = HookState::Dispatched;
                hook.last_run_at = Some(last_run_at);
                hook.next_run_at = None;
                hook.run_count += 1;
                hook.last_logs = logs;
                self.emit_hook_event(HOOK_RUN_COMPLETED, hook, None).await;
                let message = with_wake_logs(&message, hook.last_logs.as_deref());
                self.wake_hook_owner(hook, &message, "dispatched").await;
                self.emit_hook_event(HOOK_DISPATCHED, hook, None).await;
                Ok(false)
            }
            RunOutcome::Failed { error, logs } => {
                self.store
                    .update_hook_run(&hook.hook_id, &last_run_at, None)
                    .await?;
                // A timed-out/engine-failed eval dies before the capture can
                // be returned (RunLogs::Lost): leave the previous run's
                // persisted last_logs untouched rather than clobbering it.
                if let RunLogs::Captured(ref logs) = logs {
                    self.store
                        .update_hook_last_logs(&hook.hook_id, logs.as_deref())
                        .await?;
                    hook.last_logs = logs.clone();
                }
                // Persist the error before the terminal state so a reader
                // that observes `evicted` always sees `lastError`.
                self.store
                    .update_hook_last_error(&hook.hook_id, Some(&error))
                    .await?;
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Evicted)
                    .await?;
                hook.state = HookState::Evicted;
                hook.last_run_at = Some(last_run_at);
                hook.next_run_at = None;
                hook.run_count += 1;
                hook.last_error = Some(error.clone());
                self.emit_hook_event(HOOK_EVICTED, hook, None).await;
                let notice = format!(
                    "Your background hook \"{}\" was evicted after a failed run: {error}",
                    hook.name
                );
                let notice = match logs {
                    RunLogs::Captured(ref l) => with_wake_logs(&notice, l.as_deref()),
                    RunLogs::Lost => notice,
                };
                self.wake_hook_owner(hook, &notice, "evicted").await;
                Ok(false)
            }
        }
    }

    /// Best-effort terminalization after a store error killed the scheduler
    /// loop: without this the row can sit in `running`/`scheduled` with no
    /// live task behind it. Every step here is itself best-effort (the store
    /// may still be failing) — log and move on rather than propagate.
    async fn evict_hook_after_store_error(&self, hook: &mut Hook, cause: &Error) {
        let error = format!("scheduler stopped after a store error: {cause}");
        tracing::warn!(hook = %hook.hook_id.0, error = %error, "evicting hook after store error");
        if let Err(e) = self
            .store
            .update_hook_last_error(&hook.hook_id, Some(&error))
            .await
        {
            tracing::warn!(hook = %hook.hook_id.0, error = %e, "failed to persist hook lastError");
        }
        if let Err(e) = self
            .store
            .update_hook_state(&hook.hook_id, HookState::Evicted)
            .await
        {
            tracing::warn!(hook = %hook.hook_id.0, error = %e, "failed to persist hook evicted state");
        }
        hook.state = HookState::Evicted;
        hook.next_run_at = None;
        hook.last_error = Some(error.clone());
        self.emit_hook_event(HOOK_EVICTED, hook, None).await;
        let notice = format!(
            "Your background hook \"{}\" was evicted after an internal error: {error}",
            hook.name
        );
        self.wake_hook_owner(hook, &notice, "evicted").await;
    }

    /// Wake the hook's owning agent via the automatic-delivery
    /// `agent.sendMessage` path (queue behind an in-flight turn, question
    /// hold respected). Best-effort: a delivery failure is logged, never
    /// propagated — the hook's own lifecycle transition already persisted.
    async fn wake_hook_owner(&self, hook: &Hook, message: &str, reason: &str) {
        let metadata = json!({
            "type": "hook_wake",
            "hookId": hook.hook_id,
            "hookName": hook.name,
            "reason": reason,
        });
        let content = format!("[Background hook \"{}\"] {message}", hook.name);
        if let Err(e) = self
            .deliver_wake_message(
                &hook.workspace_id,
                &hook.agent_id,
                &content,
                Some(&metadata),
            )
            .await
        {
            tracing::warn!(
                hook = %hook.hook_id.0,
                agent = %hook.agent_id.0,
                error = %e,
                "hook owner wake delivery failed"
            );
        }
    }

    /// Emit one `hook:*` lifecycle event with the canonical
    /// `{ workspaceId, agentId, hookId, name, nextRunAt?, state, lastError? }`
    /// payload.
    async fn emit_hook_event(&self, event_type: &str, hook: &Hook, next_run_at: Option<String>) {
        let mut data = json!({
            "workspaceId": hook.workspace_id,
            "agentId": hook.agent_id,
            "hookId": hook.hook_id,
            "name": hook.name,
            "state": hook.state,
        });
        if let Some(next) = next_run_at {
            data["nextRunAt"] = Value::String(next);
        }
        if let Some(err) = &hook.last_error {
            if event_type == HOOK_EVICTED {
                data["lastError"] = Value::String(err.clone());
            }
        }
        let event = NewEvent {
            workspace_id: hook.workspace_id.clone(),
            timestamp: now_iso(),
            event_type: event_type.to_string(),
            actor: system_actor(),
            session_id: Some(hook.agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        publish_event(&self.event_bus, event).await;
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use intent_core::{
        AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceStatus,
    };
    use intent_store::Store;

    use super::*;
    use crate::events::EventBus;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-hook-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    fn workspace(id: &WorkspaceId) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            checkout_mode: None,
            disk_usage: None,
        }
    }

    fn note(ws: &WorkspaceId, id: &str, content: &str) -> intent_core::Note {
        let ts = now_iso();
        intent_core::Note {
            id: intent_core::NoteId::from(id),
            workspace_id: ws.clone(),
            title: "Probe".to_string(),
            content: content.to_string(),
            content_type: intent_core::ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: intent_core::NoteVisibility::Workspace,
            metadata: intent_core::NoteMetadata::default(),
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        }
    }

    fn agent(ws: &WorkspaceId, id: &str) -> AgentSession {
        AgentSession {
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Owner".to_string(),
            name_explicitly_set: true,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
        }
    }

    /// Store + Services + workspace + owning agent, with an event bus wired
    /// so `hook:*` lifecycle events persist to the event log. The temp
    /// workspaces root keeps `ws.workspace.*` bindings hermetic (the
    /// cowSupported probe must never touch `~/intent/workspaces`).
    async fn setup() -> (TempDb, tempfile::TempDir, Services, WorkspaceId, AgentId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let owner = AgentId::from("agent-hooks");
        store
            .insert_agent_session(&agent(&ws, "agent-hooks"))
            .await
            .expect("agent");
        let bus = EventBus::new(store.clone());
        let root = tempfile::tempdir().expect("temp workspaces root");
        let services = Services::new(store)
            .with_event_bus(bus)
            .with_workspaces_root(root.path().to_path_buf());
        (tmp, root, services, ws, owner)
    }

    /// Poll the persisted hook until `pred` holds or the timeout elapses.
    async fn wait_for_hook<F>(svc: &Services, id: &HookId, pred: F) -> Hook
    where
        F: Fn(&Hook) -> bool,
    {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let hook = svc.store().get_hook(id).await.expect("get hook");
            if pred(&hook) {
                return hook;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for hook state; last = {:?}",
                hook.state
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Poll the owner's persisted session until some message contains
    /// `needle`, returning the serialized messages. The wake lands after the
    /// terminal state persists, so a plain read can race it.
    async fn wait_for_wake(svc: &Services, owner: &AgentId, needle: &str) -> String {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let session = svc.store().get_agent_session(owner).await.unwrap();
            let text = serde_json::to_string(&session.messages).unwrap();
            if text.contains(needle) {
                return text;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "wake never contained {needle:?}; last = {text}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Persisted `hook:*` event types for a workspace, oldest-first.
    async fn hook_event_types(svc: &Services, ws: &WorkspaceId) -> Vec<String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let mut evs = svc
                .store()
                .query_events(&intent_store::EventQuery {
                    workspace_id: Some(ws.clone()),
                    ..Default::default()
                })
                .await
                .expect("query events");
            evs.retain(|e| e.event_type.starts_with("hook:"));
            evs.reverse();
            if !evs.is_empty() || std::time::Instant::now() >= deadline {
                return evs.into_iter().map(|e| e.event_type).collect();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn schedule_validates_name_delay_and_code() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Name too long (20 chars > 19 cap).
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "a".repeat(20), "code": "return;", "delayMs": 10_000 }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at most 19"), "{err}");
        // Delay below the floor.
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "ok", "code": "return;", "delayMs": 9_999 }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at least 10000"), "{err}");
        // Missing code.
        let err = svc
            .hook_schedule_op(&ws, &owner, &json!({ "name": "ok", "delayMs": 10_000 }))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("`code` is required"), "{err}");
        // Nothing persisted by any failed validation.
        let hooks = svc.store().list_hooks_by_agent(&owner).await.unwrap();
        assert!(hooks.is_empty());
    }

    #[tokio::test]
    async fn schedule_runs_immediately_and_registers_task() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "poller",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        assert_eq!(out["dispatched"], json!(false));
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        assert_eq!(hook.run_count, 1);
        assert!(hook.next_run_at.is_some(), "nextRunAt persisted");
        assert!(svc.hook_task_alive(&hook.hook_id), "task registered");
        // Immediate first run emitted run-completed + scheduled.
        let types = hook_event_types(&svc, &ws).await;
        assert!(types.contains(&HOOK_RUN_COMPLETED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_SCHEDULED.to_string()), "{types:?}");
        // list surfaces it.
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(listed["hooks"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn immediate_dispatch_short_circuits_schedule() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "one-shot",
                    "code": "return { dispatch: true, message: 'done already' };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        assert_eq!(out["dispatched"], json!(true));
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Dispatched);
        assert!(!svc.hook_task_alive(&hook.hook_id), "no task spawned");
        // Owner was woken with the dispatch message (store-only path — no
        // AgentManager attached in tests).
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let last = session.messages.last().expect("wake message persisted");
        let text = serde_json::to_string(&last.content).unwrap();
        assert!(text.contains("done already"), "{text}");
        let types = hook_event_types(&svc, &ws).await;
        assert!(types.contains(&HOOK_DISPATCHED.to_string()), "{types:?}");
    }

    #[tokio::test]
    async fn schedule_rejects_when_first_run_throws() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "broken",
                    "code": "throw new Error('boom');",
                    "delayMs": 10_000,
                }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("first run failed"), "{err}");
        assert!(err.to_string().contains("boom"), "{err}");
        let hooks = svc.store().list_hooks_by_agent(&owner).await.unwrap();
        assert!(hooks.is_empty(), "failed validation run persists nothing");
    }

    #[tokio::test]
    async fn per_agent_cap_is_enforced() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let svc = svc.with_hooks_max_per_agent(2);
        for i in 0..2 {
            svc.hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": format!("hook-{i}"),
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule under cap");
        }
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "one-too-many",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("max 2"), "{err}");
    }

    #[tokio::test]
    async fn run_now_dispatch_wakes_owner_and_terminates() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // The script polls a note through the real `ws.*` bindings: the first
        // (validation) run sees "wait" and continues; the test then flips the
        // note to "go" and triggers a `runNow`, exercising the periodic
        // re-run → dispatch contract without waiting out the 10s floor.
        let mut probe = note(&ws, "ci-note", "wait");
        svc.store().insert_note(&probe).await.unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "ci-watch",
                    "code": "const n = await ws.note.read('ci-note'); \
                             if (n.content.includes('go')) { \
                               return { dispatch: true, message: 'CI is green' }; \
                             } \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        probe.content = "go".to_string();
        svc.store().update_note(&probe).await.unwrap();
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        assert_eq!(hook.run_count, 2);
        assert!(hook.next_run_at.is_none());
        wait_for_wake(&svc, &owner, "CI is green").await;
        // Task deregistered after the terminal outcome.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while svc.hook_task_alive(&hook.hook_id) {
            assert!(std::time::Instant::now() < deadline, "task not removed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn throwing_run_evicts_and_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Seed a scheduled row directly (the throwing script would fail the
        // schedule-time validation run) and rehydrate to spawn its task.
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "will-throw".to_string(),
            code: "throw new Error('kaput');".to_string(),
            delay_ms: 10_000,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Evicted).await;
        assert!(hook.last_error.as_deref().unwrap().contains("kaput"));
        let types = hook_event_types(&svc, &ws).await;
        assert!(types.contains(&HOOK_EVICTED.to_string()), "{types:?}");
        let text = wait_for_wake(&svc, &owner, "evicted").await;
        assert!(text.contains("kaput"), "{text}");
    }

    #[tokio::test]
    async fn timeout_run_evicts_and_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let svc = svc.with_hook_eval_timeout(Duration::from_millis(200));
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "spinner".to_string(),
            code: "for (;;) {}".to_string(),
            delay_ms: 10_000,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Evicted).await;
        assert!(
            hook.last_error.as_deref().unwrap().contains("timed out"),
            "{:?}",
            hook.last_error
        );
        // Timeout kills the eval before the console capture can return: the
        // logs from that run are lost and last_logs stays untouched.
        assert_eq!(hook.last_logs, None);
        wait_for_wake(&svc, &owner, "evicted").await;
    }

    #[tokio::test]
    async fn cancel_stops_task_and_fe_cancel_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "cancel-me",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(svc.hook_task_alive(&hook.hook_id));
        // FE-initiated cancel (by_owner = false) → owner woken.
        let cancelled = svc
            .hook_cancel_op(&ws, &hook.hook_id, false)
            .await
            .expect("cancel");
        assert_eq!(cancelled["hook"]["state"], json!("cancelled"));
        assert!(!svc.hook_task_alive(&hook.hook_id), "task aborted");
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Cancelled);
        assert!(stored.next_run_at.is_none());
        let types = hook_event_types(&svc, &ws).await;
        assert!(types.contains(&HOOK_CANCELLED.to_string()), "{types:?}");
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(text.contains("cancelled from the app"), "{text}");
        // A second cancel fails: the hook is no longer active.
        let err = svc
            .hook_cancel_op(&ws, &hook.hook_id, true)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not active"), "{err}");
    }

    #[tokio::test]
    async fn owner_cancel_does_not_wake_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "self-cancel",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        svc.hook_cancel_op(&ws, &hook.hook_id, true)
            .await
            .expect("owner cancel");
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        assert!(
            session.messages.is_empty(),
            "owner-initiated cancel must not wake the owner"
        );
    }

    #[tokio::test]
    async fn rehydration_resumes_active_hooks_and_prunes_orphans() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let mk = |name: &str, state: HookState, agent: &AgentId| Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: agent.clone(),
            name: name.to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 10_000,
            state,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 3,
            last_error: None,
            last_logs: None,
        };
        let scheduled = mk("sched", HookState::Scheduled, &owner);
        let running = mk("mid-run", HookState::Running, &owner);
        let done = mk("done", HookState::Dispatched, &owner);
        // Owned by an agent whose session row is gone.
        let ghost_owner = AgentId::from("agent-ghost");
        svc.store()
            .insert_agent_session(&agent(&ws, "agent-ghost"))
            .await
            .unwrap();
        let orphan = mk("orphan", HookState::Scheduled, &ghost_owner);
        for h in [&scheduled, &running, &done, &orphan] {
            svc.store().insert_hook(h).await.unwrap();
        }
        svc.store()
            .set_agent_session_status(
                &ws,
                &ghost_owner,
                AgentStatus::Deleted,
                false,
                &now_iso(),
                None,
            )
            .await
            .unwrap();

        let resumed = svc.rehydrate_hooks().await.expect("rehydrate");
        assert_eq!(resumed, 2, "scheduled + running resume; others do not");
        assert!(svc.hook_task_alive(&scheduled.hook_id));
        assert!(svc.hook_task_alive(&running.hook_id));
        assert!(!svc.hook_task_alive(&done.hook_id));
        assert!(!svc.hook_task_alive(&orphan.hook_id));
        // The mid-run row was healed back to scheduled with a fresh countdown.
        let healed = svc.store().get_hook(&running.hook_id).await.unwrap();
        assert_eq!(healed.state, HookState::Scheduled);
        assert!(healed.next_run_at.is_some());
        // The orphan was cancelled.
        let orphaned = svc.store().get_hook(&orphan.hook_id).await.unwrap();
        assert_eq!(orphaned.state, HookState::Cancelled);
        // Idempotent: a second pass resumes nothing new.
        assert_eq!(svc.rehydrate_hooks().await.expect("second pass"), 0);
    }

    #[tokio::test]
    async fn hook_scripts_reach_ws_bindings() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // The script calls a real `ws.*` binding (workspace details) during
        // its immediate validation run, proving the prelude + host dispatch
        // are wired with the hook's workspace pinned (the same environment
        // that carries `ws.host.exec`).
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "ws-probe",
                    "code": "const d = await ws.workspace.details(); \
                             if (!d.id) throw new Error('no workspace'); \
                             return { dispatch: true, message: 'ws id ' + d.id };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        assert_eq!(out["dispatched"], json!(true));
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(text.contains(&format!("ws id {}", ws.0)), "{text}");
    }

    #[tokio::test]
    async fn console_logs_persist_on_continue_and_dispatch() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Continue run (the schedule-time validation run) captures logs.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "logger",
                    "code": "console.log('checked', 3, 'PRs'); \
                             console.warn({ ok: true }); \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(
            hook.last_logs.as_deref(),
            Some("checked 3 PRs\n{\"ok\":true}")
        );
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.last_logs, hook.last_logs);
        // hook.list serializes lastLogs.
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(
            listed["hooks"][0]["lastLogs"],
            json!("checked 3 PRs\n{\"ok\":true}")
        );

        // Dispatch run persists that run's logs and appends them to the wake.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "one-shot-log",
                    "code": "console.info('all green'); \
                             return { dispatch: true, message: 'done' };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.last_logs.as_deref(), Some("all green"));
        let text = wait_for_wake(&svc, &owner, "[hook logs]").await;
        assert!(text.contains("all green"), "{text}");
    }

    #[tokio::test]
    async fn no_logging_leaves_last_logs_null() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "silent",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.last_logs, None);
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(listed["hooks"][0].get("lastLogs"), None);
        // No `[hook logs]` section on a log-free run's wake path either.
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(!text.contains("[hook logs]"), "{text}");
    }

    #[tokio::test]
    async fn console_logs_persist_when_run_throws() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Seed a scheduled row directly (the throwing script would fail the
        // schedule-time validation run) and rehydrate to spawn its task.
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "log-then-throw".to_string(),
            code: "console.log('made it here'); throw new Error('kaput');".to_string(),
            delay_ms: 10_000,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Evicted).await;
        assert!(hook.last_error.as_deref().unwrap().contains("kaput"));
        assert_eq!(hook.last_logs.as_deref(), Some("made it here"));
        // The evict wake carries the logs section.
        let text = wait_for_wake(&svc, &owner, "[hook logs]").await;
        assert!(text.contains("made it here"), "{text}");
    }

    #[tokio::test]
    async fn console_buffer_truncates_from_the_head() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // 150 lines > the 100-line cap: the head is dropped and the marker
        // line prepended; the newest lines survive.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "chatty",
                    "code": "for (let i = 0; i < 150; i++) console.log('line', i); \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        let logs = hook.last_logs.expect("logs captured");
        assert!(
            logs.starts_with("[earlier log lines truncated]\n"),
            "{logs}"
        );
        assert!(!logs.contains("line 49\n"), "head dropped: {logs}");
        assert!(logs.contains("line 50"), "{logs}");
        assert!(logs.ends_with("line 149"), "{logs}");
        let lines = logs.lines().count();
        assert_eq!(lines, 101, "100 kept + marker, got {lines}");
    }

    #[test]
    fn wake_logs_section_is_head_truncated() {
        assert_eq!(with_wake_logs("msg", None), "msg");
        assert_eq!(with_wake_logs("msg", Some("")), "msg");
        assert_eq!(
            with_wake_logs("msg", Some("a\nb")),
            "msg\n\n[hook logs]\na\nb"
        );
        let long = "x".repeat(HOOK_WAKE_LOGS_CAP + 100);
        let out = with_wake_logs("msg", Some(&long));
        assert!(out.starts_with("msg\n\n[hook logs]\n[earlier log lines truncated]\n"));
        assert!(out.ends_with(&"x".repeat(HOOK_WAKE_LOGS_CAP)));
        assert_eq!(
            out.len(),
            "msg\n\n[hook logs]\n[earlier log lines truncated]\n".len() + HOOK_WAKE_LOGS_CAP
        );
    }
}
