//! Turn-end safety net for agent script waits.
//!
//! Agents start a long command-mode script with `ws.script.start`, end their
//! turn saying they will continue "when it finishes", and arm nothing that
//! would wake them; the work then stalls until a human or coordinator
//! notices (three stalls in one day in the "proper-implement" workspace,
//! intentd#2588 / intentd#2592 / intent#7559). `ws.script.monitor` exists but
//! is opt-in.
//!
//! The MCP binding records which agent started each run
//! ([`Services::record_agent_script_start`]). When that agent's turn ends
//! normally (`agent:idle`), every recorded run that is still live, is
//! command-mode, and is not already covered — no active script monitor on the
//! script, and no active hook of the agent whose code names the script — gets
//! a script monitor owned by the agent. The existing monitor machinery then
//! wakes the agent with the run's exit status. Service-mode scripts are never
//! auto-monitored: they are meant to keep running.
//!
//! Records are one-shot: a run is armed (or found finished/covered) once and
//! forgotten. A run whose agent still has queued messages is kept for the
//! next idle, since the agent is about to take another turn anyway.

use crate::events::SubscriptionFilter;
use crate::Services;
use intent_core::events::AGENT_IDLE;
use intent_core::{AgentId, Event, HookState, ScriptMode, WorkspaceId};
use serde_json::{json, Value};

/// Lifetime of an auto-armed monitor. Long enough for a slow gate on a
/// heavily loaded host; on expiry the agent is still woken (state
/// `expired`), so the wait is never silently lost.
pub(crate) const AUTO_SCRIPT_MONITOR_TTL_MS: i64 = 6 * 60 * 60 * 1000;

/// Per-agent bound on remembered runs; the oldest record is dropped first.
const MAX_RECORDS_PER_AGENT: usize = 32;

/// One agent-started run awaiting the agent's next turn end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StartedRun {
    pub(crate) workspace: WorkspaceId,
    pub(crate) script: String,
    pub(crate) run: String,
}

/// What the turn-end check decided for one recorded run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WakeDecision {
    /// A monitor was registered; carries its id.
    Armed(String),
    /// The run already finished or was replaced; nothing to wait for.
    NotLive,
    /// Service-mode (or deleted) script; never auto-monitored.
    NotCommand,
    /// An active script monitor already covers the script.
    Monitored,
    /// An active hook of the agent names the script.
    HookCovers,
    /// The agent has queued messages; re-checked at its next turn end.
    Deferred,
    /// Monitor registration was refused or failed.
    Refused(String),
}

impl Services {
    /// Remember that `agent` started `run_id` of `script_id`. A newer run of
    /// the same script replaces the older record.
    pub(crate) fn record_agent_script_start(
        &self,
        workspace_id: &WorkspaceId,
        agent: &AgentId,
        script_id: &str,
        run_id: &str,
    ) {
        let mut starters = self
            .script_starters
            .lock()
            .expect("script starter registry poisoned");
        let runs = starters.entry(agent.clone()).or_default();
        runs.retain(|r| !(r.workspace == *workspace_id && r.script == script_id));
        runs.push(StartedRun {
            workspace: workspace_id.clone(),
            script: script_id.to_owned(),
            run: run_id.to_owned(),
        });
        if runs.len() > MAX_RECORDS_PER_AGENT {
            let excess = runs.len() - MAX_RECORDS_PER_AGENT;
            runs.drain(..excess);
        }
    }

    /// Spawn the `agent:idle` subscriber that arms missing script wakes.
    /// No-op-safe without an event bus.
    pub fn spawn_script_wake_guard_loop(&self) -> tokio::task::JoinHandle<()> {
        let Some(bus) = self.event_bus.clone() else {
            tracing::info!("script wake guard disabled: no event bus");
            return intent_core::spawn_daemon(async {});
        };
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let mut sub = bus.subscribe(SubscriptionFilter {
                event_types: vec![AGENT_IDLE.to_string()],
                ..Default::default()
            });
            loop {
                let events = tokio::select! {
                    biased;
                    () = services.settings_tasks.closed() => break,
                    events = sub.recv() => events,
                };
                let Some(events) = events else { break };
                for event in events {
                    if services.settings_tasks.is_closed() {
                        break;
                    }
                    services.handle_agent_idle_script_wake(&event).await;
                }
            }
        })
    }

    /// One `agent:idle` event. Only a normal turn end arms wakes: a
    /// cancelled or failed turn has a human or the failure path in charge.
    pub(crate) async fn handle_agent_idle_script_wake(
        &self,
        event: &Event,
    ) -> Vec<(StartedRun, WakeDecision)> {
        if event.event_type != AGENT_IDLE {
            return Vec::new();
        }
        let Some(agent) = event.data.get("agentId").and_then(Value::as_str) else {
            return Vec::new();
        };
        let finish = event.data.get("finishReason").and_then(Value::as_str);
        if !matches!(
            finish,
            Some("end_turn" | "max_tokens" | "max_turn_requests" | "stream_complete")
        ) {
            return Vec::new();
        }
        self.arm_idle_script_wakes(&AgentId::from(agent)).await
    }

    /// Decide every recorded run of `agent` and arm the uncovered live ones.
    pub(crate) async fn arm_idle_script_wakes(
        &self,
        agent: &AgentId,
    ) -> Vec<(StartedRun, WakeDecision)> {
        let runs = self
            .script_starters
            .lock()
            .expect("script starter registry poisoned")
            .remove(agent)
            .unwrap_or_default();
        if runs.is_empty() {
            return Vec::new();
        }
        let queued = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent)
            .is_some_and(|q| !q.is_empty());
        let mut decisions = Vec::with_capacity(runs.len());
        let mut kept = Vec::new();
        for run in runs {
            let decision = self.decide_script_wake(agent, &run, queued).await;
            if decision == WakeDecision::Deferred {
                kept.push(run.clone());
            }
            match &decision {
                WakeDecision::Armed(monitor_id) => tracing::info!(
                    agent = %agent.0,
                    workspace = %run.workspace.0,
                    script = %run.script,
                    run = %run.run,
                    monitor = %monitor_id,
                    "turn ended with a live unwatched script; auto-armed a completion wake"
                ),
                WakeDecision::Refused(reason) => tracing::warn!(
                    agent = %agent.0,
                    script = %run.script,
                    run = %run.run,
                    %reason,
                    "auto script wake not armed"
                ),
                _ => {}
            }
            decisions.push((run, decision));
        }
        if !kept.is_empty() {
            let mut starters = self
                .script_starters
                .lock()
                .expect("script starter registry poisoned");
            let entry = starters.entry(agent.clone()).or_default();
            // A start recorded while we were deciding is newer; keep it.
            for run in kept {
                if !entry
                    .iter()
                    .any(|r| r.workspace == run.workspace && r.script == run.script)
                {
                    entry.push(run);
                }
            }
        }
        decisions
    }

    async fn decide_script_wake(
        &self,
        agent: &AgentId,
        run: &StartedRun,
        queued: bool,
    ) -> WakeDecision {
        let ws = &run.workspace;
        if !self
            .script_manager()
            .run_is_live(ws.as_str(), &run.script, &run.run)
        {
            return WakeDecision::NotLive;
        }
        match self.store.get_script_in_workspace(ws, &run.script).await {
            Ok(Some(def)) if def.mode == ScriptMode::Command => {}
            Ok(_) => return WakeDecision::NotCommand,
            Err(e) => return WakeDecision::Refused(e.to_string()),
        }
        match self.store.script_monitors(ws, None).await {
            Ok(rows) => {
                if rows
                    .iter()
                    .any(|m| m.script_id == run.script && m.state == "active")
                {
                    return WakeDecision::Monitored;
                }
            }
            Err(e) => return WakeDecision::Refused(e.to_string()),
        }
        match self.store.list_hooks_by_agent(agent).await {
            Ok(hooks) => {
                if hooks.iter().any(|h| {
                    h.workspace_id == *ws
                        && matches!(h.state, HookState::Scheduled | HookState::Running)
                        && h.code.contains(run.script.as_str())
                }) {
                    return WakeDecision::HookCovers;
                }
            }
            Err(e) => return WakeDecision::Refused(e.to_string()),
        }
        if queued {
            return WakeDecision::Deferred;
        }
        let options = json!({"ttlMs": AUTO_SCRIPT_MONITOR_TTL_MS, "runId": run.run});
        match intent_core::WorkspaceApi::script_monitor(
            self,
            ws.clone(),
            agent.clone(),
            run.script.clone(),
            options,
        )
        .await
        {
            Ok(reply) if reply["ok"] == json!(true) => WakeDecision::Armed(
                reply["monitor"]["monitorId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            ),
            Ok(reply) if reply["reason"] == json!("run-finished") => WakeDecision::NotLive,
            Ok(reply) if reply["reason"] == json!("already-monitored") => WakeDecision::Monitored,
            Ok(reply) => WakeDecision::Refused(reply.to_string()),
            Err(e) => WakeDecision::Refused(e.to_string()),
        }
    }
}
