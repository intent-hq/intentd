//! Script monitors share the definition admission lock with start/restart/stop.
//! The shorter monitor lane orders output, TTL and terminal decisions; teardown
//! reserves its outcome then releases that lane before joining a supervisor.
use super::{
    json, now_iso, publish_event, script_event, Error, Ordering, PtyId, Result, ScriptManager,
    SpawnSpec, Value, WorkspaceId, LOST_AT_DAEMON_STOP_ERROR,
};
use intent_core::script_output::LineDecoder;
use intent_core::{AgentId, ScriptLastRun, ScriptMonitor, ScriptMonitorTrigger, ScriptRunOutcome};
use intent_pty::OutputChunk;

pub(crate) struct Window {
    row: ScriptMonitor,
    pattern: Option<regex::Regex>,
    decoder: LineDecoder,
    attempt: Option<PtyId>,
    cursor: u64,
    count: u32,
    cancelled: bool,
    closed: bool,
    pending_terminal: Option<ScriptMonitor>,
}

fn invalid(message: &str) -> Error {
    Error::InvalidParams(message.into())
}

struct Options {
    ttl: i64,
    run: Option<String>,
    pattern: Option<regex::Regex>,
    text: Option<String>,
    count: Option<u32>,
}
impl Options {
    fn parse(value: &Value) -> Result<Self> {
        let ttl = value
            .get("ttlMs")
            .and_then(Value::as_i64)
            .filter(|v| (1..=86_400_000).contains(v))
            .ok_or_else(|| invalid("ttlMs must be an integer from 1 to 86400000"))?;
        let run = value
            .get("runId")
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| invalid("runId must be a nonempty string"))
            })
            .transpose()?;
        let text = value.get("outputPattern").map(|v| v.as_str().filter(|s|!s.is_empty() && s.len() <= 1024 && !s.contains(['\r','\n'])).map(str::to_owned).ok_or_else(||invalid("outputPattern must be a nonempty single-line regex of at most 1024 UTF-8 bytes"))).transpose()?;
        let pattern = text.as_ref().map(|s| regex::RegexBuilder::new(s).size_limit(1024*1024).dfa_size_limit(1024*1024).nest_limit(128).build().map_err(|_|invalid("outputPattern has invalid or unsupported syntax, or exceeds regex compilation limits"))).transpose()?;
        let count = value
            .get("lineCount")
            .map(|v| {
                v.as_u64()
                    .filter(|n| (1..=1_000_000).contains(n))
                    .map(|n| u32::try_from(n).expect("bounded threshold"))
                    .ok_or_else(|| invalid("lineCount must be an integer from 1 to 1000000"))
            })
            .transpose()?;
        Ok(Self {
            ttl,
            run,
            pattern,
            text,
            count,
        })
    }
}

impl ScriptManager {
    fn monitor_now(&self) -> chrono::DateTime<chrono::Utc> {
        self.parks
            .monitor_clock
            .as_ref()
            .and_then(|clock| chrono::DateTime::from_timestamp_millis(clock.load(Ordering::SeqCst)))
            .unwrap_or_else(chrono::Utc::now)
    }
    async fn monitor_inactive_reason(
        &self,
        ws: &WorkspaceId,
        agent: &AgentId,
    ) -> Result<Option<&'static str>> {
        match self.store.get_agent_session_summary(agent).await {
            Err(Error::NotFound(_)) => return Ok(Some("owner-deleted")),
            Err(error) => return Err(error),
            Ok(owner) if owner.workspace_id != *ws => return Ok(Some("owner-deleted")),
            Ok(_) => {}
        }
        if self
            .store
            .get_agent_session_retired_at(agent)
            .await?
            .is_some()
        {
            return Ok(Some("owner-retired"));
        }
        if !ws.is_chief() {
            match self.store.get_workspace(ws).await {
                Err(Error::NotFound(_)) => return Ok(Some("workspace-deleted")),
                Err(error) => return Err(error),
                Ok(workspace) if workspace.archived => return Ok(Some("workspace-archived")),
                Ok(_) => {}
            }
        }
        Ok(None)
    }

    async fn eligible_monitor_owner(&self, ws: &WorkspaceId, agent: &AgentId) -> Result<()> {
        let owner = self.store.get_agent_session_summary(agent).await?;
        if owner.workspace_id != *ws
            || crate::agent_ops::is_terminal_status(owner.status)
            || self
                .store
                .get_agent_session_retired_at(agent)
                .await?
                .is_some()
        {
            return Err(invalid(
                "script monitor owner is inactive or outside this workspace",
            ));
        }
        if !ws.is_chief() && self.store.get_workspace(ws).await?.archived {
            return Err(invalid("workspace is archived"));
        }
        Ok(())
    }

    pub(crate) async fn monitor(
        &self,
        ws: &WorkspaceId,
        agent: &AgentId,
        id: &str,
        options: Value,
    ) -> Result<Value> {
        let options = Options::parse(&options)?;
        let definition = self.locks.definition_lock(id);
        let _definition = definition.lock().await;
        let _lane = self.locks.monitor_lane.lock().await;
        self.eligible_monitor_owner(ws, agent).await?;
        let rows = self.store.script_monitors(ws, None).await?;
        if let Some(row) = rows
            .iter()
            .find(|m| m.script_id == id && m.state == "active")
        {
            if row.agent_id != *agent {
                let mut response = json!({"ok":false,"refused":true,"reason":"already-monitored","ownerAgentId":row.agent_id,"monitorId":row.monitor_id,"workspaceId":ws,"scriptId":id,"runId":row.run_id,"instruction":"Ask the owner to relay results or stop monitoring before registering."});
                if let Ok(owner) = self.store.get_agent_session_summary(&row.agent_id).await {
                    response["ownerAgentName"] = json!(owner.name);
                }
                return Ok(response);
            }
            if options.run.as_ref().is_some_and(|run| run != &row.run_id) {
                return Err(invalid("runId differs from the active monitor"));
            }
            return Ok(json!({"ok":true,"monitor":row}));
        }
        let latest = self.store.latest_script_run(ws, id).await?;
        let run = options
            .run
            .as_ref()
            .or_else(|| latest.as_ref().map(|(run, _)| run));
        if let Some(row) = rows.iter().rev().find(|m| {
            m.agent_id == *agent
                && m.script_id == id
                && m.state == "completed"
                && Some(&m.run_id) == run
        }) {
            return Ok(json!({"ok":true,"monitor":row}));
        }
        let (run, result) = latest.ok_or_else(|| invalid("script has no accepted run"))?;
        if options.run.as_ref().is_some_and(|r| r != &run) {
            return Err(invalid("runId is unavailable"));
        }
        let def = self
            .store
            .get_script_in_workspace(ws, id)
            .await?
            .ok_or_else(|| invalid("unknown script"))?;
        let cutoff = (chrono::Utc::now() - chrono::Duration::days(7))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        self.store.prune_script_monitors(ws, &cutoff, true).await?;
        let rows = self.store.script_monitors(ws, None).await?;
        if rows.len() >= 1000 {
            return Err(invalid(
                "workspace retained script monitor limit 1000 exhausted",
            ));
        }
        if rows
            .iter()
            .filter(|m| m.agent_id == *agent && m.state == "active")
            .count()
            >= 5
        {
            return Err(invalid("owner active script monitor limit 5 exhausted"));
        }
        let created = self.monitor_now();
        let mut row = ScriptMonitor {
            monitor_id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws.clone(),
            agent_id: agent.clone(),
            script_id: id.into(),
            run_id: run,
            script_name: def.name,
            mode: def.mode,
            state: "active".into(),
            created_at: created.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            expires_at: (created + chrono::Duration::milliseconds(options.ttl))
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            output_pattern: options.text,
            line_count: options.count,
            settled_at: None,
            reason: None,
            result: None,
            trigger: None,
        };
        let attempt = self
            .scripts
            .lock()
            .unwrap()
            .get(&(ws.clone(), id.to_owned()))
            .and_then(|m| m.monitor_attempt);
        let (cursor, decoder) = if result.is_some() {
            (0, LineDecoder::new(true))
        } else if let Some(pty) = attempt {
            match self.pty.observation_cursor(pty) {
                Ok(window) => window,
                // A service can be between attempts after its old PTY was
                // removed. Its next spawn installs a fresh window under this lane.
                Err(Error::NotFound(_)) => (0, LineDecoder::new(true)),
                Err(error) => return Err(error),
            }
        } else {
            (0, LineDecoder::new(true))
        };
        self.store.insert_script_monitor(&row).await?;
        self.locks.monitor_windows.lock().unwrap().insert(
            row.monitor_id.clone(),
            Window {
                row: row.clone(),
                pattern: options.pattern,
                decoder,
                attempt,
                cursor,
                count: 0,
                cancelled: false,
                closed: false,
                pending_terminal: None,
            },
        );
        self.emit_monitor(&row, "registered").await;
        if let Some(result) = result {
            self.complete_monitor(&mut row, result).await?;
        }
        if let Some(services) = &self.owner_services {
            services.start_script_monitor_maintenance();
        }

        Ok(json!({"ok":true,"monitor":row}))
    }

    async fn emit_monitor(&self, row: &ScriptMonitor, kind: &str) {
        publish_event(
            self.bus.as_ref(),
            script_event(
                &row.workspace_id,
                &format!("scriptMonitor:{kind}"),
                json!({"monitor":row}),
            ),
        )
        .await;
    }

    async fn commit_monitor(&self, row: &ScriptMonitor) -> Result<()> {
        let committed = match self.store.settle_script_monitor(row).await {
            Ok(committed) => committed,
            Err(error) => {
                if let Some(window) = self
                    .locks
                    .monitor_windows
                    .lock()
                    .unwrap()
                    .get_mut(&row.monitor_id)
                {
                    window.pending_terminal = Some(row.clone());
                }
                return Err(error);
            }
        };
        if committed {
            self.locks
                .monitor_windows
                .lock()
                .unwrap()
                .remove(&row.monitor_id);
            self.emit_monitor(row, &row.state).await;
            if let Some(services) = self.owner_services.clone() {
                let row = row.clone();
                tokio::spawn(async move {
                    services.dispatch_script_monitor(&row).await;
                });
            }
        }
        Ok(())
    }

    async fn complete_monitor(
        &self,
        row: &mut ScriptMonitor,
        mut result: ScriptLastRun,
    ) -> Result<()> {
        result.run_id = None;
        row.state = "completed".into();
        row.reason = Some("finished".into());
        row.settled_at = Some(now_iso());
        row.result = Some(result);
        self.commit_monitor(row).await
    }

    /// Pre-input arbitration: lifecycle, cancellation reservation, durable result, TTL.
    async fn reconcile_locked(&self, row: &mut ScriptMonitor) -> Result<bool> {
        if row.state != "active" {
            return Ok(true);
        }
        let committed = self
            .store
            .script_monitor(&row.workspace_id, &row.monitor_id)
            .await?;
        if committed.state != "active" {
            *row = committed;
            self.locks
                .monitor_windows
                .lock()
                .unwrap()
                .remove(&row.monitor_id);
            return Ok(true);
        }
        if let Some(reason) = self
            .monitor_inactive_reason(&row.workspace_id, &row.agent_id)
            .await?
        {
            row.state = "cancelled".into();
            row.reason = Some(reason.into());
            row.settled_at = Some(now_iso());
            self.commit_monitor(row).await?;
            return Ok(true);
        }
        if self
            .locks
            .monitor_windows
            .lock()
            .unwrap()
            .get(&row.monitor_id)
            .is_some_and(|w| w.cancelled)
        {
            return Ok(true);
        }
        let pending = self
            .locks
            .monitor_windows
            .lock()
            .unwrap()
            .get(&row.monitor_id)
            .and_then(|w| w.pending_terminal.clone());
        if let Some(terminal) = pending {
            self.commit_monitor(&terminal).await?;
            *row = terminal;
            return Ok(true);
        }
        if let Some((run, Some(result))) = self
            .store
            .latest_script_run(&row.workspace_id, &row.script_id)
            .await?
        {
            if run == row.run_id {
                self.complete_monitor(row, result).await?;
                return Ok(true);
            }
        }
        if self.monitor_now()
            >= chrono::DateTime::parse_from_rfc3339(&row.expires_at)
                .map_err(|e| Error::Internal(e.to_string()))?
        {
            row.state = "expired".into();
            row.reason = Some("ttl-expired".into());
            row.settled_at = Some(now_iso());
            self.commit_monitor(row).await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn cleanup_monitors(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
        reason: &str,
    ) -> Result<()> {
        let _lane = self.locks.monitor_lane.lock().await;
        for mut row in self.store.script_monitors(ws, agent).await? {
            if row.state == "active" {
                row.state = "cancelled".into();
                row.reason = Some(reason.into());
                row.settled_at = Some(now_iso());
                self.commit_monitor(&row).await?;
            }
            self.store
                .finish_script_monitor_wake(&row.monitor_id, true)
                .await?;
            self.locks
                .monitor_windows
                .lock()
                .unwrap()
                .remove(&row.monitor_id);
        }
        Ok(())
    }

    pub(crate) async fn recover_monitors(&self) -> Result<()> {
        let _lane = self.locks.monitor_lane.lock().await;
        let pending_runs = self.store.pending_script_runs().await?;
        for (mut row, cancel_intent) in self.store.pending_script_monitors().await? {
            if self
                .locks
                .monitor_windows
                .lock()
                .unwrap()
                .contains_key(&row.monitor_id)
            {
                continue;
            }
            if let Some(reason) = self
                .monitor_inactive_reason(&row.workspace_id, &row.agent_id)
                .await?
            {
                if row.state == "active" {
                    row.state = "cancelled".into();
                    row.reason = Some(reason.into());
                    row.settled_at = Some(now_iso());
                    self.commit_monitor(&row).await?;
                }
                self.store
                    .finish_script_monitor_wake(&row.monitor_id, true)
                    .await?;
                continue;
            }
            if row.state == "active" && (cancel_intent || !self.reconcile_locked(&mut row).await?) {
                let recorded = self
                    .store
                    .latest_script_run(&row.workspace_id, &row.script_id)
                    .await?
                    .and_then(|(run, result)| (run == row.run_id).then_some(result).flatten())
                    .filter(|r| !cancel_intent || r.outcome == ScriptRunOutcome::Cancelled);
                let started_at = pending_runs
                    .iter()
                    .find(|(ws, id, run, _)| {
                        ws == &row.workspace_id && id == &row.script_id && run == &row.run_id
                    })
                    .and_then(|(_, _, _, started)| started.clone());
                let result = recorded.unwrap_or_else(|| ScriptLastRun {
                    run_id: None,
                    outcome: if cancel_intent {
                        ScriptRunOutcome::Cancelled
                    } else {
                        ScriptRunOutcome::Interrupted
                    },
                    exit_code: if cancel_intent { None } else { Some(-1) },
                    started_at,
                    stopped_at: now_iso(),
                    error: Some(
                        if cancel_intent {
                            "cancelled run interrupted by daemon restart"
                        } else {
                            LOST_AT_DAEMON_STOP_ERROR
                        }
                        .into(),
                    ),
                });
                self.complete_monitor(&mut row, result).await?;
            } else if row.state != "active" {
                if let Some(services) = self.owner_services.clone() {
                    tokio::spawn(async move {
                        services.dispatch_script_monitor(&row).await;
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn reconcile_monitor(&self, ws: &WorkspaceId, id: &str) -> Result<()> {
        let _lane = self.locks.monitor_lane.lock().await;
        let mut row = self.store.script_monitor(ws, id).await?;
        self.reconcile_locked(&mut row).await?;
        Ok(())
    }

    pub(crate) async fn cancel_monitor(
        &self,
        ws: &WorkspaceId,
        id: &str,
        owner: Option<&AgentId>,
        stop: bool,
    ) -> Result<Value> {
        let mgr = self.clone();
        let ws = ws.clone();
        let id = id.to_owned();
        let owner = owner.cloned();
        self.spawn_owned(async move {
            mgr.cancel_monitor_owned(&ws, &id, owner.as_ref(), stop)
                .await
        })
        .await
        .map_err(|e| Error::Internal(format!("monitor cancellation task failed: {e}")))?
    }

    async fn cancel_monitor_owned(
        &self,
        ws: &WorkspaceId,
        id: &str,
        owner: Option<&AgentId>,
        stop: bool,
    ) -> Result<Value> {
        let initial = self.store.script_monitor(ws, id).await?;
        let definition = self.locks.definition_lock(&initial.script_id);
        let _definition = definition.lock().await;
        let lane = self.locks.monitor_lane.lock().await;
        let mut row = self.store.script_monitor(ws, id).await?;
        if owner.is_some_and(|a| a != &row.agent_id) {
            return Err(invalid(&format!("monitor belongs to {}", row.agent_id)));
        }
        if self.reconcile_locked(&mut row).await? {
            return Ok(if stop {
                json!({"ok":true,"monitor":row,"runStopped":false})
            } else {
                json!({"ok":true,"monitor":row})
            });
        }
        if !stop {
            row.state = "cancelled".into();
            row.reason = Some("unmonitored".into());
            row.settled_at = Some(now_iso());
            self.commit_monitor(&row).await?;
            return Ok(json!({"ok":true,"monitor":row}));
        }
        let current = self
            .scripts
            .lock()
            .unwrap()
            .get(&(ws.clone(), row.script_id.clone()))
            .and_then(|m| m.run_id.clone());
        if current.as_ref() != Some(&row.run_id) {
            self.complete_monitor(
                &mut row,
                ScriptLastRun {
                    run_id: None,
                    outcome: ScriptRunOutcome::Interrupted,
                    exit_code: Some(-1),
                    started_at: None,
                    stopped_at: now_iso(),
                    error: Some("bound run is no longer available".into()),
                },
            )
            .await?;
            return Ok(json!({"ok":true,"monitor":row,"runStopped":false}));
        }
        self.store
            .script_monitor_cancel_intent(ws, id, true)
            .await?;
        if let Some(window) = self.locks.monitor_windows.lock().unwrap().get_mut(id) {
            window.cancelled = true;
        }
        if let Some(managed) = self
            .scripts
            .lock()
            .unwrap()
            .get_mut(&(ws.clone(), row.script_id.clone()))
        {
            managed.pending_result = None;
        }
        drop(lane);
        let stopped = self.stop_inner(ws, &row.script_id, true).await;
        let _lane = self.locks.monitor_lane.lock().await;
        if let Err(error) = stopped {
            self.store
                .script_monitor_cancel_intent(ws, id, false)
                .await?;
            if let Some(window) = self.locks.monitor_windows.lock().unwrap().get_mut(id) {
                window.cancelled = false;
            }
            return Err(error);
        }
        let result = self
            .store
            .latest_script_run(ws, &row.script_id)
            .await
            .and_then(|latest| {
                latest
                    .and_then(|(run, result)| (run == row.run_id).then_some(result).flatten())
                    .ok_or_else(|| Error::Internal("cancelled run has not durably settled".into()))
            });
        // Teardown has finished. A failed terminal write reserves its captured
        // decision in pending_terminal; do not leave it behind the in-flight
        // cancellation flag forever. The durable intent still fences recovery.
        if let Some(window) = self.locks.monitor_windows.lock().unwrap().get_mut(id) {
            window.cancelled = false;
        }
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.store
                    .script_monitor_cancel_intent(ws, id, false)
                    .await?;
                return Err(error);
            }
        };
        self.complete_monitor(&mut row, result).await?;
        Ok(json!({"ok":true,"monitor":row,"runStopped":true}))
    }

    pub(super) async fn monitor_gap(&self, ws: &WorkspaceId, id: &str, pty: PtyId) {
        let _lane = self.locks.monitor_lane.lock().await;
        for window in self
            .locks
            .monitor_windows
            .lock()
            .unwrap()
            .values_mut()
            .filter(|w| {
                w.row.workspace_id == *ws && w.row.script_id == id && w.attempt == Some(pty)
            })
        {
            window.decoder.gap();
        }
    }

    pub(super) async fn spawn_monitored(
        &self,
        ws: &WorkspaceId,
        id: &str,
        generation: u64,
        spec: SpawnSpec,
    ) -> Result<PtyId> {
        if let Some(park) = &self.parks.before_spawn {
            park.entered.notify_one();
            park.release.notified().await;
        }
        let _lane = self.locks.monitor_lane.lock().await;
        if self
            .scripts
            .lock()
            .unwrap()
            .get(&(ws.clone(), id.into()))
            .is_none_or(|m| m.generation != generation || m.stopped_by_user)
        {
            return Err(Error::InvalidParams("script launch was cancelled".into()));
        }
        let pty = self.pty.spawn(spec)?;
        if let Some(m) = self
            .scripts
            .lock()
            .unwrap()
            .get_mut(&(ws.clone(), id.into()))
            .filter(|m| m.generation == generation)
        {
            m.monitor_attempt = Some(pty);
        }
        for window in self
            .locks
            .monitor_windows
            .lock()
            .unwrap()
            .values_mut()
            .filter(|w| w.row.workspace_id == *ws && w.row.script_id == id)
        {
            window.attempt = Some(pty);
            window.cursor = 0;
            window.decoder = LineDecoder::new(true);
            window.closed = false;
        }
        Ok(pty)
    }

    pub(super) async fn observe_monitor_chunk(
        &self,
        ws: &WorkspaceId,
        id: &str,
        pty: PtyId,
        chunk: &OutputChunk,
    ) {
        if !self
            .locks
            .monitor_windows
            .lock()
            .unwrap()
            .values()
            .any(|w| {
                w.row.workspace_id == *ws
                    && w.row.script_id == id
                    && (w.row.output_pattern.is_some() || w.row.line_count.is_some())
            })
        {
            return;
        }
        let mut start = 0;
        while start < chunk.len() {
            let mut end = start;
            let mut delimiters = 0;
            while end < chunk.len() && end - start < 16 * 1024 && delimiters < 64 {
                if matches!(chunk[end], b'\r' | b'\n') {
                    delimiters += 1;
                }
                end += 1;
            }
            let offset = chunk.start_offset + start as u64;
            let slice = OutputChunk {
                bytes: chunk[start..end].to_vec(),
                start_offset: offset,
                end_offset: chunk.start_offset + end as u64,
            };
            if let Err(error) = self.monitor_output(ws, id, pty, Some(&slice), false).await {
                tracing::warn!(%error,"monitor output failed");
                break;
            }
            start = end;
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn reconcile_script_monitors_locked(
        &self,
        ws: &WorkspaceId,
        id: &str,
    ) -> Result<()> {
        for mut row in self
            .store
            .script_monitors(ws, None)
            .await?
            .into_iter()
            .filter(|m| m.script_id == id && m.state == "active")
        {
            self.reconcile_locked(&mut row).await?;
        }
        Ok(())
    }

    pub(super) async fn monitor_output(
        &self,
        ws: &WorkspaceId,
        id: &str,
        pty: PtyId,
        chunk: Option<&OutputChunk>,
        eof: bool,
    ) -> Result<()> {
        let _lane = self.locks.monitor_lane.lock().await;
        let selected = self
            .locks
            .monitor_windows
            .lock()
            .unwrap()
            .values()
            .find(|w| w.row.workspace_id == *ws && w.row.script_id == id)
            .map(|w| w.row.clone());
        let Some(mut row) = selected else {
            return Ok(());
        };
        if self.reconcile_locked(&mut row).await? {
            return Ok(());
        }
        if row.output_pattern.is_none() && row.line_count.is_none() {
            return Ok(());
        }
        let mut trigger = None;
        {
            let mut windows = self.locks.monitor_windows.lock().unwrap();
            let Some(w) = windows.get_mut(&row.monitor_id) else {
                return Ok(());
            };
            if w.attempt != Some(pty) || w.closed {
                return Ok(());
            }
            if eof {
                w.closed = true;
            }
            let mut evaluate = |line: intent_core::script_output::Line| {
                w.count = w.count.saturating_add(1).min(i32::MAX as u32);
                if line
                    .text
                    .as_ref()
                    .is_some_and(|text| w.pattern.as_ref().is_some_and(|p| p.is_match(text)))
                {
                    Some((
                        "output-match",
                        ScriptMonitorTrigger {
                            observed_line_count: w.count,
                            matched_line: line.text,
                        },
                    ))
                } else if w
                    .row
                    .line_count
                    .is_some_and(|threshold| w.count >= threshold)
                {
                    Some((
                        "line-count",
                        ScriptMonitorTrigger {
                            observed_line_count: w.count,
                            matched_line: None,
                        },
                    ))
                } else {
                    None
                }
            };
            if let Some(chunk) = chunk {
                if chunk.start_offset > w.cursor {
                    w.decoder.gap();
                }
                let skip = usize::try_from(w.cursor.saturating_sub(chunk.start_offset))
                    .unwrap_or(usize::MAX)
                    .min(chunk.len());
                for byte in &chunk[skip..] {
                    if let Some(line) = w.decoder.push(*byte) {
                        if let Some(hit) = evaluate(line) {
                            trigger = Some(hit);
                            break;
                        }
                    }
                }
                w.cursor = w.cursor.max(chunk.end_offset);
            }
            if eof && trigger.is_none() {
                if let Some(line) = w.decoder.eof() {
                    trigger = evaluate(line);
                }
            }
        }
        if let Some((reason, hit)) = trigger {
            row.state = "triggered".into();
            row.reason = Some(reason.into());
            row.trigger = Some(hit);
            row.settled_at = Some(now_iso());
            self.commit_monitor(&row).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_options_and_bounded_rust_regex() {
        for value in [
            json!({}),
            json!({"ttlMs":null}),
            json!({"ttlMs":1.5}),
            json!({"ttlMs":0}),
            json!({"ttlMs":86_400_001}),
            json!({"ttlMs":1,"runId":null}),
            json!({"ttlMs":1,"lineCount":0}),
            json!({"ttlMs":1,"outputPattern":"(?=x)"}),
            json!({"ttlMs":1,"outputPattern":"(a)\\1"}),
            json!({"ttlMs":1,"outputPattern":"a\nb"}),
            json!({"ttlMs":1,"outputPattern":"x".repeat(1025)}),
            json!({"ttlMs":1,"outputPattern":format!("{}x{}","(".repeat(129),")".repeat(129))}),
        ] {
            assert!(Options::parse(&value).is_err(), "{value}");
        }
        let options =
            Options::parse(&json!({"ttlMs":1,"outputPattern":"(?i)^é+$","lineCount":1_000_000}))
                .unwrap();
        assert!(options.pattern.unwrap().is_match("Éé"));
    }
}
