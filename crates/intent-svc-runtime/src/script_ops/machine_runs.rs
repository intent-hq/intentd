//! Machine runs (`remoteScripts: 1`): a remote agent's run of a saved script
//! on its own machine, in its checkout. The backend keeps the run's state,
//! `lastRun`, output log, service restarts and monitors here; only the
//! process runs on the machine, driven over the executor link's script
//! frames (nodeProtocol 8 or later). A run is keyed by its place: the
//! workspace, the script and the remote agent, so each agent has at most
//! one machine run per script, separate from the script's home run.
//!
//! Each process is one `spawnId`: frames naming another spawn (an earlier
//! restart, or a settled run) are ignored. The §5.8a `runId` spans a
//! service's automatic restarts and never crosses the link. Transitions are
//! published as `script:changed` only (through [`Hooks::changed`]).
//!
//! Monitors are the workspace's script monitors too: their rows (without
//! the machine fields) are listed with the home runs' and their
//! transitions published as `scriptMonitor:*` (through [`Hooks::monitor`]),
//! so a remote agent's watch shows like a local one.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::machines::{MachineUpdateRequired, UpdateSide};
use intent_core::node_link::{
    LinkFrame, ScriptEnvVar, ScriptExited, ScriptProcessRef, ScriptStartCommand, SCRIPT_PROTOCOL,
};
use intent_core::{
    now_iso, AgentId, BoxFuture, Error, Result, Script, ScriptLastRun, ScriptMode,
    ScriptRunOutcome, ScriptRuntimeState, ScriptStatus, WorkspaceId,
};
use intent_services::feature_api::feature_ports::MachineScriptLink;
use serde_json::{json, Value};
use tokio::sync::watch;

use super::monitors::Options;

/// The output a machine run keeps for `script.output`, `script.run` and
/// monitors: the trailing bytes.
const OUTPUT_KEEP_BYTES: usize = 512 * 1024;
/// A service restarted this many times in a row without staying up for
/// [`STABLE_AFTER`] stops restarting.
const MAX_FAST_RESTARTS: u32 = 5;
const STABLE_AFTER: Duration = Duration::from_secs(10);

/// Resolves an agent's machine link, connecting on demand.
pub(crate) type LinkFor =
    Arc<dyn Fn(AgentId) -> BoxFuture<'static, Result<MachineScriptLink>> + Send + Sync>;
/// Publishes `script:changed` for a script.
pub(crate) type Changed = Arc<dyn Fn(WorkspaceId, String) -> BoxFuture<'static, ()> + Send + Sync>;
/// Wakes an agent with a monitor message.
pub(crate) type Wake =
    Arc<dyn Fn(WorkspaceId, AgentId, String) -> BoxFuture<'static, ()> + Send + Sync>;
/// Publishes `scriptMonitor:<kind>` with a monitor row.
pub(crate) type MonitorChanged =
    Arc<dyn Fn(WorkspaceId, String, Value) -> BoxFuture<'static, ()> + Send + Sync>;

/// How machine runs reach the rest of the daemon.
pub(crate) struct Hooks {
    pub link: LinkFor,
    pub changed: Changed,
    pub wake: Wake,
    pub monitor: MonitorChanged,
    /// Wait before restarting a service that exited.
    pub restart_backoff: Duration,
    /// How long `stop` waits for the machine's acknowledgement.
    pub stop_wait: Duration,
}

/// Where a machine run is: its script and the remote agent running it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[expect(
    clippy::struct_field_names,
    reason = "the wire names: workspaceId, scriptId, agentId"
)]
pub(crate) struct RunPlace {
    pub workspace_id: WorkspaceId,
    pub script_id: String,
    pub agent_id: AgentId,
}

/// How a process's run ended, for the monitor's final-line check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// The machine reported the process's exit (`scriptExited`): its output
    /// reached EOF, so an unterminated final line is a real line.
    ProcessExit,
    /// The run ended without the process's own EOF (lost link, unsent
    /// frame, unacknowledged stop, lease end): a cut-off line is a gap.
    StreamGap,
}

struct Monitor {
    id: String,
    run_id: Option<String>,
    pattern: Option<regex::Regex>,
    /// Splits the run's output into lines as the home run's monitors do.
    decoder: intent_core::script_output::LineDecoder,
    /// Lines seen, for a trigger's `observedLineCount`.
    lines: u32,
    expires: tokio::time::Instant,
    created_at: String,
    expires_at: String,
}

impl Run {
    /// The machine as wake messages name it: its name, else its node id.
    fn machine(&self) -> &str {
        if self.machine_name.trim().is_empty() {
            &self.node_id
        } else {
            &self.machine_name
        }
    }
}

impl Monitor {
    fn expired(&self) -> bool {
        self.expires <= tokio::time::Instant::now()
    }
}

/// A new monitor: the agent's reply, the terminal row of the monitor it
/// replaced, and its own id, workspace row and expiry.
struct Registered {
    reply: Value,
    replaced: Option<Value>,
    id: String,
    row: Value,
    expires: tokio::time::Instant,
}

/// What ending a monitor leaves to do once the lock is released: its
/// terminal row to publish and the wake for its owner.
#[derive(Default)]
#[must_use]
struct Ended {
    event: Option<Value>,
    wake: Option<String>,
}

#[expect(
    clippy::struct_field_names,
    reason = "`last_run` is the wire `lastRun`"
)]
struct Run {
    node_id: String,
    /// The machine's name, for wake messages.
    machine_name: String,
    state: ScriptRuntimeState,
    last_run: Option<ScriptLastRun>,
    def: Script,
    /// The live process, if any.
    spawn_id: Option<String>,
    link: Option<MachineScriptLink>,
    stopping: bool,
    output: String,
    /// Unfinished last line, for output monitors.
    partial: String,
    spawned_at: Option<tokio::time::Instant>,
    fast_restarts: u32,
    monitor: Option<Monitor>,
}

struct Inner {
    hooks: Hooks,
    runs: Mutex<HashMap<RunPlace, Run>>,
    /// Bumped whenever a run settles (exit, loss, stop).
    settled: watch::Sender<u64>,
    /// Agents whose final stop is under way: no new process starts for
    /// them until the machine acknowledges it.
    ending: Mutex<HashSet<AgentId>>,
}

/// Every machine run of this daemon. Cloning shares them.
#[derive(Clone)]
pub(crate) struct MachineRuns {
    inner: Arc<Inner>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn start_frame(place: &RunPlace, def: &Script, spawn_id: &str) -> LinkFrame {
    LinkFrame::ScriptStart(ScriptStartCommand {
        agent_id: place.agent_id.clone(),
        script_id: place.script_id.clone(),
        spawn_id: spawn_id.to_owned(),
        command: def.command.clone(),
        mode: def.mode,
        cwd: def.cwd.clone().filter(|cwd| !cwd.is_empty()),
        env: def
            .env
            .iter()
            .flatten()
            .map(|(key, value)| ScriptEnvVar {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
    })
}

fn stop_frame(place: &RunPlace, spawn_id: &str) -> LinkFrame {
    LinkFrame::ScriptStop(ScriptProcessRef {
        agent_id: place.agent_id.clone(),
        script_id: place.script_id.clone(),
        spawn_id: spawn_id.to_owned(),
    })
}

fn is_live(state: &ScriptRuntimeState) -> bool {
    matches!(
        state.status,
        ScriptStatus::Starting | ScriptStatus::Running | ScriptStatus::Restarting
    )
}

fn tail(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut start = text.len() - max;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

impl MachineRuns {
    pub(crate) fn new(hooks: Hooks) -> Self {
        Self {
            inner: Arc::new(Inner {
                hooks,
                runs: Mutex::default(),
                settled: watch::channel(0).0,
                ending: Mutex::default(),
            }),
        }
    }

    fn refuse_if_ending(&self, place: &RunPlace) -> Result<()> {
        if lock(&self.inner.ending).contains(&place.agent_id) {
            return Err(agent_ending(place));
        }
        Ok(())
    }

    async fn changed(&self, place: &RunPlace) {
        (self.inner.hooks.changed)(place.workspace_id.clone(), place.script_id.clone()).await;
    }

    /// Publish `ended`'s terminal row, then wake its owner.
    async fn deliver(&self, place: &RunPlace, ended: Ended) {
        if let Some(row) = ended.event {
            self.publish_monitor(place, row).await;
        }
        if let Some(content) = ended.wake {
            (self.inner.hooks.wake)(place.workspace_id.clone(), place.agent_id.clone(), content)
                .await;
        }
    }

    async fn publish_monitor(&self, place: &RunPlace, row: Value) {
        let kind = match row["state"].as_str() {
            Some("active") | None => "registered".to_owned(),
            Some(state) => state.to_owned(),
        };
        (self.inner.hooks.monitor)(place.workspace_id.clone(), kind, row).await;
    }

    fn bump(&self) {
        self.inner.settled.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// A usable link for a run on `place`'s machine.
    async fn link(&self, place: &RunPlace) -> Result<MachineScriptLink> {
        let link = (self.inner.hooks.link)(place.agent_id.clone()).await?;
        if link.protocol < SCRIPT_PROTOCOL {
            return Err(Error::MachineUpdateRequired(Box::new(
                MachineUpdateRequired {
                    machine_name: link.machine_name.clone(),
                    machine_version: link.machine_version.clone(),
                    backend_version: env!("CARGO_PKG_VERSION").to_owned(),
                    update: UpdateSide::Machine,
                    hint: Some(
                        "Remote agents' saved scripts need the machine on a newer Intent."
                            .to_owned(),
                    ),
                    reason: None,
                },
            )));
        }
        Ok(link)
    }

    /// Start `place`'s machine run of `def` (its first start creates the
    /// entry). A live run is left as is. Returns the run's state.
    ///
    /// # Errors
    ///
    /// `machine-unreachable` when no link comes up; `machine-update-required`
    /// when the machine links below protocol 8.
    pub(crate) async fn start(&self, place: &RunPlace, def: &Script) -> Result<ScriptRuntimeState> {
        self.refuse_if_ending(place)?;
        if let Some(state) = self.state(place).filter(is_live) {
            return Ok(state);
        }
        let link = self.link(place).await?;
        let spawn_id = new_id();
        let state = {
            let mut runs = lock(&self.inner.runs);
            // A final stop may have begun while the link was looked up.
            if lock(&self.inner.ending).contains(&place.agent_id) {
                return Err(agent_ending(place));
            }
            // Another start may have won while the link was looked up: it
            // owns the process, so report its run instead of a second one.
            if let Some(live) = runs.get(place).map(|run| &run.state).filter(|s| is_live(s)) {
                return Ok(live.clone());
            }
            let run = runs.entry(place.clone()).or_insert_with(|| Run {
                node_id: link.node_id.clone(),
                machine_name: link.machine_name.clone(),
                state: ScriptRuntimeState::default(),
                last_run: None,
                def: def.clone(),
                spawn_id: None,
                link: None,
                stopping: false,
                output: String::new(),
                partial: String::new(),
                spawned_at: None,
                fast_restarts: 0,
                monitor: None,
            });
            run.node_id.clone_from(&link.node_id);
            run.machine_name.clone_from(&link.machine_name);
            run.def = def.clone();
            run.link = Some(link.clone());
            run.spawn_id = Some(spawn_id.clone());
            run.stopping = false;
            run.output.clear();
            run.partial.clear();
            run.spawned_at = Some(tokio::time::Instant::now());
            run.fast_restarts = 0;
            run.state = ScriptRuntimeState {
                run_id: Some(new_id()),
                status: ScriptStatus::Starting,
                started_at: Some(now_iso()),
                ..ScriptRuntimeState::default()
            };
            run.state.clone()
        };
        if let Err(error) = (link.send)(start_frame(place, def, &spawn_id)).await {
            self.settle(
                place,
                &spawn_id,
                None,
                Some(format!("remote-script-lost: {error}")),
                Ending::StreamGap,
            )
            .await;
            return self.state(place).ok_or(error);
        }
        self.changed(place).await;
        Ok(state)
    }

    /// Stop `place`'s run and wait (bounded) for the machine's
    /// acknowledgement; a run that is not live is dismissed (its entry
    /// removed). Returns the state after the stop.
    ///
    /// # Errors
    ///
    /// `InvalidParams` when `place` has no machine run.
    pub(crate) async fn stop(&self, place: &RunPlace) -> Result<ScriptRuntimeState> {
        self.stop_because(place, "the run was stopped").await
    }

    /// [`Self::stop`], settling the run's monitor as cancelled with `reason`.
    async fn stop_because(&self, place: &RunPlace, reason: &str) -> Result<ScriptRuntimeState> {
        let (live, link, spawn_id, cancelled) = {
            let mut runs = lock(&self.inner.runs);
            let run = runs.get_mut(place).ok_or_else(|| no_run(place))?;
            if is_live(&run.state) {
                run.stopping = true;
                // The run being stopped takes its monitor with it: a watch
                // never outlives its run into the next one. As the home
                // run's, the monitor settles cancelled and wakes its owner.
                let cancelled = cancelled_wake(place, run, reason);
                (true, run.link.clone(), run.spawn_id.clone(), cancelled)
            } else {
                runs.remove(place);
                (false, None, None, Ended::default())
            }
        };
        if !live {
            self.changed(place).await;
            return Ok(ScriptRuntimeState::default());
        }
        if let (Some(link), Some(spawn_id)) = (link, spawn_id) {
            let mut settled = self.inner.settled.subscribe();
            let _ = (link.send)(stop_frame(place, &spawn_id)).await;
            let _ = tokio::time::timeout(self.inner.hooks.stop_wait, async {
                while self.spawn_of(place).as_deref() == Some(spawn_id.as_str()) {
                    if settled.changed().await.is_err() {
                        return;
                    }
                }
            })
            .await;
            // Unacknowledged within the wait: the machine stops it once
            // the link drops; settle it here so the run is not left live.
            if self.spawn_of(place).as_deref() == Some(spawn_id.as_str()) {
                self.settle(place, &spawn_id, None, None, Ending::StreamGap)
                    .await;
            }
        } else {
            // Restarting between processes: nothing runs on the machine.
            let mut runs = lock(&self.inner.runs);
            if let Some(run) = runs.get_mut(place) {
                run.state = idle_after(&run.state);
                run.stopping = false;
            }
        }
        self.changed(place).await;
        self.deliver(place, cancelled).await;
        Ok(self.state(place).unwrap_or_default())
    }

    /// Stop then start `place`'s run again on its machine.
    ///
    /// # Errors
    ///
    /// As [`Self::start`]; `InvalidParams` when `place` has no machine run.
    pub(crate) async fn restart(
        &self,
        place: &RunPlace,
        def: &Script,
    ) -> Result<ScriptRuntimeState> {
        // Check the final stop and the link before stopping, so a refused
        // restart stops nothing.
        self.refuse_if_ending(place)?;
        if !lock(&self.inner.runs).contains_key(place) {
            return Err(no_run(place));
        }
        self.link(place).await?;
        if self.state(place).as_ref().is_some_and(is_live) {
            self.stop_because(place, "the run was restarted").await?;
        }
        self.start(place, def).await
    }

    /// Start a command run and wait for it to end (or `timeout`).
    ///
    /// # Errors
    ///
    /// As [`Self::start`].
    pub(crate) async fn run(
        &self,
        place: &RunPlace,
        def: &Script,
        timeout: Duration,
        max_lines: usize,
    ) -> Result<Value> {
        let mut settled = self.inner.settled.subscribe();
        let state = self.start(place, def).await?;
        let run_id = state.run_id.clone();
        let done = tokio::time::timeout(timeout, async {
            loop {
                let current = self.state(place);
                if current
                    .as_ref()
                    .is_none_or(|s| !is_live(s) || s.run_id != run_id)
                {
                    return;
                }
                if settled.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .is_ok();
        let state = self.state(place).unwrap_or_default();
        let output = self.output_lines(place, max_lines).unwrap_or_default();
        // As the home run's `script.run`: the newest bytes that fit the
        // reply budget once JSON-escaped, `outputTruncated` on a cut.
        let (output, cut) = super::output_tail_within_budget(&output);
        let mut result = json!({
            "exitCode": if done { state.exit_code } else { None },
            "output": output,
        });
        if cut {
            result["outputTruncated"] = json!(true);
        }
        if !done {
            result["timedOut"] = json!(true);
        }
        if let Some(error) = state.error.filter(|_| done) {
            result["error"] = json!(error);
        }
        Ok(result)
    }

    fn spawn_of(&self, place: &RunPlace) -> Option<String> {
        lock(&self.inner.runs)
            .get(place)
            .and_then(|run| run.spawn_id.clone())
    }

    /// `place`'s runtime state, if it has a machine run.
    pub(crate) fn state(&self, place: &RunPlace) -> Option<ScriptRuntimeState> {
        lock(&self.inner.runs)
            .get(place)
            .map(|run| run.state.clone())
    }

    /// `place`'s kept output.
    pub(crate) fn output(&self, place: &RunPlace) -> Option<String> {
        lock(&self.inner.runs)
            .get(place)
            .map(|run| format!("{}{}", run.output, run.partial))
    }

    /// `script.output` of `place`'s run, as the home run's: the newest
    /// `line_count` lines under a `[… lines]` header, cut to the reply
    /// budget ("Bounded output reads"); `None` without a machine run.
    pub(crate) fn output_reply(&self, place: &RunPlace, line_count: usize) -> Option<Value> {
        let text = self.output(place)?;
        if text.trim().is_empty() {
            return Some(Value::String("No output yet.".to_owned()));
        }
        let text = text.strip_suffix('\n').unwrap_or(&text);
        let lines: Vec<&str> = text.split('\n').collect();
        let total = lines.len();
        let tail = lines[total.saturating_sub(line_count)..].join("\n");
        Some(Value::String(super::format_output_tail(
            &tail, total, line_count,
        )))
    }

    /// The last `max_lines` lines of `place`'s output.
    pub(crate) fn output_lines(&self, place: &RunPlace, max_lines: usize) -> Option<String> {
        let text = self.output(place)?;
        let lines: Vec<&str> = text.lines().collect();
        let start = lines.len().saturating_sub(max_lines);
        Some(lines[start..].join("\n"))
    }

    /// The `machineRuns` entries of a script, oldest agent first.
    pub(crate) fn entries(&self, workspace_id: &WorkspaceId, script_id: &str) -> Vec<Value> {
        let runs = lock(&self.inner.runs);
        let mut entries: Vec<(&RunPlace, &Run)> = runs
            .iter()
            .filter(|(place, _)| {
                &place.workspace_id == workspace_id && place.script_id == script_id
            })
            .collect();
        entries.sort_by(|a, b| a.0.agent_id.as_str().cmp(b.0.agent_id.as_str()));
        entries
            .into_iter()
            .map(|(place, run)| {
                let mut entry = json!({
                    "remoteAgentId": place.agent_id.as_str(),
                    "nodeId": run.node_id,
                    "runtime": run.state,
                });
                if let Some(last) = &run.last_run {
                    let mut last = serde_json::to_value(last).unwrap_or(Value::Null);
                    if let Some(map) = last.as_object_mut() {
                        map.remove("runId");
                    }
                    entry["lastRun"] = last;
                }
                entry
            })
            .collect()
    }

    /// Whether a machine run is live on `node_id`.
    pub(crate) fn live_on(&self, node_id: &str) -> bool {
        lock(&self.inner.runs)
            .values()
            .any(|run| run.node_id == node_id && is_live(&run.state))
    }

    /// Handle a script frame from machine `node_id`.
    pub(crate) async fn on_frame(&self, node_id: &str, frame: LinkFrame) {
        let (agent, script, spawn) = match &frame {
            LinkFrame::ScriptStarted(f) => (&f.agent_id, &f.script_id, &f.spawn_id),
            LinkFrame::ScriptOutput(f) => (&f.agent_id, &f.script_id, &f.spawn_id),
            LinkFrame::ScriptExited(f) => (&f.agent_id, &f.script_id, &f.spawn_id),
            _ => return,
        };
        let place = lock(&self.inner.runs)
            .iter()
            .find(|(place, run)| {
                &place.agent_id == agent
                    && &place.script_id == script
                    && run.node_id == node_id
                    && run.spawn_id.as_deref() == Some(spawn.as_str())
            })
            .map(|(place, _)| place.clone());
        let Some(place) = place else {
            tracing::debug!("machine run: ignoring a frame for a process that is not current");
            return;
        };
        match frame {
            LinkFrame::ScriptStarted(started) => {
                {
                    let mut runs = lock(&self.inner.runs);
                    if let Some(run) = runs.get_mut(&place) {
                        run.state.status = ScriptStatus::Running;
                        run.state.pid = Some(started.pid);
                    }
                }
                self.changed(&place).await;
            }
            LinkFrame::ScriptOutput(output) => self.append(&place, &output.data).await,
            LinkFrame::ScriptExited(ScriptExited {
                spawn_id,
                exit_code,
                error,
                ..
            }) => {
                self.settle(
                    &place,
                    &spawn_id,
                    exit_code.map(i64::from),
                    error,
                    Ending::ProcessExit,
                )
                .await;
            }
            _ => {}
        }
    }

    async fn append(&self, place: &RunPlace, data: &str) {
        let wake = {
            let mut runs = lock(&self.inner.runs);
            let Some(run) = runs.get_mut(place) else {
                return;
            };
            // Monitors see lines as the home run's do: the shared decoder
            // keeps one line up to its bound, and an oversized line counts
            // but cannot match, so a wake never carries a huge line.
            let mut matched = None;
            if let Some(monitor) = run
                .monitor
                .as_mut()
                .filter(|m| m.run_id.is_none() || m.run_id == run.state.run_id)
            {
                for byte in data.bytes() {
                    if let Some(line) = monitor.decoder.push(byte) {
                        monitor.lines = monitor.lines.saturating_add(1);
                        if let Some(text) = line.text.filter(|text| {
                            monitor.pattern.as_ref().is_some_and(|p| p.is_match(text))
                        }) {
                            matched = Some(text);
                            break;
                        }
                    }
                }
            }
            run.partial.push_str(data);
            if let Some(end) = run.partial.rfind('\n') {
                let complete: String = run.partial.drain(..=end).collect();
                run.output.push_str(&complete);
            }
            // An unfinished line that would take the run past the keep
            // bound (output without newlines) joins the kept output, which is
            // trimmed to the bound; `output()` reads both as one text.
            if run.output.len() + run.partial.len() > OUTPUT_KEEP_BYTES {
                let unfinished = std::mem::take(&mut run.partial);
                run.output.push_str(&unfinished);
            }
            if run.output.len() > OUTPUT_KEEP_BYTES {
                run.output = tail(&run.output, OUTPUT_KEEP_BYTES).to_owned();
            }
            matched
                .and_then(|line| {
                    let monitor = run.monitor.take()?;
                    Some(triggered(place, run, &monitor, &line))
                })
                .unwrap_or_default()
        };
        self.deliver(place, wake).await;
    }

    /// Settle `spawn_id`'s process of `place`: a stop or a command ends the
    /// run; a service that exited on its own restarts.
    async fn settle(
        &self,
        place: &RunPlace,
        spawn_id: &str,
        exit_code: Option<i64>,
        error: Option<String>,
        ending: Ending,
    ) {
        let (restart, wake) = {
            let mut runs = lock(&self.inner.runs);
            let Some(run) = runs.get_mut(place) else {
                return;
            };
            if run.spawn_id.as_deref() != Some(spawn_id) {
                return;
            }
            run.spawn_id = None;
            if !run.partial.is_empty() {
                let rest = std::mem::take(&mut run.partial);
                run.output.push_str(&rest);
            }
            let stable = run
                .spawned_at
                .is_some_and(|at| at.elapsed() >= STABLE_AFTER);
            if stable {
                run.fast_restarts = 0;
            }
            let restart = !run.stopping
                && error.is_none()
                && run.def.mode == ScriptMode::Service
                && run.fast_restarts < MAX_FAST_RESTARTS;
            if run.stopping {
                run.stopping = false;
                let cancelled = cancelled_wake(place, run, "the run was stopped");
                if run.def.mode == ScriptMode::Command {
                    run.last_run = Some(last_run(
                        &run.state,
                        ScriptRunOutcome::Cancelled,
                        exit_code,
                        None,
                    ));
                }
                run.state = idle_after(&run.state);
                (false, cancelled)
            } else if restart {
                run.fast_restarts += 1;
                run.state.status = ScriptStatus::Restarting;
                run.state.restart_count += 1;
                run.state.pid = None;
                run.state.exit_code = exit_code;
                (true, Ended::default())
            } else {
                let code = exit_code.unwrap_or(super::EXIT_CODE_UNOBSERVABLE);
                run.state.status = ScriptStatus::Exited;
                run.state.exit_code = Some(code);
                run.state.error.clone_from(&error);
                run.state.stopped_at = Some(now_iso());
                run.state.pid = None;
                let outcome = if error
                    .as_deref()
                    .is_some_and(|e| e.starts_with("remote-script-lost"))
                {
                    ScriptRunOutcome::Interrupted
                } else if code == 0 {
                    ScriptRunOutcome::Succeeded
                } else {
                    ScriptRunOutcome::Failed
                };
                let result = last_run(&run.state, outcome, Some(code), error.clone());
                if run.def.mode == ScriptMode::Command {
                    run.last_run = Some(result.clone());
                }
                let ended = run.monitor.take().map(|mut monitor| {
                    if monitor.expired() {
                        return expired(place, run, &monitor);
                    }
                    // As the home run's monitor: the final line, unterminated
                    // at exit, is checked before the exit wake.
                    // Only the process's own EOF flushes the final line; a
                    // line cut off by a lost link is a stream gap.
                    let last = (ending == Ending::ProcessExit)
                        .then(|| monitor.decoder.eof())
                        .flatten()
                        .and_then(|line| {
                            monitor.lines = monitor.lines.saturating_add(1);
                            line.text
                        })
                        .filter(|text| monitor.pattern.as_ref().is_some_and(|p| p.is_match(text)));
                    if let Some(line) = last {
                        return triggered(place, run, &monitor, &line);
                    }
                    let wake = match &error {
                        Some(error) => format!(
                            "Script \"{}\" (machine run on {}) exited with code {code}: {error}",
                            run.def.name,
                            run.machine()
                        ),
                        None => format!(
                            "Script \"{}\" (machine run on {}) exited with code {code}",
                            run.def.name,
                            run.machine()
                        ),
                    };
                    Ended {
                        event: Some(completed(place, run, &monitor, result)),
                        wake: Some(wake),
                    }
                });
                (false, ended.unwrap_or_default())
            }
        };
        self.bump();
        self.changed(place).await;
        self.deliver(place, wake).await;
        if restart {
            let this = self.clone();
            let place = place.clone();
            let backoff = self.inner.hooks.restart_backoff;
            // caller-binding: allow — a machine run's service restart, as the home supervisor's backoff
            tokio::spawn(async move {
                tokio::time::sleep(backoff).await;
                this.respawn(place).await;
            });
        }
    }

    /// Start a new process for a restarting service run. Boxed: it settles
    /// a failed start, which may schedule the next restart.
    fn respawn(&self, place: RunPlace) -> BoxFuture<'static, ()> {
        let this = self.clone();
        Box::pin(async move { this.respawn_now(&place).await })
    }

    async fn respawn_now(&self, place: &RunPlace) {
        let respawn = {
            let mut runs = lock(&self.inner.runs);
            let Some(run) = runs.get_mut(place) else {
                return;
            };
            if run.state.status != ScriptStatus::Restarting || run.spawn_id.is_some() {
                return;
            }
            if run.stopping {
                // A stop (or a final stop) came during the backoff: the
                // service stays stopped instead of starting a new process.
                run.stopping = false;
                let cancelled = cancelled_wake(place, run, "the run was stopped");
                run.state = idle_after(&run.state);
                Err(cancelled)
            } else {
                let Some(link) = run.link.clone() else { return };
                let spawn_id = new_id();
                run.spawn_id = Some(spawn_id.clone());
                run.spawned_at = Some(tokio::time::Instant::now());
                run.state.status = ScriptStatus::Starting;
                Ok((link, run.def.clone(), spawn_id))
            }
        };
        let (link, def, spawn_id) = match respawn {
            Ok(respawn) => respawn,
            Err(cancelled) => {
                self.bump();
                self.changed(place).await;
                self.deliver(place, cancelled).await;
                return;
            }
        };
        if (link.send)(start_frame(place, &def, &spawn_id))
            .await
            .is_err()
        {
            self.settle(
                place,
                &spawn_id,
                None,
                Some("remote-script-lost: the machine disconnected".into()),
                Ending::StreamGap,
            )
            .await;
            return;
        }
        self.changed(place).await;
    }

    /// Machine `node_id`'s link ended: every live run on it is lost.
    pub(crate) async fn link_lost(&self, node_id: &str, reason: &str) {
        let lost: Vec<(RunPlace, Option<String>)> = lock(&self.inner.runs)
            .iter_mut()
            .filter(|(_, run)| run.node_id == node_id && is_live(&run.state))
            .map(|(place, run)| {
                run.stopping = false;
                // A service between processes is lost too.
                let spawn = run.spawn_id.get_or_insert_with(new_id).clone();
                (place.clone(), Some(spawn))
            })
            .collect();
        for (place, spawn) in lost {
            if let Some(spawn) = spawn {
                self.settle(
                    &place,
                    &spawn,
                    None,
                    Some(reason.to_owned()),
                    Ending::StreamGap,
                )
                .await;
            }
        }
    }

    /// Lease end for one agent: [`Self::end_agents`]; whether every stop
    /// was acknowledged.
    pub(crate) async fn end_agent(
        &self,
        agent_id: &AgentId,
        deadline: tokio::time::Instant,
        reason: &str,
    ) -> bool {
        self.end_agents(std::slice::from_ref(agent_id), deadline, reason)
            .await
            .is_empty()
    }

    /// Lease end: send a stop for every live run of `agents`, wait once for
    /// the acknowledgements until `deadline`, settle what is left with
    /// `reason`, then drop the agents' entries. Returns the agents with an
    /// unacknowledged stop.
    pub(crate) async fn end_agents(
        &self,
        agents: &[AgentId],
        deadline: tokio::time::Instant,
        reason: &str,
    ) -> Vec<AgentId> {
        let live: Vec<(RunPlace, MachineScriptLink, String)> = lock(&self.inner.runs)
            .iter_mut()
            .filter(|(place, run)| agents.contains(&place.agent_id) && is_live(&run.state))
            .filter_map(|(place, run)| {
                run.stopping = true;
                Some((place.clone(), run.link.clone()?, run.spawn_id.clone()?))
            })
            .collect();
        let mut settled = self.inner.settled.subscribe();
        for (place, link, spawn) in &live {
            let _ = (link.send)(stop_frame(place, spawn)).await;
        }
        let pending = |runs: &MachineRuns| -> Vec<&(RunPlace, MachineScriptLink, String)> {
            live.iter()
                .filter(|(place, _, spawn)| runs.spawn_of(place).as_deref() == Some(spawn.as_str()))
                .collect()
        };
        let _ = tokio::time::timeout_at(deadline, async {
            while !pending(self).is_empty() {
                if settled.changed().await.is_err() {
                    return;
                }
            }
        })
        .await;
        let mut unacked: Vec<AgentId> = Vec::new();
        for (place, _, _) in pending(self) {
            if !unacked.contains(&place.agent_id) {
                unacked.push(place.agent_id.clone());
            }
        }
        // A run whose stop was never acknowledged is lost with `reason`, not
        // stopped: nobody saw it end.
        for (place, _, spawn) in &live {
            self.lose(place, spawn, reason).await;
        }
        let dropped: Vec<RunPlace> = {
            let mut runs = lock(&self.inner.runs);
            let places: Vec<RunPlace> = runs
                .keys()
                .filter(|place| agents.contains(&place.agent_id))
                .cloned()
                .collect();
            for place in &places {
                runs.remove(place);
            }
            places
        };
        // `ending` stays: an agent in it has a queued final stop, which a
        // lease end (even a failed removal) does not end; only its ack or
        // expiry does (`final_stop_acked`).
        for place in dropped {
            self.changed(&place).await;
        }
        unacked
    }

    /// Settle `spawn_id`'s process of `place` as lost with `reason` (a
    /// stream gap: no exit was seen), even if a stop had been requested.
    async fn lose(&self, place: &RunPlace, spawn_id: &str, reason: &str) {
        if let Some(run) = lock(&self.inner.runs).get_mut(place) {
            run.stopping = false;
        }
        self.settle(
            place,
            spawn_id,
            None,
            Some(reason.to_owned()),
            Ending::StreamGap,
        )
        .await;
    }

    /// A final stop for `agent_id` is under way: the machine stops the
    /// agent's scripts itself, so an exit it reports for one of them now
    /// ends that run (no service restart).
    pub(crate) fn final_stop_begun(&self, agent_id: &AgentId) {
        lock(&self.inner.ending).insert(agent_id.clone());
        for (place, run) in lock(&self.inner.runs).iter_mut() {
            if &place.agent_id == agent_id && is_live(&run.state) {
                run.stopping = true;
            }
        }
    }

    /// The machine acknowledged `agent_id`'s final stop: every machine run of
    /// the agent it never reported ending is lost with `reason`, as the lease
    /// end settles them, and the agent's entries are dropped. No frame is
    /// sent: the machine already stopped them.
    pub(crate) async fn final_stop_acked(&self, agent_id: &AgentId, reason: &str) {
        let live: Vec<(RunPlace, String)> = lock(&self.inner.runs)
            .iter_mut()
            .filter(|(place, run)| &place.agent_id == agent_id && is_live(&run.state))
            .map(|(place, run)| {
                // A service between processes is settled too.
                let spawn = run.spawn_id.get_or_insert_with(new_id).clone();
                (place.clone(), spawn)
            })
            .collect();
        for (place, spawn) in &live {
            self.lose(place, spawn, reason).await;
        }
        let dropped: Vec<RunPlace> = {
            let mut runs = lock(&self.inner.runs);
            let places: Vec<RunPlace> = runs
                .keys()
                .filter(|place| &place.agent_id == agent_id)
                .cloned()
                .collect();
            for place in &places {
                runs.remove(place);
            }
            places
        };
        lock(&self.inner.ending).remove(agent_id);
        for place in dropped {
            self.changed(&place).await;
        }
    }

    /// The definition changed or was removed: stop every live machine run
    /// of the script, settle each as cancelled with `reason`, and drop all
    /// its entries.
    pub(crate) async fn cancel_script(
        &self,
        workspace_id: &WorkspaceId,
        script_id: &str,
        reason: &str,
    ) {
        let removed: Vec<(RunPlace, Run)> = {
            let mut runs = lock(&self.inner.runs);
            let places: Vec<RunPlace> = runs
                .keys()
                .filter(|p| &p.workspace_id == workspace_id && p.script_id == script_id)
                .cloned()
                .collect();
            places
                .into_iter()
                .filter_map(|p| runs.remove(&p).map(|run| (p, run)))
                .collect()
        };
        if removed.is_empty() {
            return;
        }
        let mut ended = Vec::new();
        for (place, mut run) in removed {
            if let (Some(link), Some(spawn)) = (&run.link, &run.spawn_id) {
                let _ = (link.send)(stop_frame(&place, spawn)).await;
            }
            ended.push((place.clone(), cancelled_wake(&place, &mut run, reason)));
        }
        self.bump();
        self.changed(&ended[0].0).await;
        for (place, ended) in ended {
            self.deliver(&place, ended).await;
        }
    }

    /// Watch `place`'s run: wake its agent when the run ends, or when an
    /// output line matches `outputPattern`. One active monitor per place;
    /// a new one replaces it. Returns the monitor row.
    ///
    /// # Errors
    ///
    /// `InvalidParams` for bad options, an unknown run, or a `runId` that
    /// is not the run's current one.
    pub(crate) async fn monitor(&self, place: &RunPlace, options: &Value) -> Result<Value> {
        let options = Options::parse(options)?;
        let registered = {
            let mut runs = lock(&self.inner.runs);
            match Self::register(&mut runs, place, options)? {
                Ok(registered) => registered,
                Err(refused) => return Ok(refused),
            }
        };
        if let Some(old) = registered.replaced {
            self.publish_monitor(place, old).await;
        }
        self.publish_monitor(place, registered.row).await;
        // A monitor that runs out leaves the list, as a home-run one does.
        let this = self.clone();
        let place = place.clone();
        let (id, expires) = (registered.id, registered.expires);
        // caller-binding: allow — a machine-run monitor's TTL, as the home monitors' sweep
        tokio::spawn(async move {
            tokio::time::sleep_until(expires).await;
            this.expire(&place, &id).await;
        });
        Ok(registered.reply)
    }

    /// Settle `place`'s monitor `id` as expired if it is still active.
    async fn expire(&self, place: &RunPlace, id: &str) {
        let ended = {
            let mut runs = lock(&self.inner.runs);
            let Some(run) = runs.get_mut(place) else {
                return;
            };
            if run.monitor.as_ref().is_none_or(|m| m.id != id) {
                return;
            }
            let Some(monitor) = run.monitor.take() else {
                return;
            };
            expired(place, run, &monitor)
        };
        self.deliver(place, ended).await;
    }

    /// Register a monitor under the runs lock; the inner `Err` is a
    /// refusal reply.
    fn register(
        runs: &mut HashMap<RunPlace, Run>,
        place: &RunPlace,
        options: Options,
    ) -> Result<std::result::Result<Registered, Value>> {
        let run = runs.get_mut(place).ok_or_else(|| no_run(place))?;
        // As for the home run, a stale `runId` is rejected first.
        if let Some(run_id) = &options.run {
            if run.state.run_id.as_ref() != Some(run_id) {
                return Err(Error::InvalidParams(format!(
                    "runId {run_id} is not the current run of this machine run"
                )));
            }
        }
        // As for the home run: a run that already finished cannot be
        // watched, its only exit having passed.
        if !is_live(&run.state) {
            return Ok(Err(json!({
                "ok": false,
                "refused": true,
                "reason": "run-finished",
                "scriptId": place.script_id,
                "runId": run.state.run_id,
            })));
        }
        let ttl = Duration::from_millis(u64::try_from(options.ttl).unwrap_or(0));
        let created = chrono::Utc::now();
        let expires = created + chrono::Duration::from_std(ttl).unwrap_or_default();
        // A line already under way when the monitor starts is not joined:
        // matching begins at the next line, as for a home-run monitor.
        let mut decoder = intent_core::script_output::LineDecoder::new(true);
        if !run.partial.is_empty() {
            decoder.gap();
        }
        let monitor = Monitor {
            id: new_id(),
            run_id: run.state.run_id.clone(),
            pattern: options.pattern,
            decoder,
            lines: 0,
            expires: tokio::time::Instant::now() + ttl,
            created_at: created.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            expires_at: expires.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        };
        let row = monitor_row(place, run, &monitor);
        let reply = json!({ "ok": true, "monitor": row.clone() });
        let (id, expires) = (monitor.id.clone(), monitor.expires);
        let replaced = run
            .monitor
            .replace(monitor)
            .map(|old| ended(place, run, &old, "cancelled", "unmonitored"));
        Ok(Ok(Registered {
            reply,
            replaced,
            id,
            row,
            expires,
        }))
    }

    /// `agent_id`'s active machine-run monitors.
    pub(crate) fn monitors_of(&self, agent_id: &AgentId) -> Vec<Value> {
        let now = tokio::time::Instant::now();
        lock(&self.inner.runs)
            .iter()
            .filter(|(place, _)| &place.agent_id == agent_id)
            .filter_map(|(place, run)| {
                let monitor = run.monitor.as_ref().filter(|m| m.expires > now)?;
                Some(monitor_row(place, run, monitor))
            })
            .collect()
    }

    /// Cancel `agent_id`'s machine-run monitor `monitor_id`; whether it
    /// existed.
    pub(crate) async fn unmonitor(&self, agent_id: &AgentId, monitor_id: &str) -> bool {
        let Some(place) = self.place_of_monitor(None, monitor_id, Some(agent_id)) else {
            return false;
        };
        self.unmonitor_at(&place, monitor_id).await
    }

    async fn unmonitor_at(&self, place: &RunPlace, monitor_id: &str) -> bool {
        let row = {
            let mut runs = lock(&self.inner.runs);
            let Some(run) = runs.get_mut(place) else {
                return false;
            };
            if run.monitor.as_ref().is_none_or(|m| m.id != monitor_id) {
                return false;
            }
            let Some(monitor) = run.monitor.take() else {
                return false;
            };
            ended(place, run, &monitor, "cancelled", "unmonitored")
        };
        self.publish_monitor(place, row).await;
        true
    }

    /// The run watched by monitor `monitor_id` (in `workspace_id`, owned
    /// by `owner`, when given).
    fn place_of_monitor(
        &self,
        workspace_id: Option<&WorkspaceId>,
        monitor_id: &str,
        owner: Option<&AgentId>,
    ) -> Option<RunPlace> {
        lock(&self.inner.runs)
            .iter()
            .find(|(place, run)| {
                workspace_id.is_none_or(|ws| &place.workspace_id == ws)
                    && owner.is_none_or(|agent| &place.agent_id == agent)
                    && run
                        .monitor
                        .as_ref()
                        .is_some_and(|m| m.id == monitor_id && !m.expired())
            })
            .map(|(place, _)| place.clone())
    }

    /// The active machine-run monitors of `workspace_id` (only `owner`'s
    /// when given), as workspace monitor rows, oldest first.
    pub(crate) fn monitors_in(
        &self,
        workspace_id: &WorkspaceId,
        owner: Option<&AgentId>,
    ) -> Vec<Value> {
        let runs = lock(&self.inner.runs);
        let mut rows: Vec<(&str, &str, Value)> = runs
            .iter()
            .filter(|(place, _)| {
                &place.workspace_id == workspace_id
                    && owner.is_none_or(|agent| &place.agent_id == agent)
            })
            .filter_map(|(place, run)| {
                let monitor = run.monitor.as_ref().filter(|m| !m.expired())?;
                Some((
                    monitor.created_at.as_str(),
                    monitor.id.as_str(),
                    monitor_row(place, run, monitor),
                ))
            })
            .collect();
        rows.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        rows.into_iter().map(|(_, _, row)| row).collect()
    }

    /// Active machine-run monitor `monitor_id` of `workspace_id`.
    pub(crate) fn monitor_in(&self, workspace_id: &WorkspaceId, monitor_id: &str) -> Option<Value> {
        self.monitors_in(workspace_id, None)
            .into_iter()
            .find(|row| row["monitorId"] == monitor_id)
    }

    /// Every active machine-run monitor (only `workspace_id`'s when given),
    /// as `(workspace, owner, waiting row)`: the `waitingOnScriptMonitors`
    /// projection of the home monitors.
    pub(crate) fn waiting(
        &self,
        workspace_id: Option<&WorkspaceId>,
    ) -> Vec<(WorkspaceId, AgentId, Value)> {
        let runs = lock(&self.inner.runs);
        let mut rows: Vec<(String, WorkspaceId, AgentId, Value)> = runs
            .iter()
            .filter(|(place, _)| workspace_id.is_none_or(|ws| &place.workspace_id == ws))
            .filter_map(|(place, run)| {
                let monitor = run.monitor.as_ref().filter(|m| !m.expired())?;
                Some((
                    monitor.created_at.clone(),
                    place.workspace_id.clone(),
                    place.agent_id.clone(),
                    json!({
                        "monitorId": monitor.id,
                        "scriptId": place.script_id,
                        "runId": monitor.run_id,
                        "scriptName": run.def.name,
                        "expiresAt": monitor.expires_at,
                    }),
                ))
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows.into_iter()
            .map(|(_, ws, agent, row)| (ws, agent, row))
            .collect()
    }

    /// `scriptMonitor.cancel` / `cancelRun` of a machine-run monitor:
    /// `None` when `monitor_id` is not one (in `workspace_id`, owned by
    /// `owner` when given). Cancelling leaves the run going; with
    /// `stop_run` the run is stopped, which settles the monitor.
    ///
    /// # Errors
    ///
    /// As [`Self::stop`].
    pub(crate) async fn cancel_monitor(
        &self,
        workspace_id: &WorkspaceId,
        monitor_id: &str,
        owner: Option<&AgentId>,
        stop_run: bool,
    ) -> Result<Option<Value>> {
        let Some(place) = self.place_of_monitor(Some(workspace_id), monitor_id, owner) else {
            return Ok(None);
        };
        if !stop_run {
            self.unmonitor_at(&place, monitor_id).await;
            return Ok(Some(json!({ "ok": true, "monitorId": monitor_id })));
        }
        self.stop(&place).await?;
        Ok(Some(
            json!({ "ok": true, "monitorId": monitor_id, "runStopped": true }),
        ))
    }
}

/// The monitor row, as the agent and the workspace (`scriptMonitor.*`,
/// `scriptMonitor:*`) see it: a home-run monitor's plus `remoteAgentId` and
/// `nodeId`.
fn monitor_row(place: &RunPlace, run: &Run, monitor: &Monitor) -> Value {
    let mut row = json!({
        "monitorId": monitor.id,
        "workspaceId": place.workspace_id.as_str(),
        "agentId": place.agent_id.as_str(),
        "scriptId": place.script_id,
        "scriptName": run.def.name,
        "mode": run.def.mode,
        "state": "active",
        "createdAt": monitor.created_at,
        "expiresAt": monitor.expires_at,
        "remoteAgentId": place.agent_id.as_str(),
        "nodeId": run.node_id,
    });
    if let Some(run_id) = &monitor.run_id {
        row["runId"] = json!(run_id);
    }
    if let Some(pattern) = &monitor.pattern {
        row["outputPattern"] = json!(pattern.as_str());
    }
    row
}

/// `monitor`'s terminal row: `state` for `reason`, settled now.
fn ended(place: &RunPlace, run: &Run, monitor: &Monitor, state: &str, reason: &str) -> Value {
    let mut row = monitor_row(place, run, monitor);
    row["state"] = json!(state);
    row["reason"] = json!(reason);
    row["settledAt"] = json!(now_iso());
    row
}

/// `monitor` ran out: it leaves the list without a wake.
fn expired(place: &RunPlace, run: &Run, monitor: &Monitor) -> Ended {
    Ended {
        event: Some(ended(place, run, monitor, "expired", "ttl-expired")),
        wake: None,
    }
}

/// `monitor` ended with its run, which ended with `result`.
fn completed(place: &RunPlace, run: &Run, monitor: &Monitor, mut result: ScriptLastRun) -> Value {
    result.run_id = None;
    let mut row = ended(place, run, monitor, "completed", "finished");
    row["result"] = serde_json::to_value(result).unwrap_or(Value::Null);
    row
}

/// `monitor` matched `line`: it settles triggered and wakes its owner
/// (unless it ran out first).
fn triggered(place: &RunPlace, run: &Run, monitor: &Monitor, line: &str) -> Ended {
    if monitor.expired() {
        return expired(place, run, monitor);
    }
    let mut row = ended(place, run, monitor, "triggered", "output-match");
    row["trigger"] = json!({ "observedLineCount": monitor.lines, "matchedLine": line });
    Ended {
        event: Some(row),
        wake: Some(format!(
            "Script \"{}\" (machine run on {}): output matched `{}`: {line}",
            run.def.name,
            run.machine(),
            monitor
                .pattern
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default()
        )),
    }
}

fn idle_after(state: &ScriptRuntimeState) -> ScriptRuntimeState {
    ScriptRuntimeState {
        run_id: state.run_id.clone(),
        status: ScriptStatus::Idle,
        started_at: state.started_at.clone(),
        stopped_at: Some(now_iso()),
        restart_count: state.restart_count,
        ..ScriptRuntimeState::default()
    }
}

fn last_run(
    state: &ScriptRuntimeState,
    outcome: ScriptRunOutcome,
    exit_code: Option<i64>,
    error: Option<String>,
) -> ScriptLastRun {
    ScriptLastRun {
        run_id: state.run_id.clone(),
        outcome,
        exit_code,
        started_at: state.started_at.clone(),
        stopped_at: now_iso(),
        error,
    }
}

/// Take `run`'s monitor and settle it as its run ends cancelled with
/// `reason` (a stop, restart or definition change, as the home run's
/// monitor): the terminal row and, unless it expired, the wake.
fn cancelled_wake(place: &RunPlace, run: &mut Run, reason: &str) -> Ended {
    let Some(monitor) = run.monitor.take() else {
        return Ended::default();
    };
    if monitor.expired() {
        return expired(place, run, &monitor);
    }
    let result = last_run(&run.state, ScriptRunOutcome::Cancelled, None, None);
    Ended {
        event: Some(completed(place, run, &monitor, result)),
        wake: Some(format!(
            "Script \"{}\" (machine run on {}) was cancelled: {reason}",
            run.def.name,
            run.machine()
        )),
    }
}

/// `remote-script-agent-ending`: `place`'s agent is being stopped for good.
fn agent_ending(place: &RunPlace) -> Error {
    Error::InvalidParams(format!(
        "remote-script-agent-ending: agent {} is being stopped for good; its scripts cannot start",
        place.agent_id
    ))
}

fn no_run(place: &RunPlace) -> Error {
    Error::InvalidParams(format!(
        "script {} has no machine run for remote agent {}",
        place.script_id, place.agent_id
    ))
}

#[cfg(test)]
#[path = "machine_runs_tests.rs"]
mod tests;
