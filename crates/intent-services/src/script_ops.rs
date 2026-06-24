//! `script.*` reconciled onto the unified `intent-pty` host (§5.8, §12.2).
//!
//! Ports `script-process-manager.ts` so scripts run as real PTYs in the *same*
//! [`PtyHost`] that backs `terminal.*` — there is no separate process-spawning
//! path. A script's PTY is workspace-scoped, so it appears in `terminal.list`
//! and a terminal can read its scrollback via `terminal.getBuffer` (attach to a
//! running script). `service` scripts auto-restart per the ported backoff
//! policy; `command` scripts run once. Service output is scanned for a local
//! dev-server URL, surfaced on the `script:state` event for the `forward.*`
//! hook. Live output streams as `script:output` (base64 `chunk`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use intent_core::events::{SCRIPT_OUTPUT, SCRIPT_STATE};
use intent_core::{
    now_iso, Error, Result, Script, ScriptCreateParams, ScriptMode, ScriptRuntimeState,
    ScriptStatus, WorkspaceId,
};
use intent_pty::{PtyExit, PtyHost, PtyId, SpawnSpec};
use intent_store::{NewEvent, Store};
use serde_json::{json, Value};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use crate::events::EventBus;
use crate::{publish_event, system_actor};

/// Delay before an auto-restart attempt (mirrors `AUTO_RESTART_DELAY_MS`).
const AUTO_RESTART_DELAY: Duration = Duration::from_millis(1000);
/// Max consecutive auto-restarts for a service (mirrors `AUTO_RESTART_MAX_RETRIES`).
const AUTO_RESTART_MAX_RETRIES: u32 = 5;
/// A run shorter than this is treated as a config error — do not auto-restart.
const TOO_FAST_MS: u128 = 2000;
/// How often the streamer polls for a natural process exit (mirrors `terminal_ops`).
const EXIT_POLL: Duration = Duration::from_millis(25);

/// In-memory bookkeeping for one managed script (definition + runtime + the live
/// PTY + supervisor task). Held in the shared registry on [`Services`].
///
/// [`Services`]: crate::Services
pub(crate) struct ManagedScript {
    def: Script,
    state: ScriptRuntimeState,
    pty_id: Option<PtyId>,
    stopped_by_user: bool,
    supervisor: Option<tokio::task::JoinHandle<()>>,
}

/// The shared registry of scripts, keyed by script id, across all workspaces.
pub(crate) type ScriptRegistry = Arc<Mutex<HashMap<String, ManagedScript>>>;

/// Thin service over the unified host: holds the shared PTY host, the event bus,
/// the store (for workspace-root resolution), and the script registry. Cheap to
/// clone (all handles); the supervisor task owns its own clone.
#[derive(Clone)]
pub(crate) struct ScriptManager {
    pty: Arc<PtyHost>,
    bus: Option<EventBus>,
    store: Store,
    scripts: ScriptRegistry,
}

impl ScriptManager {
    /// Wire the manager over the shared host/bus/store/registry.
    pub(crate) fn new(
        pty: Arc<PtyHost>,
        bus: Option<EventBus>,
        store: Store,
        scripts: ScriptRegistry,
    ) -> Self {
        Self {
            pty,
            bus,
            store,
            scripts,
        }
    }

    /// `script.create`: register a definition and return it.
    pub(crate) async fn create(
        &self,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> Result<Value> {
        let id = params
            .script_id
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let def = Script {
            id: id.clone(),
            workspace_id: workspace_id.as_str().to_string(),
            name: params.name,
            command: params.command,
            cwd: params.cwd,
            env: params.env,
            mode: params.mode,
            category: params.category,
            source: "user".to_string(),
            auto_start: params.auto_start,
            created_at: now_iso(),
            updated_at: None,
        };
        self.scripts.lock().unwrap().insert(
            id,
            ManagedScript {
                def: def.clone(),
                state: ScriptRuntimeState::default(),
                pty_id: None,
                stopped_by_user: false,
                supervisor: None,
            },
        );
        Ok(serde_json::to_value(def).unwrap_or_else(|_| json!({})))
    }

    /// `script.list`: the workspace's scripts with merged runtime state.
    pub(crate) fn list(&self, workspace_id: &WorkspaceId) -> Result<Value> {
        let guard = self.scripts.lock().unwrap();
        let mut scripts: Vec<(String, Value)> = guard
            .values()
            .filter(|m| m.def.workspace_id == workspace_id.as_str())
            .map(|m| (m.def.created_at.clone(), with_runtime(&m.def, &m.state)))
            .collect();
        scripts.sort_by(|a, b| a.0.cmp(&b.0));
        let scripts: Vec<Value> = scripts.into_iter().map(|(_, v)| v).collect();
        Ok(json!({ "scripts": scripts }))
    }

    /// `script.remove`: stop (if running) and forget a script.
    pub(crate) async fn remove(&self, script_id: &str) -> Result<Value> {
        let removed = self.scripts.lock().unwrap().remove(script_id);
        let Some(mut managed) = removed else {
            return Err(Error::NotFound(format!("script {script_id}")));
        };
        if let Some(handle) = managed.supervisor.take() {
            handle.abort();
        }
        if let Some(pty_id) = managed.pty_id {
            self.pty.kill(pty_id).await;
        }
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// `script.status`: the script's runtime state.
    pub(crate) fn status(&self, script_id: &str) -> Result<Value> {
        let guard = self.scripts.lock().unwrap();
        let m = guard
            .get(script_id)
            .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
        Ok(serde_json::to_value(&m.state).unwrap_or_else(|_| json!({})))
    }

    /// `script.output`: the script's current PTY scrollback rendered as
    /// plaintext output-buffer text — a bare JSON string, not an object
    /// (mirrors `ws.script.output`, PROTOCOL §5.8). The trailing `max_lines`
    /// (default 100) are returned under a `[... lines]` header; an empty buffer
    /// yields `"No output yet."`.
    pub(crate) fn output(&self, script_id: &str, max_lines: Option<i64>) -> Result<Value> {
        let pty_id = {
            let guard = self.scripts.lock().unwrap();
            let m = guard
                .get(script_id)
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            m.pty_id
        };
        let buffer = match pty_id {
            Some(id) => {
                let bytes = self.pty.scrollback(id).unwrap_or_default();
                String::from_utf8_lossy(&bytes).into_owned()
            }
            None => String::new(),
        };
        let line_count = clamp_line_count(max_lines, 100);
        let text = last_n_lines(&buffer, line_count);
        if text.trim().is_empty() {
            return Ok(Value::String("No output yet.".to_string()));
        }
        let total = buffer.split('\n').count();
        let header = if total > line_count {
            format!("[showing last {line_count} of {total} lines]")
        } else {
            format!("[{total} lines]")
        };
        Ok(Value::String(format!("{header}\n{text}")))
    }

    /// `script.start`: spawn the script and run its supervisor loop. A script
    /// already running is a no-op (mirrors the TS warn-and-return).
    pub(crate) async fn start(&self, script_id: &str) -> Result<Value> {
        let def = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(script_id)
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            if m.state.status == ScriptStatus::Running {
                return Ok(json!({ "ok": true, "scriptId": script_id }));
            }
            m.stopped_by_user = false;
            m.def.clone()
        };
        let mgr = self.clone();
        let sid = script_id.to_string();
        let handle = tokio::spawn(async move { mgr.supervise(sid, def).await });
        if let Some(m) = self.scripts.lock().unwrap().get_mut(script_id) {
            m.supervisor = Some(handle);
        } else {
            handle.abort();
        }
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// `script.stop`: flag user-stop, kill the PTY (cancelling auto-restart), and
    /// await the supervisor's teardown.
    pub(crate) async fn stop(&self, script_id: &str) -> Result<Value> {
        let (handle, pty_id, was_running) = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(script_id)
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            m.stopped_by_user = true;
            (
                m.supervisor.take(),
                m.pty_id,
                m.state.status == ScriptStatus::Running,
            )
        };
        if let Some(pty_id) = pty_id {
            self.pty.kill(pty_id).await;
        }
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        if !was_running {
            let mut guard = self.scripts.lock().unwrap();
            if let Some(m) = guard.get_mut(script_id) {
                if m.state.status != ScriptStatus::Running {
                    m.state.status = ScriptStatus::Idle;
                }
            }
        }
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// `script.restart`: stop, reset the restart counter, then start.
    pub(crate) async fn restart(&self, script_id: &str) -> Result<Value> {
        self.stop(script_id).await?;
        {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(script_id)
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            m.state.restart_count = 0;
            m.stopped_by_user = false;
        }
        self.start(script_id).await
    }

    /// `script.run`: run a command-mode script to completion (optional timeout),
    /// returning its captured output + exit code; service scripts return a
    /// `warning` directing callers to `script.start`.
    pub(crate) async fn run(
        &self,
        script_id: &str,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> Result<Value> {
        let def = self
            .scripts
            .lock()
            .unwrap()
            .get(script_id)
            .map(|m| m.def.clone())
            .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
        if def.mode == ScriptMode::Service {
            return Ok(json!({
                "output": "",
                "warning": "Script is a service; use script.start instead of script.run.",
            }));
        }
        let ws = WorkspaceId::from(def.workspace_id.as_str());
        let cwd = self.resolve_cwd(&ws, &def).await?;
        let pty_id = self.pty.spawn(self.build_spec(&ws, &def, &cwd))?;
        self.mark_running(script_id, &ws, pty_id).await;
        let timed_out = match timeout_seconds.filter(|s| *s > 0) {
            Some(s) => {
                let fut = self.run_one(&ws, script_id, pty_id, false);
                match tokio::time::timeout(Duration::from_secs(s as u64), fut).await {
                    Ok(_) => false,
                    Err(_) => {
                        self.pty.kill(pty_id).await;
                        true
                    }
                }
            }
            None => {
                self.run_one(&ws, script_id, pty_id, false).await;
                false
            }
        };
        let exit = self.pty.try_exit(pty_id).ok().flatten();
        self.mark_exited(script_id, &ws, exit.clone()).await;
        let bytes = self.pty.scrollback(pty_id).unwrap_or_default();
        let mut output = String::from_utf8_lossy(&bytes).into_owned();
        if let Some(n) = max_lines.filter(|n| *n > 0) {
            output = last_n_lines(&output, n as usize);
        }
        Ok(json!({
            "exitCode": exit.map(|e| e.exit_code as i64),
            "output": output,
            "timedOut": timed_out,
        }))
    }

    /// The per-script supervisor: spawn → stream → (service) auto-restart per the
    /// ported backoff policy, until a user-stop, a command-mode exit, a too-fast
    /// crash, or the retry cap.
    async fn supervise(self, script_id: String, def: Script) {
        let ws = WorkspaceId::from(def.workspace_id.as_str());
        let cwd = match self.resolve_cwd(&ws, &def).await {
            Ok(c) => c,
            Err(e) => {
                self.fail(&script_id, &ws, &e.to_string()).await;
                return;
            }
        };
        let detect = def.mode == ScriptMode::Service;
        let mut prev: Option<PtyId> = None;
        loop {
            if let Some(old) = prev.take() {
                self.pty.kill(old).await;
            }
            let pty_id = match self.pty.spawn(self.build_spec(&ws, &def, &cwd)) {
                Ok(id) => id,
                Err(e) => {
                    self.fail(&script_id, &ws, &e.to_string()).await;
                    return;
                }
            };
            prev = Some(pty_id);
            let started = Instant::now();
            if !self.mark_running(&script_id, &ws, pty_id).await {
                self.pty.kill(pty_id).await;
                return;
            }
            let exit = self.run_one(&ws, &script_id, pty_id, detect).await;
            let (stopped_by_user, restart_count) =
                match self.mark_exited(&script_id, &ws, exit).await {
                    Some(v) => v,
                    None => return,
                };
            if stopped_by_user || def.mode != ScriptMode::Service {
                break;
            }
            if started.elapsed().as_millis() < TOO_FAST_MS {
                let ms = started.elapsed().as_millis();
                self.emit_separator(
                    &ws,
                    &script_id,
                    &format!(
                        "Exited too quickly ({ms}ms) — not restarting. Check your configuration."
                    ),
                )
                .await;
                break;
            }
            if restart_count >= AUTO_RESTART_MAX_RETRIES {
                break;
            }
            let attempt = {
                let mut guard = self.scripts.lock().unwrap();
                let Some(m) = guard.get_mut(&script_id) else {
                    return;
                };
                m.state.restart_count += 1;
                m.state.restart_count
            };
            tokio::time::sleep(AUTO_RESTART_DELAY).await;
            {
                let guard = self.scripts.lock().unwrap();
                match guard.get(&script_id) {
                    Some(m) if m.stopped_by_user => break,
                    Some(_) => {}
                    None => return,
                }
            }
            self.emit_separator(
                &ws,
                &script_id,
                &format!("Restarting (attempt {attempt}/{AUTO_RESTART_MAX_RETRIES})"),
            )
            .await;
        }
    }

    /// Attach to a freshly spawned PTY, fan its output onto the bus as
    /// `script:output`, scan for a dev-server URL (service mode), and return its
    /// exit status once the child ends (polling like `terminal_ops`).
    async fn run_one(
        &self,
        ws: &WorkspaceId,
        script_id: &str,
        pty_id: PtyId,
        detect_url: bool,
    ) -> Option<PtyExit> {
        let attachment = match self.pty.attach(pty_id) {
            Ok(a) => a,
            Err(_) => return self.pty.try_exit(pty_id).ok().flatten(),
        };
        let mut live = attachment.live;
        let mut url_done = !detect_url;
        if !attachment.backlog.is_empty() {
            self.emit_output(ws, script_id, &attachment.backlog).await;
            if !url_done {
                url_done = self
                    .try_detect_url(ws, script_id, &attachment.backlog)
                    .await;
            }
        }
        loop {
            tokio::select! {
                recv = live.recv() => match recv {
                    Ok(chunk) => {
                        self.emit_output(ws, script_id, &chunk).await;
                        if !url_done {
                            url_done = self.try_detect_url(ws, script_id, &chunk).await;
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                },
                _ = tokio::time::sleep(EXIT_POLL) => {
                    if matches!(self.pty.try_exit(pty_id), Ok(Some(_))) {
                        loop {
                            match live.try_recv() {
                                Ok(chunk) => {
                                    self.emit_output(ws, script_id, &chunk).await;
                                    if !url_done {
                                        url_done =
                                            self.try_detect_url(ws, script_id, &chunk).await;
                                    }
                                }
                                Err(TryRecvError::Lagged(_)) => continue,
                                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
                            }
                        }
                        break;
                    }
                }
            }
        }
        self.pty.try_exit(pty_id).ok().flatten()
    }

    /// Scan a chunk for the first local dev-server URL; on a first hit, latch it
    /// into the runtime state and emit `script:state`. Returns whether detection
    /// is now complete (found or the script vanished).
    async fn try_detect_url(&self, ws: &WorkspaceId, script_id: &str, bytes: &[u8]) -> bool {
        let text = String::from_utf8_lossy(bytes);
        let clean = strip_ansi(&text);
        let Some(url) = find_local_url(&clean) else {
            return false;
        };
        let state = {
            let mut guard = self.scripts.lock().unwrap();
            let Some(m) = guard.get_mut(script_id) else {
                return true;
            };
            if m.state.detected_url.is_some() {
                return true;
            }
            m.state.detected_url = Some(url);
            m.state.clone()
        };
        self.emit_state(ws, script_id, &state).await;
        true
    }

    /// Flip a script to `running` and emit `script:state`. Returns `false` if the
    /// script was removed concurrently (caller should reap the PTY).
    async fn mark_running(&self, script_id: &str, ws: &WorkspaceId, pty_id: PtyId) -> bool {
        let pid = self.pty.pid(pty_id);
        let state = {
            let mut guard = self.scripts.lock().unwrap();
            let Some(m) = guard.get_mut(script_id) else {
                return false;
            };
            m.pty_id = Some(pty_id);
            m.state.status = ScriptStatus::Running;
            m.state.pid = pid;
            m.state.started_at = Some(now_iso());
            m.state.exit_code = None;
            m.state.stopped_at = None;
            m.state.error = None;
            m.state.detected_url = None;
            m.state.clone()
        };
        self.emit_state(ws, script_id, &state).await;
        true
    }

    /// Flip a script to `exited`, record the exit code, and emit `script:state`.
    /// Returns `(stopped_by_user, restart_count)` for the restart decision.
    async fn mark_exited(
        &self,
        script_id: &str,
        ws: &WorkspaceId,
        exit: Option<PtyExit>,
    ) -> Option<(bool, u32)> {
        let (state, flags) = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard.get_mut(script_id)?;
            m.state.status = ScriptStatus::Exited;
            m.state.exit_code = exit.as_ref().map(|e| e.exit_code as i64);
            m.state.stopped_at = Some(now_iso());
            (m.state.clone(), (m.stopped_by_user, m.state.restart_count))
        };
        self.emit_state(ws, script_id, &state).await;
        Some(flags)
    }

    /// Record a spawn/cwd failure on the runtime state and emit `script:state`.
    async fn fail(&self, script_id: &str, ws: &WorkspaceId, err: &str) {
        let state = {
            let mut guard = self.scripts.lock().unwrap();
            let Some(m) = guard.get_mut(script_id) else {
                return;
            };
            m.state.status = ScriptStatus::Exited;
            m.state.error = Some(err.to_string());
            m.state.stopped_at = Some(now_iso());
            m.pty_id = None;
            m.state.clone()
        };
        self.emit_state(ws, script_id, &state).await;
    }

    /// Publish a `script:output` event carrying a base64 output `chunk`.
    async fn emit_output(&self, ws: &WorkspaceId, script_id: &str, bytes: &[u8]) {
        let chunk = base64::engine::general_purpose::STANDARD.encode(bytes);
        publish_event(
            &self.bus,
            script_event(
                ws,
                SCRIPT_OUTPUT,
                json!({ "scriptId": script_id, "chunk": chunk }),
            ),
        )
        .await;
    }

    /// Publish a self-sufficient `script:state` event (the runtime state plus the
    /// `scriptId`).
    async fn emit_state(&self, ws: &WorkspaceId, script_id: &str, state: &ScriptRuntimeState) {
        let mut data = serde_json::to_value(state).unwrap_or_else(|_| json!({}));
        if let Value::Object(ref mut map) = data {
            map.insert("scriptId".to_string(), json!(script_id));
        }
        publish_event(&self.bus, script_event(ws, SCRIPT_STATE, data)).await;
    }

    /// Stream a synthetic separator line (e.g. restart notices) as `script:output`.
    async fn emit_separator(&self, ws: &WorkspaceId, script_id: &str, message: &str) {
        let line = format!("\r\n--- {message} ---\r\n");
        self.emit_output(ws, script_id, line.as_bytes()).await;
    }

    /// Resolve the script's working directory: the workspace worktree root, or a
    /// `cwd` relative to it (rejecting absolute paths / `..` traversal).
    async fn resolve_cwd(&self, ws: &WorkspaceId, def: &Script) -> Result<Option<PathBuf>> {
        let workspace = self.store.get_workspace(ws).await?;
        let Some(root) = crate::git_ops::worktree_path(&workspace) else {
            return Ok(None);
        };
        match def.cwd.as_deref().filter(|s| !s.is_empty()) {
            None => Ok(Some(root)),
            Some(rel) => {
                if !is_safe_relative(rel) {
                    return Err(Error::Internal(format!(
                        "Script \"{}\" cwd escapes workspace root: {rel}",
                        def.name
                    )));
                }
                Ok(Some(root.join(rel)))
            }
        }
    }

    /// Build the [`SpawnSpec`] for a run: login shell + `-c command`, workspace
    /// scope, and the FORCE_COLOR/TERM + script env overlay.
    fn build_spec(&self, ws: &WorkspaceId, def: &Script, cwd: &Option<PathBuf>) -> SpawnSpec {
        let shell = default_shell();
        let mut spec = SpawnSpec::new(ws.as_str(), shell.clone());
        spec.args = shell_args(&shell, &def.command);
        spec.cwd = cwd.clone();
        let mut env = vec![
            ("FORCE_COLOR".to_string(), "1".to_string()),
            ("TERM".to_string(), "xterm-256color".to_string()),
        ];
        if let Some(map) = &def.env {
            for (k, v) in map {
                env.push((k.clone(), v.clone()));
            }
        }
        spec.env = env;
        spec
    }
}

/// The login shell to run scripts under (`$SHELL`, then `/bin/sh`).
fn default_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
}

/// Shell args for `command`, mirroring the TS `getShellArgs`: `/bin/sh` uses
/// `-c`; zsh/bash use `-l -c` (login shell for nvm/fnm PATH); Windows uses
/// PowerShell `-Command` or `cmd /c`.
fn shell_args(shell: &str, command: &str) -> Vec<String> {
    let file = std::path::Path::new(shell)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(shell)
        .to_lowercase();
    let base = file.strip_suffix(".exe").unwrap_or(&file);
    if cfg!(windows) {
        if base == "powershell" || base == "pwsh" {
            return vec![
                "-NoProfile".to_string(),
                "-NoLogo".to_string(),
                "-NonInteractive".to_string(),
                "-Command".to_string(),
                command.to_string(),
            ];
        }
        return vec!["/c".to_string(), command.to_string()];
    }
    if base == "sh" {
        return vec!["-c".to_string(), command.to_string()];
    }
    vec!["-l".to_string(), "-c".to_string(), command.to_string()]
}

/// Clamp a caller-supplied `maxLines` to a sane positive count (mirrors
/// `clampLineCount`): use `fallback` when absent, then bound to `1..=10_000`.
fn clamp_line_count(max_lines: Option<i64>, fallback: usize) -> usize {
    max_lines.unwrap_or(fallback as i64).clamp(1, 10_000) as usize
}

/// The trailing `n` newline-delimited lines of `text` (mirrors `getLastText`).
fn last_n_lines(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    if lines.len() <= n {
        return text.to_string();
    }
    lines[lines.len() - n..].join("\n")
}

/// Whether `rel` is a safe workspace-relative path (no absolute root, no `..`).
fn is_safe_relative(rel: &str) -> bool {
    let p = std::path::Path::new(rel);
    !p.is_absolute()
        && !p
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
}

/// Merge a script definition with its runtime state into one `script.list` entry.
fn with_runtime(def: &Script, state: &ScriptRuntimeState) -> Value {
    let mut v = serde_json::to_value(def).unwrap_or_else(|_| json!({}));
    if let Value::Object(ref mut map) = v {
        map.insert(
            "runtime".to_string(),
            serde_json::to_value(state).unwrap_or(Value::Null),
        );
    }
    v
}

/// Strip ANSI CSI (`ESC [ … letter`) and OSC (`ESC ] … BEL`) sequences before
/// URL matching (mirrors the TS regex pre-clean).
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('[') => {
                for nc in chars.by_ref() {
                    if nc.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            Some(']') => {
                for nc in chars.by_ref() {
                    if nc == '\u{07}' {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Find the first `http(s)://localhost|127.0.0.1:PORT[/path]` URL (ported from
/// the TS `URL_REGEX`; a port is required).
fn find_local_url(text: &str) -> Option<String> {
    let mut i = 0;
    while i < text.len() {
        if let Some(url) = match_local_url(&text[i..]) {
            return Some(url);
        }
        i += 1;
        while i < text.len() && !text.is_char_boundary(i) {
            i += 1;
        }
    }
    None
}

/// Match a local dev-server URL anchored at the start of `rest`, or `None`.
fn match_local_url(rest: &str) -> Option<String> {
    let scheme = if rest.starts_with("https://") {
        "https://"
    } else if rest.starts_with("http://") {
        "http://"
    } else {
        return None;
    };
    let after_scheme = &rest[scheme.len()..];
    let host = if after_scheme.starts_with("localhost") {
        "localhost"
    } else if after_scheme.starts_with("127.0.0.1") {
        "127.0.0.1"
    } else {
        return None;
    };
    let after_colon = after_scheme[host.len()..].strip_prefix(':')?;
    let digits: String = after_colon
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        return None;
    }
    let after_port = &after_colon[digits.len()..];
    let path: String = if after_port.starts_with('/') {
        after_port
            .chars()
            .take_while(|&c| !c.is_whitespace() && !matches!(c, ')' | '}' | ']' | '"' | '\''))
            .collect()
    } else {
        String::new()
    };
    Some(format!("{scheme}{host}:{digits}{path}"))
}

/// Build a `script:*` change event with the daemon system actor.
fn script_event(workspace_id: &WorkspaceId, event_type: &str, data: Value) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}
