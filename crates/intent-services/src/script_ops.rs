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

/// The shared registry of scripts, keyed by `(workspace_id, script_id)` so a
/// client-supplied `scriptId` (`"dev"`, `"build"`, …) can be minted concurrently
/// by any number of workspaces without collision or cross-workspace mutation.
pub(crate) type ScriptRegistry = Arc<Mutex<HashMap<(WorkspaceId, String), ManagedScript>>>;

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

    /// `script.create`: register (or upsert) a definition, persist it, and
    /// return it.
    pub(crate) async fn create(
        &self,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> Result<Value> {
        let id = params
            .script_id
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        // Upsert of an existing id (`ws.script.create` with `scriptId`):
        // the definition is replaced with `source`/`createdAt` preserved and
        // `updatedAt` stamped (FE parity), and — unlike the FE, whose manager
        // re-reads definitions from disk — the daemon must tear down the old
        // supervisor/PTY here so a running replaced script is never orphaned.
        let existing = self
            .scripts
            .lock()
            .unwrap()
            .remove(&(workspace_id.clone(), id.clone()));
        let (source, created_at, updated_at) = match &existing {
            Some(old) => (
                old.def.source.clone(),
                old.def.created_at.clone(),
                Some(now_iso()),
            ),
            None => ("user".to_string(), now_iso(), None),
        };
        if let Some(mut old) = existing {
            if let Some(handle) = old.supervisor.take() {
                handle.abort();
            }
            if let Some(pty_id) = old.pty_id {
                self.pty.kill(pty_id).await;
            }
        }
        let def = Script {
            id: id.clone(),
            workspace_id: workspace_id.as_str().to_string(),
            name: params.name,
            command: params.command,
            cwd: params.cwd,
            env: params.env,
            mode: params.mode,
            category: params.category,
            source,
            auto_start: params.auto_start,
            created_at,
            updated_at,
        };
        // Persist first (FE `upsertScript` parity — definitions survive a
        // daemon restart); the runtime registry only registers what is durable.
        self.store.upsert_script(&def).await?;
        self.scripts.lock().unwrap().insert(
            (workspace_id.clone(), id),
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

    /// Boot-time hydration: load every persisted definition into the runtime
    /// registry with a fresh idle state (runtime state is never persisted).
    /// Ids already registered are left untouched. Returns the number loaded.
    pub(crate) async fn hydrate(&self) -> Result<usize> {
        let defs = self.store.list_all_scripts().await?;
        let mut guard = self.scripts.lock().unwrap();
        let mut loaded = 0;
        for def in defs {
            let key = (WorkspaceId::from(def.workspace_id.as_str()), def.id.clone());
            guard.entry(key).or_insert_with(|| {
                loaded += 1;
                ManagedScript {
                    def,
                    state: ScriptRuntimeState::default(),
                    pty_id: None,
                    stopped_by_user: false,
                    supervisor: None,
                }
            });
        }
        Ok(loaded)
    }

    /// `script.list`: the workspace's scripts with merged runtime state.
    /// When empty, bootstrap from repo config `scripts[]` (FE parity:
    /// scripts.ipc.ts L291-320).
    pub(crate) async fn list(&self, workspace_id: &WorkspaceId) -> Result<Value> {
        // First check: read existing scripts
        {
            let guard = self.scripts.lock().unwrap();
            let mut scripts: Vec<(String, Value)> = guard
                .iter()
                .filter(|((ws, _), _)| ws == workspace_id)
                .map(|(_, m)| (m.def.created_at.clone(), with_runtime(&m.def, &m.state)))
                .collect();
            if !scripts.is_empty() {
                scripts.sort_by(|a, b| a.0.cmp(&b.0));
                let scripts: Vec<Value> = scripts.into_iter().map(|(_, v)| v).collect();
                return Ok(json!({ "scripts": scripts }));
            }
        } // guard dropped here

        // Bootstrap from repo config if workspace has no scripts (double-check inside lock below)
        {
            if let Ok(ws) = self.store.get_workspace(workspace_id).await {
                if let Some(repo_path) = ws
                    .repository_path
                    .as_deref()
                    .filter(|p| !p.is_empty())
                    .map(PathBuf::from)
                {
                    let repo_config = crate::repo_config::read_repo_config(&repo_path).await;
                    if let Some(repo_scripts) = repo_config.scripts {
                        // Double-check emptiness before bootstrapping to avoid race condition
                        // where multiple concurrent calls could each bootstrap duplicate scripts
                        let needs_bootstrap = {
                            let guard = self.scripts.lock().unwrap();
                            !guard.iter().any(|((ws, _), _)| ws == workspace_id)
                        };
                        if !needs_bootstrap {
                            // Another thread bootstrapped while we were reading repo config
                            let guard = self.scripts.lock().unwrap();
                            let mut scripts: Vec<(String, Value)> = guard
                                .iter()
                                .filter(|((ws, _), _)| ws == workspace_id)
                                .map(|(_, m)| {
                                    (m.def.created_at.clone(), with_runtime(&m.def, &m.state))
                                })
                                .collect();
                            scripts.sort_by(|a, b| a.0.cmp(&b.0));
                            let scripts: Vec<Value> = scripts.into_iter().map(|(_, v)| v).collect();
                            return Ok(json!({ "scripts": scripts }));
                        }
                        // Convert RepoScript -> Script definitions and persist them
                        let now = now_iso();
                        for repo_script in repo_scripts {
                            let script_id = uuid::Uuid::new_v4().to_string();
                            let script = Script {
                                id: script_id.clone(),
                                workspace_id: workspace_id.to_string(),
                                name: repo_script.name,
                                command: repo_script.command,
                                cwd: repo_script.cwd,
                                env: repo_script.env,
                                mode: match repo_script.mode {
                                    intent_core::RepoScriptMode::Service => ScriptMode::Service,
                                    intent_core::RepoScriptMode::Command => ScriptMode::Command,
                                },
                                category: repo_script.category.map(|c| {
                                    match c {
                                        intent_core::RepoScriptCategory::Dev => "dev",
                                        intent_core::RepoScriptCategory::Test => "test",
                                        intent_core::RepoScriptCategory::Build => "build",
                                        intent_core::RepoScriptCategory::Lint => "lint",
                                        intent_core::RepoScriptCategory::Typecheck => "typecheck",
                                        intent_core::RepoScriptCategory::Format => "format",
                                        intent_core::RepoScriptCategory::Storybook => "storybook",
                                        intent_core::RepoScriptCategory::Other => "other",
                                    }
                                    .to_string()
                                }),
                                source: "user".to_string(),
                                auto_start: repo_script.auto_start,
                                created_at: now.clone(),
                                updated_at: None,
                            };
                            // Persist and register
                            self.store.upsert_script(&script).await?;
                            self.scripts.lock().unwrap().insert(
                                (workspace_id.clone(), script_id),
                                ManagedScript {
                                    def: script,
                                    state: ScriptRuntimeState::default(),
                                    pty_id: None,
                                    stopped_by_user: false,
                                    supervisor: None,
                                },
                            );
                        }
                        // Re-read scripts after bootstrapping
                        let guard = self.scripts.lock().unwrap();
                        let mut scripts: Vec<(String, Value)> = guard
                            .iter()
                            .filter(|((ws, _), _)| ws == workspace_id)
                            .map(|(_, m)| {
                                (m.def.created_at.clone(), with_runtime(&m.def, &m.state))
                            })
                            .collect();
                        scripts.sort_by(|a, b| a.0.cmp(&b.0));
                        let scripts: Vec<Value> = scripts.into_iter().map(|(_, v)| v).collect();
                        return Ok(json!({ "scripts": scripts }));
                    }
                }
            }
        }

        // If we get here, workspace was empty and no repo config scripts found
        Ok(json!({ "scripts": [] }))
    }

    /// `script.remove`: stop (if running), forget, and unpersist a script.
    /// Scoped to `workspace_id`: an id owned by a different workspace surfaces
    /// as `NotFound` (no cross-workspace takeover).
    pub(crate) async fn remove(
        &self,
        workspace_id: &WorkspaceId,
        script_id: &str,
    ) -> Result<Value> {
        let removed = self
            .scripts
            .lock()
            .unwrap()
            .remove(&(workspace_id.clone(), script_id.to_string()));
        let Some(mut managed) = removed else {
            return Err(Error::NotFound(format!("script {script_id}")));
        };
        if let Some(handle) = managed.supervisor.take() {
            handle.abort();
        }
        if let Some(pty_id) = managed.pty_id {
            self.pty.kill(pty_id).await;
        }
        self.store.remove_script(script_id).await?;
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// `script.status`: the script's runtime state. Scoped to `workspace_id`.
    pub(crate) fn status(&self, workspace_id: &WorkspaceId, script_id: &str) -> Result<Value> {
        let guard = self.scripts.lock().unwrap();
        let m = guard
            .get(&(workspace_id.clone(), script_id.to_string()))
            .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
        Ok(serde_json::to_value(&m.state).unwrap_or_else(|_| json!({})))
    }

    /// `script.output`: the script's current PTY scrollback rendered as
    /// plaintext output-buffer text — a bare JSON string, not an object
    /// (mirrors `ws.script.output`, PROTOCOL §5.8). The trailing `max_lines`
    /// (default 100) are returned under a `[... lines]` header; an empty buffer
    /// yields `"No output yet."`.
    ///
    /// TA-2 / §5.5 opt-in pagination: when `paginate` (or a `page_token`) is set,
    /// the historical scrollback is returned as a `{ items, nextToken }` envelope
    /// of lines ordered newest→oldest with an opaque continuation token, instead
    /// of the legacy bare string.
    pub(crate) fn output(
        &self,
        workspace_id: &WorkspaceId,
        script_id: &str,
        max_lines: Option<i64>,
        paginate: bool,
        page_token: Option<String>,
    ) -> Result<Value> {
        let pty_id = {
            let guard = self.scripts.lock().unwrap();
            let m = guard
                .get(&(workspace_id.clone(), script_id.to_string()))
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
        if paginate || page_token.is_some() {
            return Ok(crate::pagination::paginate_text_lines(
                &buffer,
                max_lines,
                page_token.as_deref(),
            ));
        }
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
    /// already running is a no-op (mirrors the TS warn-and-return). Scoped to
    /// `workspace_id`.
    pub(crate) async fn start(&self, workspace_id: &WorkspaceId, script_id: &str) -> Result<Value> {
        let key = (workspace_id.clone(), script_id.to_string());
        let def = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(&key)
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            if m.state.status == ScriptStatus::Running {
                return Ok(json!({ "ok": true, "scriptId": script_id }));
            }
            m.stopped_by_user = false;
            m.def.clone()
        };
        let mgr = self.clone();
        let ws = workspace_id.clone();
        let sid = script_id.to_string();
        let handle = tokio::spawn(async move { mgr.supervise(ws, sid, def).await });
        if let Some(m) = self.scripts.lock().unwrap().get_mut(&key) {
            m.supervisor = Some(handle);
        } else {
            handle.abort();
        }
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// `script.stop`: flag user-stop, kill the PTY (cancelling auto-restart), and
    /// await the supervisor's teardown. Scoped to `workspace_id`.
    pub(crate) async fn stop(&self, workspace_id: &WorkspaceId, script_id: &str) -> Result<Value> {
        let key = (workspace_id.clone(), script_id.to_string());
        let (handle, pty_id, was_running) = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(&key)
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
            if let Some(m) = guard.get_mut(&key) {
                if m.state.status != ScriptStatus::Running {
                    m.state.status = ScriptStatus::Idle;
                }
            }
        }
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// `script.restart`: stop, reset the restart counter, then start. Scoped to
    /// `workspace_id`.
    pub(crate) async fn restart(
        &self,
        workspace_id: &WorkspaceId,
        script_id: &str,
    ) -> Result<Value> {
        self.stop(workspace_id, script_id).await?;
        {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(&(workspace_id.clone(), script_id.to_string()))
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            m.state.restart_count = 0;
            m.stopped_by_user = false;
        }
        self.start(workspace_id, script_id).await
    }

    /// `script.run`: run a command-mode script to completion (optional timeout),
    /// returning its captured output + exit code; service scripts return a
    /// `warning` directing callers to `script.start`. Scoped to `workspace_id`.
    pub(crate) async fn run(
        &self,
        workspace_id: &WorkspaceId,
        script_id: &str,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> Result<Value> {
        let def = self
            .scripts
            .lock()
            .unwrap()
            .get(&(workspace_id.clone(), script_id.to_string()))
            .map(|m| m.def.clone())
            .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
        if def.mode == ScriptMode::Service {
            return Ok(json!({
                "output": "",
                "warning": "Script is a service; use script.start instead of script.run.",
            }));
        }
        let ws = workspace_id.clone();
        let cwd = self.resolve_cwd(&ws, &def).await?;
        let pty_id = self.pty.spawn(self.build_spec(&ws, &def, &cwd))?;
        self.mark_running(&ws, script_id, pty_id).await;
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
        self.mark_exited(&ws, script_id, exit.clone()).await;
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
    /// crash, or the retry cap. Scoped to `workspace_id` so registry lookups use
    /// the composite `(workspace_id, script_id)` key.
    async fn supervise(self, ws: WorkspaceId, script_id: String, def: Script) {
        let cwd = match self.resolve_cwd(&ws, &def).await {
            Ok(c) => c,
            Err(e) => {
                self.fail(&ws, &script_id, &e.to_string()).await;
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
                    self.fail(&ws, &script_id, &e.to_string()).await;
                    return;
                }
            };
            prev = Some(pty_id);
            let started = Instant::now();
            if !self.mark_running(&ws, &script_id, pty_id).await {
                self.pty.kill(pty_id).await;
                return;
            }
            let exit = self.run_one(&ws, &script_id, pty_id, detect).await;
            let (stopped_by_user, restart_count) =
                match self.mark_exited(&ws, &script_id, exit).await {
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
            let key = (ws.clone(), script_id.clone());
            let attempt = {
                let mut guard = self.scripts.lock().unwrap();
                let Some(m) = guard.get_mut(&key) else {
                    return;
                };
                m.state.restart_count += 1;
                m.state.restart_count
            };
            tokio::time::sleep(AUTO_RESTART_DELAY).await;
            {
                let guard = self.scripts.lock().unwrap();
                match guard.get(&key) {
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
            let Some(m) = guard.get_mut(&(ws.clone(), script_id.to_string())) else {
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
    async fn mark_running(&self, ws: &WorkspaceId, script_id: &str, pty_id: PtyId) -> bool {
        let pid = self.pty.pid(pty_id);
        let state = {
            let mut guard = self.scripts.lock().unwrap();
            let Some(m) = guard.get_mut(&(ws.clone(), script_id.to_string())) else {
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
        ws: &WorkspaceId,
        script_id: &str,
        exit: Option<PtyExit>,
    ) -> Option<(bool, u32)> {
        let (state, flags) = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard.get_mut(&(ws.clone(), script_id.to_string()))?;
            m.state.status = ScriptStatus::Exited;
            m.state.exit_code = exit.as_ref().map(|e| e.exit_code as i64);
            m.state.stopped_at = Some(now_iso());
            (m.state.clone(), (m.stopped_by_user, m.state.restart_count))
        };
        self.emit_state(ws, script_id, &state).await;
        Some(flags)
    }

    /// Record a spawn/cwd failure on the runtime state and emit `script:state`.
    async fn fail(&self, ws: &WorkspaceId, script_id: &str, err: &str) {
        let state = {
            let mut guard = self.scripts.lock().unwrap();
            let Some(m) = guard.get_mut(&(ws.clone(), script_id.to_string())) else {
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
    /// scope, and the FORCE_COLOR/TERM + enhanced-PATH + script env overlay,
    /// with `npm_config_prefix` scrubbed so nvm's login-shell init succeeds.
    fn build_spec(&self, ws: &WorkspaceId, def: &Script, cwd: &Option<PathBuf>) -> SpawnSpec {
        let shell = default_shell();
        let mut spec = SpawnSpec::new(ws.as_str(), shell.clone());
        spec.args = shell_args(&shell, &def.command);
        spec.cwd = cwd.clone();
        spec.env = spawn_env_overlay(def.env.as_ref());
        spec.env_remove = SCRUBBED_ENV_VARS.iter().map(|s| s.to_string()).collect();
        spec
    }
}

/// Environment variables removed from daemon-spawned shells. `npm_config_prefix`
/// (set by the app's launcher) makes nvm abort its `~/.zshrc` init, which breaks
/// the wrapped git/node tools agents rely on; the overlay can only add keys, so
/// this is applied via `SpawnSpec::env_remove` at the PTY spawn site.
const SCRUBBED_ENV_VARS: &[&str] = &["npm_config_prefix"];

/// Build the env overlay for a spawned script/agent shell: FORCE_COLOR/TERM, an
/// enhanced PATH (essential system dirs + homebrew + node/version-manager dirs),
/// then the script's own `env` last so it can override. The enhanced PATH keeps
/// git/node resolvable even when the daemon inherited a sparse Finder/launchd
/// PATH or the login-shell init is degraded.
fn spawn_env_overlay(
    def_env: Option<&std::collections::BTreeMap<String, String>>,
) -> Vec<(String, String)> {
    let mut env = vec![
        ("FORCE_COLOR".to_string(), "1".to_string()),
        ("TERM".to_string(), "xterm-256color".to_string()),
    ];
    if let Some(path) = enhanced_shell_path() {
        env.push(("PATH".to_string(), path));
    }
    if let Some(map) = def_env {
        for (k, v) in map {
            env.push((k.clone(), v.clone()));
        }
    }
    env
}

/// The enhanced PATH for a spawned shell, reusing the canonical de-duplicated,
/// order-preserving dir list (current PATH first, then essential system dirs and
/// node/version-manager locations). Returns `None` when no dirs resolve.
fn enhanced_shell_path() -> Option<String> {
    let dirs = crate::auggie_discovery::enhanced_path_dirs();
    if dirs.is_empty() {
        return None;
    }
    std::env::join_paths(dirs)
        .ok()
        .map(|joined| joined.to_string_lossy().into_owned())
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use intent_core::{
        now_iso, ScriptCreateParams, ScriptMode, ScriptStatus, Workspace, WorkspaceActivity,
        WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    use serde_json::{json, Value};

    use super::*;
    use crate::events::{EventBus, Subscription, SubscriptionFilter};
    use crate::Services;

    // ---- pure-helper tests (no PTY, no event bus) --------------------------

    #[test]
    fn clamp_line_count_clamps_extremes_and_uses_fallback() {
        assert_eq!(clamp_line_count(None, 100), 100);
        assert_eq!(clamp_line_count(Some(0), 50), 1);
        assert_eq!(clamp_line_count(Some(-5), 50), 1);
        assert_eq!(clamp_line_count(Some(99_999), 50), 10_000);
        assert_eq!(clamp_line_count(Some(42), 50), 42);
    }

    #[test]
    fn last_n_lines_returns_verbatim_when_short_enough() {
        assert_eq!(last_n_lines("a\nb\nc", 5), "a\nb\nc");
        assert_eq!(last_n_lines("a\nb\nc", 3), "a\nb\nc");
    }

    #[test]
    fn last_n_lines_keeps_trailing_n_when_longer() {
        assert_eq!(last_n_lines("a\nb\nc\nd\ne", 2), "d\ne");
        assert_eq!(last_n_lines("a\nb\nc\nd\ne", 1), "e");
    }

    #[test]
    fn is_safe_relative_accepts_subpath_rejects_absolute_and_parent() {
        assert!(is_safe_relative("src/main.rs"));
        assert!(is_safe_relative("a/b/c"));
        assert!(!is_safe_relative("/etc/passwd"));
        assert!(!is_safe_relative("../escape"));
        assert!(!is_safe_relative("a/../b"));
    }

    #[test]
    fn strip_ansi_removes_csi_and_osc_sequences() {
        assert_eq!(strip_ansi("\u{1b}[31mred\u{1b}[0m text"), "red text");
        assert_eq!(strip_ansi("\u{1b}]0;title\u{07}body"), "body");
        assert_eq!(strip_ansi("plain"), "plain");
        // Lone ESC trailer (no follow-up) is dropped without panic.
        assert_eq!(strip_ansi("ok\u{1b}"), "ok");
    }

    #[test]
    fn find_local_url_matches_http_and_https_with_port_and_path() {
        assert_eq!(
            find_local_url("listening on http://localhost:3000/").as_deref(),
            Some("http://localhost:3000/")
        );
        assert_eq!(
            find_local_url("ready: https://127.0.0.1:8443/api?x=1").as_deref(),
            Some("https://127.0.0.1:8443/api?x=1")
        );
        assert_eq!(
            find_local_url("dev http://localhost:5173 done").as_deref(),
            Some("http://localhost:5173")
        );
    }

    #[test]
    fn find_local_url_ignores_non_local_or_portless() {
        assert!(find_local_url("https://example.com:443/").is_none());
        assert!(find_local_url("http://localhost/no-port").is_none());
        assert!(find_local_url("nothing here").is_none());
    }

    #[test]
    fn find_local_url_terminates_path_on_whitespace_and_punctuation() {
        assert_eq!(
            find_local_url("see (http://localhost:3000/x) for").as_deref(),
            Some("http://localhost:3000/x")
        );
        assert_eq!(
            find_local_url("\"http://localhost:3000/y\" said it").as_deref(),
            Some("http://localhost:3000/y")
        );
    }

    #[test]
    fn match_local_url_rejects_unsupported_schemes_and_hosts() {
        assert!(match_local_url("ftp://localhost:3000/").is_none());
        assert!(match_local_url("http://example.com:3000/").is_none());
        assert!(match_local_url("http://localhost").is_none());
    }

    #[test]
    fn with_runtime_attaches_runtime_field() {
        let def = Script {
            id: "s1".into(),
            workspace_id: "ws".into(),
            name: "n".into(),
            command: "echo".into(),
            cwd: None,
            env: None,
            mode: ScriptMode::Command,
            category: None,
            source: "user".into(),
            auto_start: None,
            created_at: "t".into(),
            updated_at: None,
        };
        let state = ScriptRuntimeState {
            status: ScriptStatus::Running,
            restart_count: 2,
            ..Default::default()
        };
        let merged = with_runtime(&def, &state);
        assert_eq!(merged["id"], "s1");
        assert_eq!(merged["runtime"]["status"], "running");
        assert_eq!(merged["runtime"]["restartCount"], 2);
    }

    #[test]
    fn shell_args_uses_dash_c_for_sh_and_login_for_bash_zsh() {
        assert_eq!(shell_args("/bin/sh", "echo hi"), vec!["-c", "echo hi"]);
        assert_eq!(
            shell_args("/bin/bash", "echo hi"),
            vec!["-l", "-c", "echo hi"]
        );
        assert_eq!(
            shell_args("/opt/homebrew/bin/zsh", "echo hi"),
            vec!["-l", "-c", "echo hi"]
        );
        // Unknown shell base falls through the login-shell default arm.
        assert_eq!(shell_args("/bin/fish", "x"), vec!["-l", "-c", "x"]);
    }

    #[test]
    fn spawn_overlay_strips_npm_config_prefix_and_enhances_path() {
        // npm_config_prefix is scrubbed via env_remove, not present as an overlay key.
        assert!(SCRUBBED_ENV_VARS.contains(&"npm_config_prefix"));
        let env = spawn_env_overlay(None);
        assert!(!env.iter().any(|(k, _)| k == "npm_config_prefix"));

        // The enhanced PATH overlay includes the essential system dirs + homebrew.
        let path = env
            .iter()
            .find(|(k, _)| k == "PATH")
            .map(|(_, v)| v.clone())
            .expect("PATH overlay present");
        let dirs: Vec<&str> = path.split(if cfg!(windows) { ';' } else { ':' }).collect();
        assert!(dirs.contains(&"/usr/bin"), "PATH missing /usr/bin: {path}");
        assert!(
            dirs.contains(&"/opt/homebrew/bin"),
            "PATH missing /opt/homebrew/bin: {path}"
        );
    }

    #[test]
    fn spawn_overlay_appends_script_env_last() {
        let mut def_env = std::collections::BTreeMap::new();
        def_env.insert("MY_VAR".to_string(), "1".to_string());
        let env = spawn_env_overlay(Some(&def_env));
        assert_eq!(env.last(), Some(&("MY_VAR".to_string(), "1".to_string())));
    }

    #[test]
    fn script_event_carries_workspace_actor_and_type() {
        let ws = WorkspaceId::from("ws-evt");
        let ev = script_event(&ws, "script:state", json!({ "scriptId": "s" }));
        assert_eq!(ev.workspace_id.as_str(), "ws-evt");
        assert_eq!(ev.event_type, "script:state");
        assert_eq!(ev.data["scriptId"], "s");
        assert_eq!(ev.actor.id.as_deref(), Some("system"));
    }

    #[test]
    fn default_shell_falls_back_when_env_unset() {
        // SAFETY: tests run with cargo-set $SHELL; we just check the function
        // returns *something* non-empty (env or "/bin/sh" fallback).
        let s = default_shell();
        assert!(!s.is_empty());
    }

    // ---- ScriptManager lifecycle / error-path tests ------------------------

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-scriptops-{}.db", uuid::Uuid::new_v4()));
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

    struct WorktreeDir(PathBuf);
    impl WorktreeDir {
        fn new() -> Self {
            let p =
                std::env::temp_dir().join(format!("intentd-scriptops-wt-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&p).expect("mkdir worktree");
            Self(p)
        }
    }
    impl Drop for WorktreeDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn workspace(id: &WorkspaceId, worktree: Option<&PathBuf>) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
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
            worktree_path: worktree.map(|p| p.display().to_string()),
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
        }
    }

    struct Harness {
        _tmp: TempDb,
        services: Services,
        bus: EventBus,
        ws: WorkspaceId,
        _worktree: Option<WorktreeDir>,
    }

    async fn harness() -> Harness {
        harness_with_worktree(false).await
    }

    async fn harness_with_worktree(with_worktree: bool) -> Harness {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        let worktree = if with_worktree {
            Some(WorktreeDir::new())
        } else {
            None
        };
        store
            .insert_workspace(&workspace(&ws, worktree.as_ref().map(|w| &w.0)))
            .await
            .expect("ws");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store).with_event_bus(bus.clone());
        Harness {
            _tmp: tmp,
            services,
            bus,
            ws,
            _worktree: worktree,
        }
    }

    fn subscribe(h: &Harness) -> Subscription {
        h.bus.subscribe(SubscriptionFilter {
            event_types: vec!["script:*".to_string()],
            workspace_id: Some(h.ws.0.clone()),
            ..Default::default()
        })
    }

    async fn create(h: &Harness, params: ScriptCreateParams) -> String {
        let v = h
            .services
            .script_create(h.ws.clone(), params)
            .await
            .expect("create");
        v["id"].as_str().expect("script id").to_string()
    }

    async fn create_simple(h: &Harness, name: &str, command: &str, mode: ScriptMode) -> String {
        create(
            h,
            ScriptCreateParams {
                name: name.to_string(),
                command: command.to_string(),
                mode,
                ..Default::default()
            },
        )
        .await
    }

    async fn await_state<F>(sub: &mut Subscription, timeout: Duration, mut pred: F) -> Value
    where
        F: FnMut(&Value) -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let batch = tokio::time::timeout(remaining, sub.recv())
                .await
                .expect("event delivered before deadline")
                .expect("subscription open");
            for ev in &batch {
                let v = serde_json::to_value(ev).expect("serialize");
                if v["type"] == "script:state" && pred(&v) {
                    return v;
                }
            }
        }
    }

    #[tokio::test]
    async fn script_status_returns_not_found_for_missing_id() {
        let h = harness().await;
        let err = h
            .services
            .script_status(h.ws.clone(), "nope".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn script_remove_returns_not_found_for_missing_id() {
        let h = harness().await;
        let err = h
            .services
            .script_remove(h.ws.clone(), "nope".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn script_start_returns_not_found_for_missing_id() {
        let h = harness().await;
        let err = h
            .services
            .script_start(h.ws.clone(), "nope".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn script_stop_returns_not_found_for_missing_id() {
        let h = harness().await;
        let err = h
            .services
            .script_stop(h.ws.clone(), "nope".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn script_output_returns_not_found_for_missing_id() {
        let h = harness().await;
        let err = h
            .services
            .script_output(h.ws.clone(), "nope".into(), None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn script_run_returns_not_found_for_missing_id() {
        let h = harness().await;
        let err = h
            .services
            .script_run(h.ws.clone(), "nope".into(), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn script_create_preserves_explicit_script_id() {
        let h = harness().await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "named".into(),
                command: "echo".into(),
                mode: ScriptMode::Command,
                script_id: Some("custom-id".into()),
                auto_start: Some(true),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(id, "custom-id");
        let listed = h.services.script_list(h.ws.clone()).await.expect("list");
        let entry = listed["scripts"]
            .as_array()
            .expect("scripts array")
            .iter()
            .find(|s| s["id"] == "custom-id")
            .expect("custom-id in list");
        assert_eq!(entry["autoStart"], true);
        assert_eq!(entry["source"], "user");
        assert_eq!(entry["runtime"]["status"], "idle");
    }

    #[tokio::test]
    async fn script_create_empty_script_id_falls_back_to_uuid() {
        let h = harness().await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "n".into(),
                command: "echo".into(),
                mode: ScriptMode::Command,
                script_id: Some(String::new()),
                ..Default::default()
            },
        )
        .await;
        assert!(!id.is_empty());
        assert_eq!(id.split('-').count(), 5, "id should be a uuid: {id}");
    }

    #[tokio::test]
    async fn script_list_is_workspace_scoped() {
        let h = harness().await;
        let _a = create_simple(&h, "a", "echo a", ScriptMode::Command).await;
        let _b = create_simple(&h, "b", "echo b", ScriptMode::Command).await;
        let other_ws = WorkspaceId::new();
        h.services
            .store()
            .insert_workspace(&workspace(&other_ws, None))
            .await
            .expect("foreign ws");
        let _foreign = h
            .services
            .script_create(
                other_ws.clone(),
                ScriptCreateParams {
                    name: "foreign".into(),
                    command: "echo".into(),
                    mode: ScriptMode::Command,
                    ..Default::default()
                },
            )
            .await
            .expect("foreign create");
        let listed = h.services.script_list(h.ws.clone()).await.expect("list");
        let scripts = listed["scripts"].as_array().expect("scripts array");
        let mut names: Vec<&str> = scripts.iter().filter_map(|s| s["name"].as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["a", "b"], "workspace-scoped");
    }

    #[tokio::test]
    async fn scripts_persist_across_service_restart() {
        let h = harness().await;
        let mut env = std::collections::BTreeMap::new();
        env.insert("PORT".to_string(), "3000".to_string());
        let id = create(
            &h,
            ScriptCreateParams {
                name: "dev".into(),
                command: "npm run dev".into(),
                mode: ScriptMode::Service,
                cwd: Some("web".into()),
                env: Some(env),
                category: Some("dev".into()),
                auto_start: Some(true),
                ..Default::default()
            },
        )
        .await;

        // Simulate a daemon restart: a fresh Services over the same store has
        // an empty registry until the boot-time hydration runs.
        let svc2 = Services::new(h.services.store().clone());
        let listed = svc2.script_list(h.ws.clone()).await.expect("list");
        assert!(
            listed["scripts"].as_array().expect("array").is_empty(),
            "registry starts empty pre-hydration"
        );
        assert_eq!(svc2.hydrate_scripts().await.expect("hydrate"), 1);
        let listed = svc2.script_list(h.ws.clone()).await.expect("list");
        let entry = &listed["scripts"].as_array().expect("array")[0];
        assert_eq!(entry["id"].as_str(), Some(id.as_str()));
        assert_eq!(entry["name"], "dev");
        assert_eq!(entry["command"], "npm run dev");
        assert_eq!(entry["cwd"], "web");
        assert_eq!(entry["env"]["PORT"], "3000");
        assert_eq!(entry["category"], "dev");
        assert_eq!(entry["autoStart"], true);
        assert_eq!(
            entry["runtime"]["status"], "idle",
            "runtime state starts fresh, never persisted"
        );

        // Hydration is idempotent — already-registered ids are untouched.
        assert_eq!(svc2.hydrate_scripts().await.expect("re-hydrate"), 0);
    }

    #[tokio::test]
    async fn script_remove_unpersists_definition() {
        let h = harness().await;
        let id = create_simple(&h, "gone", "echo bye", ScriptMode::Command).await;
        h.services
            .script_remove(h.ws.clone(), id)
            .await
            .expect("remove");

        let svc2 = Services::new(h.services.store().clone());
        assert_eq!(
            svc2.hydrate_scripts().await.expect("hydrate"),
            0,
            "removed script is unpersisted"
        );
    }

    /// Regression: `script.create` upserting an id whose script is running
    /// must stop the old supervisor/PTY (no orphaned process), preserve the
    /// original `createdAt`/`source`, stamp `updatedAt`, and reset the
    /// runtime state to idle.
    #[tokio::test]
    async fn script_create_upsert_stops_running_predecessor() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create(
            &h,
            ScriptCreateParams {
                name: "svc".into(),
                command: "sleep 30".into(),
                mode: ScriptMode::Service,
                script_id: Some("upsert-1".into()),
                ..Default::default()
            },
        )
        .await;
        let listed = h.services.script_list(h.ws.clone()).await.expect("list");
        let created_at = listed["scripts"][0]["createdAt"]
            .as_str()
            .expect("createdAt")
            .to_string();

        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let running = await_state(&mut sub, Duration::from_secs(5), |v| {
            v["data"]["status"] == "running"
        })
        .await;
        let pid = running["data"]["pid"].as_i64().expect("pid");

        // Upsert the same id with a new command while the old one runs.
        let v = create(
            &h,
            ScriptCreateParams {
                name: "svc renamed".into(),
                command: "echo hi".into(),
                mode: ScriptMode::Command,
                script_id: Some(id.clone()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(v, id, "upsert keeps the id");

        let listed = h.services.script_list(h.ws.clone()).await.expect("list");
        let scripts = listed["scripts"].as_array().expect("array");
        assert_eq!(scripts.len(), 1, "no duplicate entry");
        let entry = &scripts[0];
        assert_eq!(entry["name"], "svc renamed");
        assert_eq!(entry["command"], "echo hi");
        assert_eq!(entry["createdAt"].as_str(), Some(created_at.as_str()));
        assert!(entry["updatedAt"].is_string(), "updatedAt stamped");
        assert_eq!(entry["source"], "user", "source preserved");
        assert_eq!(entry["runtime"]["status"], "idle", "runtime reset");

        // The replaced PTY process must die — no orphan (kill -0 fails).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let alive = std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stderr(std::process::Stdio::null())
                .status()
                .expect("run kill -0")
                .success();
            if !alive {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "old script process {pid} is still alive after upsert"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn script_start_is_noop_when_already_running() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "svc", "sleep 5", ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, Duration::from_secs(5), |v| {
            v["data"]["status"] == "running"
        })
        .await;
        // Second start while already running is a no-op (returns Ok, no extra `running` event).
        let v = h
            .services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("noop start");
        assert_eq!(v["ok"], true);
        assert_eq!(v["scriptId"], id);
        // Drain a short window — there must be no SECOND `running` state transition.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, sub.recv()).await {
                Err(_) => break,
                Ok(None) => break,
                Ok(Some(batch)) => {
                    for ev in &batch {
                        let v = serde_json::to_value(ev).expect("serialize");
                        if v["type"] == "script:state" && v["data"]["status"] == "running" {
                            panic!("redundant start re-emitted `running`: {v}");
                        }
                    }
                }
            }
        }
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
    }

    #[tokio::test]
    async fn script_stop_on_idle_keeps_idle_state() {
        let h = harness().await;
        let id = create_simple(&h, "idle", "echo nope", ScriptMode::Command).await;
        let v = h
            .services
            .script_stop(h.ws.clone(), id.clone())
            .await
            .expect("stop");
        assert_eq!(v["ok"], true);
        let st = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .expect("status");
        assert_eq!(st["status"], "idle");
    }

    #[tokio::test]
    async fn script_restart_returns_ok_and_emits_running() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "svc", "sleep 5", ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, Duration::from_secs(5), |v| {
            v["data"]["status"] == "running"
        })
        .await;
        let v = h
            .services
            .script_restart(h.ws.clone(), id.clone())
            .await
            .expect("restart");
        assert_eq!(v["ok"], true);
        await_state(&mut sub, Duration::from_secs(5), |v| {
            v["data"]["status"] == "running"
        })
        .await;
        let st = h
            .services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["restartCount"], 0);
        assert_eq!(st["status"], "running");
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
    }

    #[tokio::test]
    async fn script_run_service_mode_returns_warning_envelope() {
        let h = harness().await;
        let id = create_simple(&h, "svc", "sleep 5", ScriptMode::Service).await;
        let out = h
            .services
            .script_run(h.ws.clone(), id, None, None)
            .await
            .expect("run service");
        assert_eq!(out["output"], "");
        assert!(
            out["warning"]
                .as_str()
                .unwrap_or("")
                .contains("script.start"),
            "warning directs caller to script.start: {out:?}"
        );
    }

    #[tokio::test]
    async fn script_run_with_timeout_marks_timed_out() {
        let h = harness().await;
        let id = create_simple(&h, "long", "sleep 10", ScriptMode::Command).await;
        let out = h
            .services
            .script_run(h.ws.clone(), id, None, Some(1))
            .await
            .expect("run");
        assert_eq!(out["timedOut"], true);
    }

    #[tokio::test]
    async fn script_run_max_lines_truncates_captured_output() {
        let h = harness().await;
        let id = create_simple(
            &h,
            "many",
            "for i in 1 2 3 4 5 ; do echo line-$i ; done",
            ScriptMode::Command,
        )
        .await;
        let out = h
            .services
            .script_run(h.ws.clone(), id, Some(2), Some(10))
            .await
            .expect("run");
        let text = out["output"].as_str().unwrap_or("");
        assert!(text.contains("line-5"), "output: {text:?}");
        assert!(!text.contains("line-1"), "output: {text:?}");
    }

    #[tokio::test]
    async fn script_output_paginated_returns_items_envelope() {
        let h = harness().await;
        let id = create_simple(&h, "echo", "echo hello-pag", ScriptMode::Command).await;
        h.services
            .script_run(h.ws.clone(), id.clone(), None, Some(10))
            .await
            .expect("run");
        let out = h
            .services
            .script_output(h.ws.clone(), id, Some(50), Some(true), None)
            .await
            .expect("output");
        assert!(out.get("items").is_some(), "envelope shape: {out:?}");
    }

    #[tokio::test]
    async fn script_output_truncated_header_shows_last_n_of_m() {
        let h = harness().await;
        let id = create_simple(
            &h,
            "many",
            "for i in 1 2 3 4 5 6 7 ; do echo line-$i ; done",
            ScriptMode::Command,
        )
        .await;
        h.services
            .script_run(h.ws.clone(), id.clone(), None, Some(10))
            .await
            .expect("run");
        let out = h
            .services
            .script_output(h.ws.clone(), id, Some(2), None, None)
            .await
            .expect("output");
        let text = out.as_str().expect("string");
        assert!(
            text.starts_with("[showing last 2 of "),
            "truncation header: {text:?}"
        );
    }

    #[tokio::test]
    async fn script_remove_kills_running_pty_and_drops_definition() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "svc", "sleep 30", ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, Duration::from_secs(5), |v| {
            v["data"]["status"] == "running"
        })
        .await;
        let res = h
            .services
            .script_remove(h.ws.clone(), id.clone())
            .await
            .expect("remove");
        assert_eq!(res["ok"], true);
        let err = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    /// Two workspaces mint the same client-supplied `scriptId` concurrently
    /// (`"dev"`) — the registry is composite-keyed by `(workspace_id, script_id)`,
    /// so both survive, `script.list` is workspace-partitioned, and
    /// `script.status` / `script.remove` from workspace A never touches workspace
    /// B's script (no cross-workspace takeover).
    #[tokio::test]
    async fn same_script_id_across_workspaces_does_not_collide() {
        let h = harness().await;
        let ws_b = WorkspaceId::new();
        h.services
            .store()
            .insert_workspace(&workspace(&ws_b, None))
            .await
            .expect("ws-b");
        let params = |name: &str| ScriptCreateParams {
            name: name.to_string(),
            command: "echo hi".into(),
            mode: ScriptMode::Command,
            script_id: Some("dev".to_string()),
            ..Default::default()
        };
        h.services
            .script_create(h.ws.clone(), params("dev-a"))
            .await
            .expect("create a");
        h.services
            .script_create(ws_b.clone(), params("dev-b"))
            .await
            .expect("create b");
        // Each list is workspace-partitioned and sees exactly its own `dev`.
        let list_a = h.services.script_list(h.ws.clone()).await.expect("list a");
        let list_b = h.services.script_list(ws_b.clone()).await.expect("list b");
        let names_a: Vec<&str> = list_a["scripts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s["name"].as_str())
            .collect();
        let names_b: Vec<&str> = list_b["scripts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s["name"].as_str())
            .collect();
        assert_eq!(names_a, vec!["dev-a"]);
        assert_eq!(names_b, vec!["dev-b"]);
        // Removing from workspace A leaves workspace B's `dev` intact.
        h.services
            .script_remove(h.ws.clone(), "dev".into())
            .await
            .expect("remove a");
        let list_a = h.services.script_list(h.ws.clone()).await.expect("list a");
        assert!(list_a["scripts"].as_array().unwrap().is_empty());
        let st_b = h
            .services
            .script_status(ws_b.clone(), "dev".into())
            .await
            .expect("status b survives");
        assert_eq!(st_b["status"], "idle");
        // Workspace A can no longer see or mutate workspace B's `dev`.
        let err = h
            .services
            .script_status(h.ws.clone(), "dev".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
        let err = h
            .services
            .script_remove(h.ws.clone(), "dev".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn supervise_records_cwd_escape_error_via_fail_path() {
        let h = harness_with_worktree(true).await;
        let mut sub = subscribe(&h);
        let id = create(
            &h,
            ScriptCreateParams {
                name: "bad-cwd".into(),
                command: "echo never".into(),
                mode: ScriptMode::Service,
                cwd: Some("../escape".into()),
                ..Default::default()
            },
        )
        .await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let ev = await_state(&mut sub, Duration::from_secs(5), |v| {
            v["data"]["status"] == "exited" && v["data"].get("error").is_some()
        })
        .await;
        let err = ev["data"]["error"].as_str().unwrap_or("");
        assert!(
            err.contains("escapes workspace root"),
            "fail() error surfaces cwd escape: {err:?}"
        );
        let st = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .expect("status");
        assert_eq!(st["status"], "exited");
        assert_eq!(st["error"], err);
    }
}
