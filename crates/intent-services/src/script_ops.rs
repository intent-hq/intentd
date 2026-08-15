//! `script.*` reconciled onto the unified `intent-pty` host (§5.8, §12.2).
//!
//! Ports `script-process-manager.ts` so scripts run as real PTYs in the *same*
//! [`PtyHost`] that backs `terminal.*` — there is no separate process-spawning
//! path. Script PTYs are omitted from `terminal.list` because scripts have their
//! own runtime UI, while their scrollback remains addressable by id and through
//! `script.output`. `service` scripts auto-restart per the ported backoff
//! policy; `command` scripts run once. Service output is scanned for a local
//! dev-server URL, surfaced on the `script:state` event for the `forward.*`
//! hook. Live output streams as `script:output` (base64 `chunk`).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use intent_core::events::{SCRIPT_CHANGED, SCRIPT_OUTPUT, SCRIPT_STATE};
use intent_core::{
    now_iso, Error, Result, Script, ScriptCreateParams, ScriptMode, ScriptRuntimeState,
    ScriptStatus, WorkspaceId,
};
use intent_pty::{PtyExit, PtyHost, PtyId, SpawnSpec};
use intent_store::{NewEvent, Store};
use serde_json::{json, Value};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::Mutex as AsyncMutex;

use crate::events::EventBus;
use crate::shell::{default_shell, scrubbed_env_vars_except, shell_args};
use crate::{publish_event, publish_event_transient, system_actor};

/// Delay before an auto-restart attempt (mirrors `AUTO_RESTART_DELAY_MS`).
const AUTO_RESTART_DELAY: Duration = Duration::from_millis(1000);
/// Max consecutive auto-restarts for a service (mirrors `AUTO_RESTART_MAX_RETRIES`).
const AUTO_RESTART_MAX_RETRIES: u32 = 5;
/// A run shorter than this is treated as a config error — do not auto-restart.
/// The production floor; tests can raise it via
/// [`Services::with_script_too_fast_ms`](crate::Services) so the decision is
/// load-independent (monorepo#514).
pub(crate) const TOO_FAST_MS: u128 = 2000;
/// How often the streamer polls for a natural process exit (mirrors `terminal_ops`).
const EXIT_POLL: Duration = Duration::from_millis(25);
/// Backstop on awaiting supervisor settles during shutdown `stop_all`
/// (monorepo#1526): their PTYs are already dead, so they normally settle in
/// milliseconds — this bound only guards a wedged supervisor from stalling
/// daemon shutdown past the FE sidecar's kill grace.
const SHUTDOWN_SETTLE_GRACE: Duration = Duration::from_secs(2);

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
    /// Identity stamp assigned at every registry insertion (monorepo#1194):
    /// a supervisor captures the generation of the entry it was started for,
    /// and every later status write validates it, so a supervisor spawned
    /// against a removed+recreated entry (same key, new generation) can never
    /// latch its PTY or state onto the recreated entry.
    generation: u64,
}

/// Process-wide monotonic counter behind [`ManagedScript::generation`]. Every
/// registry insertion (create, upsert, hydration, bootstrap) takes a fresh
/// value, so two entries under the same key over time are always
/// distinguishable.
static SCRIPT_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Next unique generation stamp for a registry insertion.
fn next_generation() -> u64 {
    SCRIPT_GENERATION.fetch_add(1, Ordering::Relaxed)
}

/// The shared registry of scripts, keyed by `(workspace_id, script_id)` so a
/// client-supplied `scriptId` (`"dev"`, `"build"`, …) can be minted concurrently
/// by any number of workspaces without collision or cross-workspace mutation.
pub(crate) type ScriptRegistry = Arc<Mutex<HashMap<(WorkspaceId, String), ManagedScript>>>;

/// Per-workspace async-mutex map for script bootstrap operations. Prevents
/// concurrent `script.list` calls from creating duplicate repo-config scripts.
/// Modeled after `intent-git::WorktreeLocks`.
#[derive(Clone, Default)]
pub(crate) struct WorkspaceScriptLocks {
    locks: Arc<Mutex<HashMap<WorkspaceId, Arc<AsyncMutex<()>>>>>,
}

impl WorkspaceScriptLocks {
    /// Create an empty lock registry.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Resolve (or create) the lock for a workspace.
    fn lock_for(&self, workspace_id: &WorkspaceId) -> Arc<AsyncMutex<()>> {
        let mut map = self.locks.lock().expect("script lock map poisoned");
        map.entry(workspace_id.clone()).or_default().clone()
    }

    /// Run `f` while holding the per-workspace script lock.
    pub(crate) async fn with_lock<F, Fut, T>(&self, workspace_id: &WorkspaceId, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let lock = self.lock_for(workspace_id);
        let _guard = lock.lock().await;
        f().await
    }
}

/// Thin service over the unified host: holds the shared PTY host, the event bus,
/// the store (for workspace-root resolution), the script registry, and bootstrap locks.
/// Cheap to clone (all handles); the supervisor task owns its own clone.
#[derive(Clone)]
pub(crate) struct ScriptManager {
    pty: Arc<PtyHost>,
    bus: Option<EventBus>,
    store: Store,
    scripts: ScriptRegistry,
    bootstrap_locks: WorkspaceScriptLocks,
    /// The too-fast-exit floor in milliseconds ([`TOO_FAST_MS`] in production;
    /// tests inject a larger floor so the decision is load-independent).
    too_fast_ms: u128,
    /// Test park seams for the `script.*` race windows; all `None` in
    /// production wiring.
    parks: ScriptParks,
}

/// Test seam for a race window: lets a test hold a task inside a window
/// (signal `entered`, await `release`). Used for `supervise()`'s
/// pre-registration window (monorepo#1180) and `start()`'s
/// spawn-to-registration window (monorepo#1194).
#[derive(Default)]
pub(crate) struct SupervisePark {
    /// Signaled by the parked task on entering the window.
    pub(crate) entered: tokio::sync::Notify,
    /// Held by the parked task inside the window until the test releases it.
    pub(crate) release: tokio::sync::Notify,
}

/// The set of `script.*` test park seams (all `None` in production wiring),
/// grouped so the manager constructor stays within arity limits.
#[derive(Clone, Default)]
pub(crate) struct ScriptParks {
    /// Parks `supervise()` in its pre-registration window — after
    /// `pty.spawn`, before `mark_running` records the id (monorepo#1180).
    pub(crate) supervise: Option<Arc<SupervisePark>>,
    /// Parks `start()` between spawning the supervisor task and taking the
    /// registration lock (monorepo#1194).
    pub(crate) start_registration: Option<Arc<SupervisePark>>,
}

/// Cancellation guard for the `script.run` reservation window (reserve →
/// `resolve_cwd` → `pty.spawn`): if the caller drops the `run()` future while
/// armed — before a PTY exists and the detached completion task takes over —
/// restore the pre-reservation status so the script is not stuck `running`
/// forever. No `script:state` was emitted for the reservation, so the restore
/// is silent.
struct RunReservation {
    mgr: ScriptManager,
    key: (WorkspaceId, String),
    prev: ScriptStatus,
    generation: u64,
    armed: bool,
}

impl Drop for RunReservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Best-effort restore: a poisoned registry mutex must not abort the
        // process by panicking inside a drop during unwinding. A recreated
        // entry (new generation) is never touched — its state is not ours.
        if let Ok(mut guard) = self.mgr.scripts.lock() {
            if let Some(m) = guard.get_mut(&self.key) {
                if m.generation == self.generation && m.state.status == ScriptStatus::Running {
                    m.state.status = self.prev;
                }
            }
        }
    }
}

impl ScriptManager {
    /// Wire the manager over the shared host/bus/store/registry/bootstrap-locks.
    pub(crate) fn new(
        pty: Arc<PtyHost>,
        bus: Option<EventBus>,
        store: Store,
        scripts: ScriptRegistry,
        bootstrap_locks: WorkspaceScriptLocks,
        too_fast_ms: u128,
        parks: ScriptParks,
    ) -> Self {
        Self {
            pty,
            bus,
            store,
            scripts,
            bootstrap_locks,
            too_fast_ms,
            parks,
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
        let change = if existing.is_some() {
            "updated"
        } else {
            "created"
        };
        if let Some(mut old) = existing {
            // Cooperative teardown (monorepo#1180): kill the recorded PTY,
            // then *await* the supervisor instead of aborting it. An abort
            // could land in the pre-registration window (after `pty.spawn`,
            // before `mark_running` records the id) and orphan the fresh PTY;
            // awaited, the supervisor sees the entry gone and reaps it itself.
            let handle = old.supervisor.take();
            if let Some(pty_id) = old.pty_id {
                self.pty.kill(pty_id).await;
            }
            if let Some(handle) = handle {
                if let Err(e) = handle.await {
                    tracing::warn!(script = %id, error = %e, "script supervisor join failed during upsert teardown");
                }
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
                generation: next_generation(),
            },
        );
        publish_event(
            &self.bus,
            script_event(
                &workspace_id,
                SCRIPT_CHANGED,
                json!({ "scriptId": def.id.clone(), "action": change }),
            ),
        )
        .await;
        Ok(serde_json::to_value(def).unwrap_or_else(|_| json!({})))
    }

    /// Boot-time hydration: load every persisted definition into the runtime
    /// registry with a fresh idle state (runtime state is never persisted,
    /// except the stored-on-write `was_running` marker, surfaced here as
    /// `previouslyRunning: true` — the script was running when the previous
    /// daemon process died). Ids already registered are left untouched.
    /// Returns the number loaded.
    pub(crate) async fn hydrate(&self) -> Result<usize> {
        let defs = self.store.list_all_scripts().await?;
        let was_running: HashSet<(String, String)> = self
            .store
            .list_was_running_script_ids()
            .await?
            .into_iter()
            .collect();
        let mut guard = self.scripts.lock().unwrap();
        let mut loaded = 0;
        for def in defs {
            let key = (WorkspaceId::from(def.workspace_id.as_str()), def.id.clone());
            guard.entry(key).or_insert_with(|| {
                loaded += 1;
                let state = ScriptRuntimeState {
                    previously_running: was_running
                        .contains(&(def.workspace_id.clone(), def.id.clone()))
                        .then_some(true),
                    ..Default::default()
                };
                ManagedScript {
                    def,
                    state,
                    pty_id: None,
                    stopped_by_user: false,
                    supervisor: None,
                    generation: next_generation(),
                }
            });
        }
        Ok(loaded)
    }

    /// `script.list`: the workspace's scripts with merged runtime state.
    /// When empty, bootstrap from repo config `scripts[]` (FE parity:
    /// scripts.ipc.ts L291-320).
    pub(crate) async fn list(&self, workspace_id: &WorkspaceId) -> Result<Value> {
        // First check: read existing scripts (fast path, no lock)
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

        // Bootstrap from repo config if workspace has no scripts.
        // Use a per-workspace async lock to prevent concurrent bootstrap attempts
        // from creating duplicate script rows (modeled after intent-git::WorktreeLocks).
        self.bootstrap_locks
            .with_lock(workspace_id, || async {
                // Re-check after acquiring the lock — another caller may have bootstrapped
                {
                    let guard = self.scripts.lock().unwrap();
                    let scripts_exist = guard.iter().any(|((ws, _), _)| ws == workspace_id);
                    if scripts_exist {
                        // Already bootstrapped by a concurrent caller
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
                } // guard dropped here

                // Now safe to bootstrap
                if let Ok(ws) = self.store.get_workspace(workspace_id).await {
                    if let Some(repo_path) = ws
                        .repository_path
                        .as_deref()
                        .filter(|p| !p.is_empty())
                        .map(PathBuf::from)
                    {
                        let repo_config = crate::repo_config::read_repo_config(&repo_path).await;
                        if let Some(repo_scripts) = repo_config.scripts {
                            let now = now_iso();
                            let scripts: Vec<Script> = repo_scripts
                                .into_iter()
                                .map(|repo_script| Script {
                                    id: uuid::Uuid::new_v4().to_string(),
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
                                            intent_core::RepoScriptCategory::Typecheck => {
                                                "typecheck"
                                            }
                                            intent_core::RepoScriptCategory::Format => "format",
                                            intent_core::RepoScriptCategory::Storybook => {
                                                "storybook"
                                            }
                                            intent_core::RepoScriptCategory::Other => "other",
                                        }
                                        .to_string()
                                    }),
                                    source: "user".to_string(),
                                    auto_start: repo_script.auto_start,
                                    created_at: now.clone(),
                                    updated_at: None,
                                })
                                .collect();
                            // Persist in one batched upsert — one INSERT per
                            // script here tripped the per-dispatch statement
                            // budget (intent-hq/monorepo#1778) — then register.
                            self.store.upsert_scripts(&scripts).await?;
                            {
                                let mut guard = self.scripts.lock().unwrap();
                                for script in scripts {
                                    guard.insert(
                                        (workspace_id.clone(), script.id.clone()),
                                        ManagedScript {
                                            def: script,
                                            state: ScriptRuntimeState::default(),
                                            pty_id: None,
                                            stopped_by_user: false,
                                            supervisor: None,
                                            generation: next_generation(),
                                        },
                                    );
                                }
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

                // If we get here, workspace was empty and no repo config scripts found
                Ok(json!({ "scripts": [] }))
            })
            .await
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
        // Cooperative teardown (monorepo#1180): kill the recorded PTY, then
        // *await* the supervisor instead of aborting it. An abort could land
        // in the pre-registration window (after `pty.spawn`, before
        // `mark_running` records the id) and orphan the fresh PTY; awaited,
        // the supervisor sees the entry gone and reaps it itself. No lock is
        // held across these awaits (the entry was already taken above).
        let handle = managed.supervisor.take();
        if let Some(pty_id) = managed.pty_id {
            self.pty.kill(pty_id).await;
        }
        if let Some(handle) = handle {
            if let Err(e) = handle.await {
                tracing::warn!(script = %script_id, error = %e, "script supervisor join failed during remove teardown");
            }
        }
        self.store.remove_script(script_id).await?;
        publish_event(
            &self.bus,
            script_event(
                workspace_id,
                SCRIPT_CHANGED,
                json!({ "scriptId": script_id, "action": "removed" }),
            ),
        )
        .await;
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
        let (def, generation) = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(&key)
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            if m.state.status == ScriptStatus::Running {
                return Ok(json!({ "ok": true, "scriptId": script_id }));
            }
            m.stopped_by_user = false;
            (m.def.clone(), m.generation)
        };
        let mgr = self.clone();
        let ws = workspace_id.clone();
        let sid = script_id.to_string();
        let handle = tokio::spawn(async move { mgr.supervise(ws, sid, def, generation).await });
        // Test seam (monorepo#1194): park here so a test can remove+recreate
        // the entry while the supervisor task exists but is not yet
        // registered.
        if let Some(park) = &self.parks.start_registration {
            park.entered.notify_one();
            park.release.notified().await;
        }
        let orphan = {
            let mut guard = self.scripts.lock().unwrap();
            match guard.get_mut(&key) {
                // Only install the handle if the entry is still the same
                // incarnation the supervisor was spawned for (monorepo#1194):
                // a removed+recreated entry has a new generation, and the
                // stale supervisor must not become its supervisor.
                Some(m) if m.generation == generation => {
                    m.supervisor = Some(handle);
                    None
                }
                _ => Some(handle),
            }
        };
        // Removed (or recreated) concurrently between spawn and registration:
        // *await* the supervisor (outside the lock) instead of aborting it
        // (monorepo#1180) — `mark_running` fails the generation check and the
        // supervisor reaps any PTY it spawned before returning.
        if let Some(handle) = orphan {
            if let Err(e) = handle.await {
                tracing::warn!(script = %script_id, error = %e, "script supervisor join failed during orphan teardown");
            }
        }
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// `script.stop`: flag user-stop, kill the PTY (cancelling auto-restart), and
    /// await the supervisor's teardown. Scoped to `workspace_id`.
    ///
    /// A stop on a non-running script that still carries the hydrated
    /// `previouslyRunning` marker is the FE dismiss affordance: it durably
    /// clears the `was_running` marker, publishes the cleared state as
    /// `script:state` so subscribers drop the marker too, and returns ok (a
    /// stopped script is exactly the requested state, not an error).
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
            let dismissed_state = {
                let mut guard = self.scripts.lock().unwrap();
                match guard.get_mut(&key) {
                    Some(m) => {
                        if m.state.status != ScriptStatus::Running {
                            m.state.status = ScriptStatus::Idle;
                        }
                        m.state.previously_running.take().map(|_| m.state.clone())
                    }
                    None => None,
                }
            };
            if let Some(state) = dismissed_state {
                self.persist_was_running(workspace_id, script_id, false)
                    .await;
                self.emit_state(workspace_id, script_id, &state).await;
            }
        }
        Ok(json!({ "ok": true, "scriptId": script_id }))
    }

    /// Clean daemon shutdown (monorepo#1526): stop every managed script across
    /// all workspaces with the same user-stop semantics as `script.stop` (so
    /// no auto-restart supervisor respawns a PTY while the daemon tears down),
    /// then kill every PTY the host still tracks — scripts and terminals alike
    /// — in one concurrent group-kill sweep. Returns
    /// `(scripts_stopped, ptys_killed)`.
    ///
    /// Three phases, bounded by a single SIGTERM grace overall:
    /// 1. Flag every entry `stopped_by_user` and take the supervisor handles
    ///    under one lock acquisition, before any PTY dies — no await points,
    ///    so no supervisor can observe a half-flagged registry.
    /// 2. `PtyHost::kill_all` reaps every tracked session concurrently (one
    ///    TERM grace wall-clock) and latches the host closed, so a spawn
    ///    racing the sweep is refused and reaped in place.
    /// 3. Await the taken supervisor handles — their PTYs are already dead
    ///    (or their respawn registration refuses on the stop flag), so they
    ///    settle promptly; the await is time-bounded as a backstop so a
    ///    wedged supervisor can never stall daemon shutdown.
    ///
    /// The settled supervisors persisted `was_running = false` on their way
    /// out (`mark_exited`), but a *graceful* daemon shutdown should leave the
    /// same relaunch affordance as a daemon death (monorepo#932): the marker
    /// is re-persisted for every service that was running when the sweep
    /// began, so the FE can offer to resurrect it on next boot.
    pub(crate) async fn stop_all(&self) -> (usize, usize) {
        struct Stopped {
            ws: WorkspaceId,
            id: String,
            handle: Option<tokio::task::JoinHandle<()>>,
            running_service: bool,
        }
        let victims: Vec<Stopped> = {
            let mut guard = self.scripts.lock().unwrap();
            guard
                .iter_mut()
                .map(|((ws, id), m)| {
                    m.stopped_by_user = true;
                    Stopped {
                        ws: ws.clone(),
                        id: id.clone(),
                        handle: m.supervisor.take(),
                        running_service: m.def.mode == ScriptMode::Service
                            && m.state.status == ScriptStatus::Running,
                    }
                })
                .filter(|s| s.handle.is_some())
                .collect()
        };
        let scripts = victims.len();
        let ptys = self.pty.kill_all().await;
        let mut settles = tokio::task::JoinSet::new();
        let mut markers = Vec::new();
        for v in victims {
            if v.running_service {
                markers.push((v.ws, v.id.clone()));
            }
            if let Some(handle) = v.handle {
                let id = v.id;
                settles.spawn(async move {
                    if let Err(e) = handle.await {
                        tracing::warn!(script = %id, error = %e, "script supervisor join failed during shutdown stop-all");
                    }
                });
            }
        }
        let drain = async {
            while let Some(res) = settles.join_next().await {
                if let Err(e) = res {
                    tracing::warn!(error = %e, "shutdown stop-all settle task failed");
                }
            }
        };
        if tokio::time::timeout(SHUTDOWN_SETTLE_GRACE, drain)
            .await
            .is_err()
        {
            tracing::warn!(
                "shutdown stop-all: supervisors still settling after {SHUTDOWN_SETTLE_GRACE:?}; proceeding"
            );
        }
        for (ws, id) in markers {
            self.persist_was_running(&ws, &id, true).await;
        }
        (scripts, ptys)
    }

    /// `script.restart`: stop, reset the restart counter, then start. Scoped to
    /// `workspace_id`. The stop→start gap is surfaced as `restarting`
    /// (monorepo#1318) so a snapshot taken mid-restart never reports
    /// `exited`/`idle`; `mark_running` flips it to `running` once the respawn
    /// is up.
    pub(crate) async fn restart(
        &self,
        workspace_id: &WorkspaceId,
        script_id: &str,
    ) -> Result<Value> {
        self.stop(workspace_id, script_id).await?;
        let state = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(&(workspace_id.clone(), script_id.to_string()))
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            m.state.restart_count = 0;
            m.stopped_by_user = false;
            m.state.status = ScriptStatus::Restarting;
            m.state.clone()
        };
        self.emit_state(workspace_id, script_id, &state).await;
        self.start(workspace_id, script_id).await
    }

    /// `script.run`: run a command-mode script to completion (optional timeout),
    /// returning its captured output + exit code; service scripts return a
    /// `warning` directing callers to `script.start`, and a script already
    /// running warn-and-returns (mirrors `start()`'s guard) so a second run
    /// can never overwrite `pty_id` and orphan the first run's PTY
    /// (monorepo#1155). The `running` status is reserved under the same lock
    /// acquisition as the guard check, so two concurrent entries cannot both
    /// pass the guard during the pre-`mark_running` window (`resolve_cwd`
    /// awaits in between). Scoped to `workspace_id`.
    pub(crate) async fn run(
        &self,
        workspace_id: &WorkspaceId,
        script_id: &str,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> Result<Value> {
        let (def, prev_status, generation) = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(&(workspace_id.clone(), script_id.to_string()))
                .ok_or_else(|| Error::NotFound(format!("script {script_id}")))?;
            if m.def.mode == ScriptMode::Service {
                return Ok(json!({
                    "output": "",
                    "warning": "Script is a service; use script.start instead of script.run.",
                }));
            }
            if m.state.status == ScriptStatus::Running {
                return Ok(json!({
                    "output": "",
                    "warning": "Script is already running; wait for it to finish or use script.stop.",
                }));
            }
            // Reserve the run before releasing the lock: a concurrent `run()`
            // entering during the awaits below must hit the guard above.
            // `mark_running` fills in pid/started_at once the PTY exists; a
            // resolve/spawn failure resets via `fail()`, and a caller-side
            // cancellation before the PTY exists restores the prior status
            // via `reservation` (nothing to clean up yet, no event emitted).
            // The previous run's `pid` is cleared here so the window never
            // reports `running` with a stale pid; `script.stop` inside the
            // window is a benign no-op (nothing spawned yet, one store read
            // wide) and the run proceeds to its own timeout/exit.
            let prev = m.state.status;
            m.state.status = ScriptStatus::Running;
            m.state.pid = None;
            (m.def.clone(), prev, m.generation)
        };
        let mut reservation = RunReservation {
            mgr: self.clone(),
            key: (workspace_id.clone(), script_id.to_string()),
            prev: prev_status,
            generation,
            armed: true,
        };
        let ws = workspace_id.clone();
        let cwd = match self.resolve_cwd(&ws, &def).await {
            Ok(cwd) => cwd,
            Err(e) => {
                reservation.armed = false;
                self.fail(&ws, script_id, generation, &e.to_string()).await;
                return Err(e);
            }
        };
        let pty_id = match self.pty.spawn(self.build_spec(&ws, &def, &cwd)) {
            Ok(id) => id,
            Err(e) => {
                reservation.armed = false;
                self.fail(&ws, script_id, generation, &e.to_string()).await;
                return Err(e);
            }
        };
        // The completion path (mark-running → stream → timeout kill →
        // `mark_exited`) runs on a detached task spawned with no await point
        // after `pty.spawn` (modeled on `host_exec_stream::run_wait_loop`), so
        // dropping this future — e.g. an eval-level timeout cancelling the
        // RPC — cannot orphan the PTY or skip the `script:state` teardown
        // (monorepo#1155).
        let mgr = self.clone();
        let ws_task = ws.clone();
        let sid = script_id.to_string();
        reservation.armed = false;
        let completion = tokio::spawn(async move {
            // Removed or recreated concurrently (script.remove /
            // create-upsert) between the reservation and here: the entry is
            // gone or carries a new generation, so reap the fresh PTY
            // ourselves — mirrors `supervise()`.
            if !mgr
                .mark_running(&ws_task, &sid, generation, pty_id, false)
                .await
            {
                mgr.pty.kill(pty_id).await;
                return (None, false);
            }
            let timed_out = match timeout_seconds.filter(|s| *s > 0) {
                Some(s) => {
                    let fut = mgr.run_one(&ws_task, &sid, pty_id, false);
                    match tokio::time::timeout(Duration::from_secs(s as u64), fut).await {
                        Ok(_) => false,
                        Err(_) => {
                            mgr.pty.kill(pty_id).await;
                            true
                        }
                    }
                }
                None => {
                    mgr.run_one(&ws_task, &sid, pty_id, false).await;
                    false
                }
            };
            // Group-keyed liveness (monorepo#1300): before the
            // exit is recorded, reap group members that outlived the shell
            // (a descendant trapping TERM+HUP) so `exited` means the whole
            // group is gone. No-op after the timeout kill (session gone) or
            // when the group is already empty.
            mgr.pty.reap_group_stragglers(pty_id).await;
            let exit = mgr.pty.try_exit(pty_id).ok().flatten();
            mgr.mark_exited(&ws_task, &sid, generation, exit.clone())
                .await;
            (exit, timed_out)
        });
        let (exit, timed_out) = completion
            .await
            .map_err(|e| Error::Internal(format!("script.run completion task failed: {e}")))?;
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
    /// the composite `(workspace_id, script_id)` key. `generation` is the stamp
    /// of the entry this supervisor was started for; every status write
    /// validates it so a stale supervisor can never mutate a recreated entry
    /// (monorepo#1194).
    async fn supervise(self, ws: WorkspaceId, script_id: String, def: Script, generation: u64) {
        let cwd = match self.resolve_cwd(&ws, &def).await {
            Ok(c) => c,
            Err(e) => {
                self.fail(&ws, &script_id, generation, &e.to_string()).await;
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
                    self.fail(&ws, &script_id, generation, &e.to_string()).await;
                    return;
                }
            };
            prev = Some(pty_id);
            // Test seam (monorepo#1180): park here so a test can drive a
            // concurrent teardown while the fresh PTY is not yet recorded.
            if let Some(park) = &self.parks.supervise {
                park.entered.notify_one();
                park.release.notified().await;
            }
            let started = Instant::now();
            // `refuse_if_stopped`: a stop/stop-all that flagged the script
            // between the loop's stopped check and this registration must not
            // have its flag outrun by the respawn (monorepo#1526) — the
            // refusal lands here and the fresh PTY is reaped below.
            if !self
                .mark_running(&ws, &script_id, generation, pty_id, true)
                .await
            {
                self.pty.kill(pty_id).await;
                return;
            }
            let exit = self.run_one(&ws, &script_id, pty_id, detect).await;
            // The too-fast decision is based on the shell's actual runtime:
            // capture it before the straggler reap below, whose TERM-grace
            // wait must not inflate a genuinely quick exit past the floor.
            let ran_for = started.elapsed();
            // Group-keyed liveness (monorepo#1300): reap group
            // members that outlived the shell (a descendant trapping
            // TERM+HUP) before the exit is recorded, so `exited` means the
            // whole group is gone — the script can never sit `running` (or
            // flip to `exited`) while trapped survivors linger.
            self.pty.reap_group_stragglers(pty_id).await;
            let (stopped_by_user, restart_count) =
                match self.mark_exited(&ws, &script_id, generation, exit).await {
                    Some(v) => v,
                    None => return,
                };
            if stopped_by_user || def.mode != ScriptMode::Service {
                break;
            }
            if ran_for.as_millis() < self.too_fast_ms {
                let ms = ran_for.as_millis();
                self.emit_separator(
                    &ws,
                    &script_id,
                    &format!(
                        "Exited too quickly ({ms}ms) — not restarting. Check your configuration."
                    ),
                );
                break;
            }
            if restart_count >= AUTO_RESTART_MAX_RETRIES {
                break;
            }
            let key = (ws.clone(), script_id.clone());
            // The restart is committed (service mode, not user-stopped, not
            // too-fast, retries left): surface the backoff window as
            // `restarting` (monorepo#1318) so clients can distinguish it from
            // a final exit. The generation filter keeps a retired supervisor
            // from overwriting a successor's status; `mark_running` flips the
            // status back to `running` after the respawn.
            let (attempt, state) = {
                let mut guard = self.scripts.lock().unwrap();
                let Some(m) = guard.get_mut(&key).filter(|m| m.generation == generation) else {
                    return;
                };
                m.state.restart_count += 1;
                m.state.status = ScriptStatus::Restarting;
                (m.state.restart_count, m.state.clone())
            };
            self.emit_state(&ws, &script_id, &state).await;
            tokio::time::sleep(AUTO_RESTART_DELAY).await;
            {
                let guard = self.scripts.lock().unwrap();
                match guard.get(&key).filter(|m| m.generation == generation) {
                    Some(m) if m.stopped_by_user => break,
                    Some(_) => {}
                    None => return,
                }
            }
            self.emit_separator(
                &ws,
                &script_id,
                &format!("Restarting (attempt {attempt}/{AUTO_RESTART_MAX_RETRIES})"),
            );
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
            self.emit_output(ws, script_id, &attachment.backlog);
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
                        self.emit_output(ws, script_id, &chunk);
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
                                    self.emit_output(ws, script_id, &chunk);
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

    /// Flip a script to `running` and emit `script:state`. Returns `false` if
    /// the script was removed — or removed and recreated under a new
    /// generation (monorepo#1194) — concurrently (caller should reap the PTY).
    ///
    /// With `refuse_if_stopped` (the supervisor's respawn path), a
    /// `stopped_by_user` flag set concurrently — a `stop`/`stop_all` landing
    /// between the supervisor's post-backoff stopped check and this
    /// registration (monorepo#1526) — also refuses: the stop keyed its PTY
    /// kill on the *previous* pty_id, so letting this registration through
    /// would leave the fresh PTY running unstopped. `run()`'s completion path
    /// keeps `false`: its reservation flow owns the stop interaction.
    ///
    /// Stored-on-write: a service-mode start durably sets the `was_running`
    /// marker (and drops any hydrated `previouslyRunning`), so a daemon that
    /// dies while the service runs hydrates it as previously running.
    /// Command-mode scripts never set the marker.
    async fn mark_running(
        &self,
        ws: &WorkspaceId,
        script_id: &str,
        generation: u64,
        pty_id: PtyId,
        refuse_if_stopped: bool,
    ) -> bool {
        let pid = self.pty.pid(pty_id);
        let (state, is_service) = {
            let mut guard = self.scripts.lock().unwrap();
            let Some(m) = guard
                .get_mut(&(ws.clone(), script_id.to_string()))
                .filter(|m| m.generation == generation)
                .filter(|m| !(refuse_if_stopped && m.stopped_by_user))
            else {
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
            m.state.previously_running = None;
            (m.state.clone(), m.def.mode == ScriptMode::Service)
        };
        if is_service {
            self.persist_was_running(ws, script_id, true).await;
        }
        self.emit_state(ws, script_id, &state).await;
        true
    }

    /// Flip a script to `exited`, record the exit code, and emit `script:state`.
    /// Returns `(stopped_by_user, restart_count)` for the restart decision;
    /// `None` when the entry is gone or recreated under a new generation
    /// (the stale writer must not touch it — monorepo#1194).
    ///
    /// Stored-on-write: a service-mode exit (user stop or natural) durably
    /// clears the `was_running` marker — the process is gone, so a daemon
    /// death from here on must not resurrect the tab. An auto-restart's
    /// respawn re-sets it via `mark_running`.
    async fn mark_exited(
        &self,
        ws: &WorkspaceId,
        script_id: &str,
        generation: u64,
        exit: Option<PtyExit>,
    ) -> Option<(bool, u32)> {
        let (state, flags, is_service) = {
            let mut guard = self.scripts.lock().unwrap();
            let m = guard
                .get_mut(&(ws.clone(), script_id.to_string()))
                .filter(|m| m.generation == generation)?;
            m.state.status = ScriptStatus::Exited;
            m.state.exit_code = exit.as_ref().map(|e| e.exit_code as i64);
            m.state.stopped_at = Some(now_iso());
            (
                m.state.clone(),
                (m.stopped_by_user, m.state.restart_count),
                m.def.mode == ScriptMode::Service,
            )
        };
        if is_service {
            self.persist_was_running(ws, script_id, false).await;
        }
        self.emit_state(ws, script_id, &state).await;
        Some(flags)
    }

    /// Best-effort stored-on-write update of the `was_running` marker: a
    /// store failure is logged, never propagated — the runtime transition
    /// (and its `script:state` event) must not fail over a bookkeeping write.
    async fn persist_was_running(&self, ws: &WorkspaceId, script_id: &str, was_running: bool) {
        if let Err(e) = self
            .store
            .set_script_was_running(ws.as_str(), script_id, was_running)
            .await
        {
            tracing::warn!(script = %script_id, error = %e, "persist script was_running marker failed");
        }
    }

    /// Record a spawn/cwd failure on the runtime state and emit `script:state`.
    /// A gone or recreated entry (generation mismatch) is left untouched.
    async fn fail(&self, ws: &WorkspaceId, script_id: &str, generation: u64, err: &str) {
        let state = {
            let mut guard = self.scripts.lock().unwrap();
            let Some(m) = guard
                .get_mut(&(ws.clone(), script_id.to_string()))
                .filter(|m| m.generation == generation)
            else {
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

    /// Broadcast a `script:output` event carrying a base64 output `chunk`.
    ///
    /// Transient (broadcast-only, never persisted — same path as
    /// `chat:stream:delta` / `terminal:data`): script PTY output is
    /// high-volume and must not serialize behind a durable SQLite commit per
    /// chunk. Scrollback replay reads the PTY host ring buffer via
    /// `script.output`, so nothing consumes persisted `script:output` rows.
    /// All durable `script:state` transitions are emitted on the same
    /// supervisor task that broadcasts output, so state never overtakes
    /// previously-broadcast chunks; the `exited` transition in particular is
    /// emitted only after `run_one` has broadcast every chunk.
    fn emit_output(&self, ws: &WorkspaceId, script_id: &str, bytes: &[u8]) {
        let chunk = base64::engine::general_purpose::STANDARD.encode(bytes);
        publish_event_transient(
            &self.bus,
            script_event(
                ws,
                SCRIPT_OUTPUT,
                json!({ "scriptId": script_id, "chunk": chunk }),
            ),
        );
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
    fn emit_separator(&self, ws: &WorkspaceId, script_id: &str, message: &str) {
        let line = format!("\r\n--- {message} ---\r\n");
        self.emit_output(ws, script_id, line.as_bytes());
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
    /// with an inherited `npm_config_prefix` scrubbed so nvm's login-shell init
    /// succeeds. An explicit script env value is preserved.
    fn build_spec(&self, ws: &WorkspaceId, def: &Script, cwd: &Option<PathBuf>) -> SpawnSpec {
        let shell = default_shell();
        let mut spec = SpawnSpec::new(ws.as_str(), shell.clone());
        spec.args = shell_args(&shell, &def.command);
        spec.cwd = cwd.clone();
        spec.env = spawn_env_overlay(def.env.as_ref());
        spec.env_remove = scrubbed_env_vars_except(&spec.env);
        spec.listed = false;
        spec
    }
}

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
    use intent_store::{EventQuery, Store};
    use serde_json::{json, Value};

    use super::*;
    use crate::events::{EventBus, Subscription, SubscriptionFilter};
    use crate::Services;

    /// Pure-liveness deadline for event-driven waits (monorepo#515): the waits
    /// below return as soon as the awaited event arrives, so this bound only
    /// has to outlast a worst-case multi-suite machine stall (login-shell
    /// spawn + exit-poll + bus delivery), never a passing run.
    const LIVENESS: Duration = Duration::from_secs(300);
    /// Service command lifetime long enough that a service under test cannot
    /// exit (and auto-restart, killing its PTY) mid-assertion under load
    /// (monorepo#515). Strictly outlives `LIVENESS` so negative checks bounded
    /// by it (e.g. the upsert orphan `kill -0` poll) can still hard-fail on a
    /// leaked process instead of the command exiting first. Every test that
    /// starts one stops or removes it.
    const SERVICE_CMD: &str = "sleep 3600";

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
    fn spawn_overlay_scrubs_only_inherited_npm_config_prefix_and_enhances_path() {
        let env = spawn_env_overlay(None);
        assert!(scrubbed_env_vars_except(&env)
            .iter()
            .any(|name| name == "npm_config_prefix"));
        assert!(!env.iter().any(|(k, _)| k == "npm_config_prefix"));

        let mut def_env = std::collections::BTreeMap::new();
        def_env.insert("npm_config_prefix".to_string(), "/custom".to_string());
        let explicit_env = spawn_env_overlay(Some(&def_env));
        assert!(scrubbed_env_vars_except(&explicit_env).is_empty());

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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
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

    async fn await_script_change(sub: &mut Subscription, action: &str) -> Value {
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let batch = tokio::time::timeout(remaining, sub.recv())
                .await
                .expect("script change delivered before liveness deadline")
                .expect("subscription open");
            for ev in &batch {
                let value = serde_json::to_value(ev).expect("serialize");
                if value["type"] == "script:changed" && value["data"]["action"] == action {
                    return value;
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
        let mut sub = subscribe(&h);
        let err = h
            .services
            .script_remove(h.ws.clone(), "nope".into())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
        let live = tokio::time::timeout(Duration::from_millis(100), sub.recv()).await;
        assert!(
            live.is_err(),
            "failed remove must not emit an event: {live:?}"
        );
        let durable = h
            .services
            .store()
            .query_events(&EventQuery {
                workspace_id: Some(h.ws.clone()),
                event_types: vec![SCRIPT_CHANGED.to_string()],
                ..Default::default()
            })
            .await
            .expect("query script changes");
        assert!(
            durable.is_empty(),
            "failed remove must not persist an event"
        );
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
    async fn script_definition_mutations_emit_changed_events() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "dev", "pnpm dev", ScriptMode::Service).await;
        let created = await_script_change(&mut sub, "created").await;
        assert_eq!(created["workspaceId"], h.ws.as_str());
        assert_eq!(created["data"]["scriptId"], id);
        assert_eq!(created["actor"]["id"], "system");
        assert!(created["timestamp"]
            .as_str()
            .is_some_and(|ts| !ts.is_empty()));

        create(
            &h,
            ScriptCreateParams {
                name: "dev".into(),
                command: "pnpm dev --host".into(),
                mode: ScriptMode::Service,
                script_id: Some(id.clone()),
                ..Default::default()
            },
        )
        .await;
        let updated = await_script_change(&mut sub, "updated").await;
        assert_eq!(updated["data"]["scriptId"], id);

        h.services
            .script_remove(h.ws.clone(), id.clone())
            .await
            .expect("remove");
        let removed = await_script_change(&mut sub, "removed").await;
        assert_eq!(removed["data"]["scriptId"], id);

        let durable = h
            .services
            .store()
            .query_events(&EventQuery {
                workspace_id: Some(h.ws.clone()),
                event_types: vec![SCRIPT_CHANGED.to_string()],
                ..Default::default()
            })
            .await
            .expect("query script changes");
        assert_eq!(durable.len(), 3, "one event per successful mutation");
        let mut actions: Vec<&str> = durable
            .iter()
            .filter_map(|event| event.data["action"].as_str())
            .collect();
        actions.sort_unstable();
        assert_eq!(actions, vec!["created", "removed", "updated"]);
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
        assert!(
            entry["runtime"].get("previouslyRunning").is_none(),
            "never-started script hydrates without the marker: {entry}"
        );

        // Hydration is idempotent — already-registered ids are untouched.
        assert_eq!(svc2.hydrate_scripts().await.expect("re-hydrate"), 0);
    }

    /// A service running when the daemon dies hydrates as `idle` with
    /// `previouslyRunning: true` (the stored-on-write `was_running` marker),
    /// and the marker persists across repeated restarts until the script is
    /// stopped.
    #[tokio::test]
    async fn service_running_at_daemon_death_hydrates_previously_running() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "svc", SERVICE_CMD, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;

        // Simulate a daemon death (no stop ran): a fresh Services over the
        // same store hydrates the marker as `previouslyRunning: true`.
        let store = h.services.store().clone();
        let svc2 = Services::new(store.clone());
        assert_eq!(svc2.hydrate_scripts().await.expect("hydrate"), 1);
        let st = svc2
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["status"], "idle");
        assert_eq!(st["previouslyRunning"], true, "marker surfaced: {st}");

        // The marker survives another restart untouched.
        let svc3 = Services::new(store.clone());
        assert_eq!(svc3.hydrate_scripts().await.expect("hydrate"), 1);
        let st = svc3
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["previouslyRunning"], true, "marker persists: {st}");

        // A real stop (running process) durably clears it.
        h.services
            .script_stop(h.ws.clone(), id.clone())
            .await
            .expect("stop");
        assert!(
            store
                .list_was_running_script_ids()
                .await
                .expect("list")
                .is_empty(),
            "stop clears the marker"
        );
        let svc4 = Services::new(store.clone());
        assert_eq!(svc4.hydrate_scripts().await.expect("hydrate"), 1);
        let st = svc4.script_status(h.ws.clone(), id).await.expect("status");
        assert!(
            st.get("previouslyRunning").is_none(),
            "post-stop hydration carries no marker: {st}"
        );
    }

    /// `script.stop` on a hydrated non-running script that carries the marker
    /// is the FE dismiss affordance: it returns ok, drops `previouslyRunning`
    /// from the runtime state, publishes the cleared state as `script:state`,
    /// and durably clears the persisted marker.
    #[tokio::test]
    async fn script_stop_dismisses_previously_running_marker() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "svc", SERVICE_CMD, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;

        let store = h.services.store().clone();
        let bus2 = EventBus::new(store.clone());
        let svc2 = Services::new(store.clone()).with_event_bus(bus2.clone());
        let mut sub2 = bus2.subscribe(SubscriptionFilter {
            event_types: vec!["script:*".to_string()],
            workspace_id: Some(h.ws.0.clone()),
            ..Default::default()
        });
        assert_eq!(svc2.hydrate_scripts().await.expect("hydrate"), 1);

        // Dismiss: stop the hydrated (idle, not running) script.
        let v = svc2
            .script_stop(h.ws.clone(), id.clone())
            .await
            .expect("stop is ok, not an error");
        assert_eq!(v["ok"], true);

        // Dismiss broadcasts the cleared state so other subscribers don't
        // retain a stale `previouslyRunning: true`.
        let ev = await_state(&mut sub2, LIVENESS, |v| {
            v["data"]["scriptId"] == id.as_str()
        })
        .await;
        assert_eq!(ev["data"]["status"], "idle");
        assert!(
            ev["data"].get("previouslyRunning").is_none(),
            "dismiss event carries no marker: {ev}"
        );
        let st = svc2
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["status"], "idle");
        assert!(
            st.get("previouslyRunning").is_none(),
            "dismiss drops the runtime marker: {st}"
        );
        assert!(
            store
                .list_was_running_script_ids()
                .await
                .expect("list")
                .is_empty(),
            "dismiss durably clears the persisted marker"
        );

        // Teardown: reap the still-running PTY owned by the first instance.
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("teardown stop");
    }

    /// Command-mode scripts never set the was-running marker — neither while
    /// running nor after exit.
    #[tokio::test]
    async fn command_mode_never_sets_was_running_marker() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "cmd", "echo done", ScriptMode::Command).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let store = h.services.store().clone();
        // `mark_running` persists (for services) strictly before emitting
        // `running`, so observing the event proves no write happened.
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
        assert!(
            store
                .list_was_running_script_ids()
                .await
                .expect("list")
                .is_empty(),
            "no marker while a command runs"
        );
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "exited").await;
        assert!(
            store
                .list_was_running_script_ids()
                .await
                .expect("list")
                .is_empty(),
            "no marker after a command exits"
        );
    }

    /// A service's natural exit durably clears the marker (the process is
    /// gone; a daemon death from here on must not resurrect the tab).
    #[tokio::test]
    async fn service_natural_exit_clears_was_running_marker() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        // Exits immediately: under the production too-fast floor the exit is
        // final (no auto-restart re-setting the marker).
        let id = create_simple(&h, "svc", "true", ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let store = h.services.store().clone();
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
        assert_eq!(
            store.list_was_running_script_ids().await.expect("list"),
            vec![(h.ws.as_str().to_string(), id.clone())],
            "service start sets the marker"
        );
        // `mark_exited` clears the marker strictly before emitting `exited`.
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "exited").await;
        assert!(
            store
                .list_was_running_script_ids()
                .await
                .expect("list")
                .is_empty(),
            "natural exit clears the marker"
        );
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
                command: SERVICE_CMD.into(),
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
        let running = await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
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
        let deadline = tokio::time::Instant::now() + LIVENESS;
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
        let id = create_simple(&h, "svc", SERVICE_CMD, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
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
    async fn running_script_is_hidden_from_terminal_list_but_output_remains_available() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "svc", SERVICE_CMD, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;

        let terminals = h
            .services
            .terminal_list(h.ws.clone())
            .await
            .expect("terminal list");
        assert_eq!(
            terminals["terminals"],
            json!([]),
            "script PTY must not become a terminal tab"
        );
        assert!(
            h.services
                .script_output(h.ws.clone(), id.clone(), None, None, None)
                .await
                .expect("script output")
                .is_string(),
            "hidden PTY output remains available through script.output"
        );

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
        let id = create_simple(&h, "svc", SERVICE_CMD, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
        let v = h
            .services
            .script_restart(h.ws.clone(), id.clone())
            .await
            .expect("restart");
        assert_eq!(v["ok"], true);
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
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

    /// The `script.restart` stop→start gap reports `restarting` (monorepo#1318):
    /// the status flips before `start()` is invoked and holds until the respawn's
    /// `mark_running`, so a snapshot taken mid-restart never reads `exited`/`idle`.
    /// The supervise park holds the respawn pre-`mark_running` so the window is
    /// open deterministically.
    #[tokio::test]
    async fn script_restart_gap_reports_restarting() {
        let h = harness().await;
        let park = Arc::new(SupervisePark::default());
        let services = h.services.clone().with_script_supervise_park(park.clone());
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "svc", SERVICE_CMD, ScriptMode::Service).await;
        services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("first spawn parked");
        park.release.notify_one();
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;

        let v = services
            .script_restart(h.ws.clone(), id.clone())
            .await
            .expect("restart");
        assert_eq!(v["ok"], true);
        // The old run's `exited` precedes the gap's `restarting` on the bus.
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "exited").await;
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "restarting").await;
        // With the respawn parked pre-`mark_running`, a snapshot inside the
        // gap reads `restarting`.
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("respawn parked");
        let st = services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["status"], "restarting");
        assert_eq!(st["restartCount"], 0, "restart() resets the counter");
        park.release.notify_one();
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
        services.script_stop(h.ws.clone(), id).await.expect("stop");
    }

    /// The auto-restart backoff window reports `restarting` (monorepo#1318):
    /// once the supervise loop commits to a restart (service mode, not
    /// user-stopped, not too-fast, retries left) the status flips before the
    /// backoff sleep and holds until the respawn's `mark_running`. The
    /// too-fast floor is injected as 0 so the immediate exit always restarts,
    /// and the supervise park holds the respawn pre-`mark_running` so the
    /// window is open deterministically. The flag file makes the second run
    /// long-lived so teardown is a clean `script.stop`.
    #[tokio::test]
    async fn auto_restart_backoff_window_reports_restarting() {
        let mut h = harness().await;
        h.services = h.services.with_script_too_fast_ms(0);
        let park = Arc::new(SupervisePark::default());
        let services = h.services.clone().with_script_supervise_park(park.clone());
        let mut sub = subscribe(&h);
        let flag = std::env::temp_dir().join(format!(
            "intentd-scriptops-flag-{}.txt",
            uuid::Uuid::new_v4()
        ));
        let _flag = PidFile(flag.clone());
        let cmd = format!(
            "if [ -e \"{p}\" ]; then exec sleep 3600; else : > \"{p}\"; fi",
            p = flag.display()
        );
        let id = create_simple(&h, "crashy", &cmd, ScriptMode::Service).await;
        services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("first spawn parked");
        park.release.notify_one();
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;

        // The immediate exit commits an auto-restart: `exited` then
        // `restarting` (with the bumped counter) are emitted before the
        // backoff sleep.
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "exited").await;
        let ev = await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "restarting").await;
        assert_eq!(ev["data"]["restartCount"], 1);
        // With the respawn parked pre-`mark_running`, a snapshot inside the
        // backoff window reads `restarting`.
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("respawn parked");
        let st = services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["status"], "restarting");
        assert_eq!(st["restartCount"], 1);
        park.release.notify_one();
        let ev = await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
        assert_eq!(ev["data"]["restartCount"], 1);
        services.script_stop(h.ws.clone(), id).await.expect("stop");
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
        // Command lifetime must outlast any load-induced timer lag so the 1s
        // run timeout always fires first (monorepo#1630); the timeout kill
        // reaps the PTY.
        let id = create_simple(&h, "long", "sleep 3600", ScriptMode::Command).await;
        let out = h
            .services
            .script_run(h.ws.clone(), id, None, Some(1))
            .await
            .expect("run");
        assert_eq!(out["timedOut"], true);
    }

    /// Regression (monorepo#1155): a second `script.run` while the script is
    /// already running must not spawn a second PTY or overwrite `pty_id`
    /// (which would orphan the first run's process on stop/remove); it
    /// warn-and-returns with the service-mode warning shape.
    #[tokio::test]
    async fn script_run_while_running_warns_without_second_pty() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "long", "sleep 3600", ScriptMode::Command).await;
        let services = h.services.clone();
        let ws = h.ws.clone();
        let sid = id.clone();
        let first = tokio::spawn(async move { services.script_run(ws, sid, None, None).await });
        let running = await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
        let pid = running["data"]["pid"].as_i64().expect("pid");

        // Second run while the first is still running: warn-and-return, no
        // second PTY spawn (bounded timeout so an unguarded second PTY fails
        // the shape assertions instead of hanging the test).
        let out = h
            .services
            .script_run(h.ws.clone(), id.clone(), None, Some(5))
            .await
            .expect("second run");
        assert_eq!(out["output"], "", "warn-and-return shape: {out:?}");
        assert!(
            out["warning"]
                .as_str()
                .unwrap_or("")
                .contains("already running"),
            "warning says already running: {out:?}"
        );

        // `pty_id` was not overwritten: status still reports the first run's pid.
        let st = h
            .services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["status"], "running");
        assert_eq!(st["pid"], json!(pid), "pid unchanged: {st:?}");

        // Stop kills the tracked (first) PTY and the first run completes.
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
        let res = first.await.expect("join").expect("first run");
        assert_eq!(res["timedOut"], false);
    }

    /// Regression (monorepo#1155): dropping the `script.run` future mid-flight
    /// (as an eval-level timeout cancelling the RPC does) must not skip
    /// cleanup — the detached completion task still enforces the script-level
    /// timeout, kills the PTY (child reaped, no orphan), and emits the
    /// `exited` `script:state` transition.
    #[tokio::test]
    async fn script_run_dropped_future_still_reaps_and_marks_exited() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create_simple(&h, "long", "sleep 3600", ScriptMode::Command).await;
        // Drive `run` until the script is `running`, then drop the future
        // mid-flight (caller-side cancellation).
        let pid = {
            let mut fut = Box::pin(
                h.services
                    .script_run(h.ws.clone(), id.clone(), None, Some(1)),
            );
            'running: loop {
                tokio::select! {
                    res = &mut fut => panic!("script.run finished before cancellation: {res:?}"),
                    batch = tokio::time::timeout(LIVENESS, sub.recv()) => {
                        let batch = batch
                            .expect("event delivered before deadline")
                            .expect("subscription open");
                        for ev in &batch {
                            let v = serde_json::to_value(ev).expect("serialize");
                            if v["type"] == "script:state" && v["data"]["status"] == "running" {
                                break 'running v["data"]["pid"].as_i64().expect("pid");
                            }
                        }
                    }
                }
            }
        };
        // The detached completion task must still time the script out, kill
        // the PTY, and flip the state to `exited`.
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "exited").await;
        let st = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .expect("status");
        assert_eq!(st["status"], "exited");
        // The child process must be reaped — no orphan (kill -0 fails).
        let deadline = tokio::time::Instant::now() + LIVENESS;
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
                "script process {pid} still alive after dropped script.run"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Poll until `pid` is gone, failing after the `LIVENESS` deadline.
    async fn await_pid_dead(pid: i64, what: &str) {
        let deadline = tokio::time::Instant::now() + LIVENESS;
        while pid_alive(pid) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{what} {pid} still alive after deadline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// SIGKILL a helper process on drop so a failed test cannot leak a
    /// TERM/HUP-trapping `sleep 300` into the suite.
    struct KillOnDrop(i64);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = std::process::Command::new("kill")
                .args(["-9", &self.0.to_string()])
                .stderr(std::process::Stdio::null())
                .status();
        }
    }

    /// Build a command whose shell backgrounds a TERM+HUP-trapping straggler:
    /// the straggler touches `flag` only after its traps are installed, and
    /// its pid is written to `pidfile`. `tail` runs in the direct shell after
    /// the traps are confirmed active.
    fn straggler_command(flag: &std::path::Path, pidfile: &std::path::Path, tail: &str) -> String {
        format!(
            r#"sh -c 'trap "" TERM HUP; : > "{f}"; sleep 300' & echo $! > "{p}"; while [ ! -e "{f}" ]; do sleep 0.05; done; {tail}"#,
            f = flag.display(),
            p = pidfile.display()
        )
    }

    /// Poll `pidfile` (and the trap flag via the command's handshake) until it
    /// yields the straggler's pid.
    async fn await_straggler_pid(pidfile: &std::path::Path) -> i64 {
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            if let Ok(s) = std::fs::read_to_string(pidfile) {
                if let Ok(pid) = s.trim().parse::<i64>() {
                    return pid;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "straggler pid never written to {pidfile:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Unique temp paths for the straggler handshake files.
    fn straggler_paths(tag: &str) -> (TempPath, TempPath) {
        let unique = format!(
            "{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        (
            TempPath(std::env::temp_dir().join(format!("intent-script-flag-{unique}"))),
            TempPath(std::env::temp_dir().join(format!("intent-script-pid-{unique}"))),
        )
    }

    /// Removes the file on drop.
    struct TempPath(std::path::PathBuf);
    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Regression (monorepo#1300): `script.stop` must reap a descendant that
    /// traps both SIGTERM and SIGHUP — the old escalation keyed SIGKILL on the
    /// direct child still running, so a shell that exited within the grace
    /// window left the trapped descendant alive forever.
    #[tokio::test]
    async fn script_stop_reaps_term_and_hup_trapping_descendant() {
        let h = harness().await;
        let (flag, pidfile) = straggler_paths("stop");
        let cmd = straggler_command(&flag.0, &pidfile.0, "wait");
        let id = create_simple(&h, "trapper", &cmd, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let straggler = await_straggler_pid(&pidfile.0).await;
        let _guard = KillOnDrop(straggler);
        assert!(pid_alive(straggler), "straggler alive before stop");

        h.services
            .script_stop(h.ws.clone(), id.clone())
            .await
            .expect("stop");
        await_pid_dead(straggler, "TERM+HUP-trapping descendant").await;
        let st = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .expect("status");
        assert_ne!(st["status"], "running", "status reflects the dead group");
    }

    /// Clean daemon shutdown (monorepo#1526): `shutdown_pty_sessions` flags a
    /// running service with user-stop semantics — the TERM+HUP-trapping
    /// straggler dies, the supervisor does not auto-restart it — and kills
    /// every PTY the host tracks (the script's and a plain terminal-style
    /// session) in one sweep, leaving the host empty and closed. The durable
    /// `was_running` marker survives the graceful shutdown so the next boot
    /// still offers the relaunch affordance (monorepo#932 parity with daemon
    /// death).
    #[tokio::test]
    async fn shutdown_pty_sessions_stops_scripts_and_kills_all_ptys() {
        let h = harness().await;
        let (flag, pidfile) = straggler_paths("shutdown");
        let cmd = straggler_command(&flag.0, &pidfile.0, "wait");
        let id = create_simple(&h, "svc", &cmd, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let straggler = await_straggler_pid(&pidfile.0).await;
        let _guard = KillOnDrop(straggler);
        assert!(pid_alive(straggler), "straggler alive before shutdown");

        // A terminal-style PTY outside any script registry entry.
        let terminal = h
            .services
            .pty()
            .spawn(SpawnSpec::new("ws-term", "cat"))
            .expect("spawn terminal");
        assert!(h.services.pty().is_alive(terminal));

        let (scripts, ptys) = h.services.shutdown_pty_sessions().await;
        assert_eq!(scripts, 1, "one running script stopped");
        assert_eq!(ptys, 2, "the script and terminal PTYs killed in one sweep");

        await_pid_dead(straggler, "TERM+HUP-trapping straggler").await;
        assert_eq!(h.services.pty().count(), 0, "host empty after shutdown");

        // User-stop semantics: the awaited supervisor exited without
        // respawning, so the script settles non-running with no new PTY.
        let st = h
            .services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_ne!(st["status"], "running", "no auto-restart after stop-all");
        assert_eq!(h.services.pty().count(), 0, "no respawned PTY");

        // Graceful shutdown preserves the relaunch affordance: the service
        // was running when the sweep began, so `was_running` stays set.
        let markers = h
            .services
            .store()
            .list_was_running_script_ids()
            .await
            .expect("list markers");
        assert!(
            markers.contains(&(h.ws.as_str().to_string(), id.clone())),
            "was_running marker survives graceful shutdown: {markers:?}"
        );
    }

    /// A stop-all racing an auto-restart respawn (monorepo#1526): the
    /// supervisor passed its post-backoff stopped check and spawned a fresh
    /// PTY, but has not yet registered it when `stop_all` flags the script
    /// and awaits the handle. The respawn's `mark_running` must refuse on the
    /// concurrent `stopped_by_user` — the supervisor reaps the fresh PTY and
    /// returns — so the shutdown sweep neither hangs on the supervisor nor
    /// lets the replacement group survive. Pre-fix, `mark_running` ignored
    /// the flag: the fresh PTY was adopted after `stop_all`'s snapshot and
    /// its group outlived the daemon.
    #[tokio::test]
    async fn stop_all_during_respawn_window_refuses_registration_and_reaps() {
        let mut h = harness().await;
        h.services = h.services.with_script_too_fast_ms(0);
        let park = Arc::new(SupervisePark::default());
        let services = h.services.clone().with_script_supervise_park(park.clone());
        let mut sub = subscribe(&h);
        let flag = std::env::temp_dir().join(format!(
            "intentd-scriptops-flag-{}.txt",
            uuid::Uuid::new_v4()
        ));
        let _flag = PidFile(flag.clone());
        let pidfile = std::env::temp_dir().join(format!(
            "intentd-scriptops-pid-{}.txt",
            uuid::Uuid::new_v4()
        ));
        let _pidfile = PidFile(pidfile.clone());
        // First run: create the flag and exit (commits an auto-restart with
        // the too-fast floor at 0). Second run: record the pid, sleep.
        let cmd = format!(
            "if [ -e \"{f}\" ]; then echo $$ > \"{p}\"; exec sleep 3600; else : > \"{f}\"; fi",
            f = flag.display(),
            p = pidfile.display()
        );
        let id = create_simple(&h, "race", &cmd, ScriptMode::Service).await;
        services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("first spawn parked");
        park.release.notify_one();
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "restarting").await;
        // The respawn parks pre-`mark_running`: the replacement PTY exists
        // (its shell has written the pidfile) but is not yet registered.
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("respawn parked");
        let fresh = read_pid(&pidfile).await;
        let _guard = KillOnDrop(fresh);
        assert!(
            pid_alive(fresh),
            "replacement shell alive inside the window"
        );

        // stop_all flags the script and awaits the parked supervisor. Let the
        // sweep task run its synchronous flag phase (everything up to the
        // teardown awaits) before releasing the park, so the respawn's
        // registration deterministically observes the concurrent stop.
        let sweep_services = h.services.clone();
        let sweep = tokio::spawn(async move { sweep_services.shutdown_pty_sessions().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        park.release.notify_one();
        let (scripts, _ptys) = tokio::time::timeout(LIVENESS, sweep)
            .await
            .expect("shutdown sweep returned before deadline")
            .expect("join");
        assert_eq!(scripts, 1, "the racing script was in the stop-all sweep");

        await_pid_dead(fresh, "replacement spawned inside the window").await;
        assert_eq!(h.services.pty().count(), 0, "host empty after the sweep");
        let st = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .expect("status");
        assert_ne!(st["status"], "running", "refused registration never ran");
    }

    /// Regression (monorepo#1300): when the shell exits on its own but a
    /// TERM+HUP-trapping group member lingers, the supervisor reaps the
    /// group before recording the exit — the script cannot present as a
    /// healthy `running` (or `exited`-with-survivors) while trapped
    /// stragglers hold the group.
    #[tokio::test]
    async fn script_exit_with_trapped_straggler_reaps_group_before_exited() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let (flag, pidfile) = straggler_paths("exit");
        let cmd = straggler_command(&flag.0, &pidfile.0, "exit 0");
        let id = create_simple(&h, "straggler", &cmd, ScriptMode::Command).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let straggler = await_straggler_pid(&pidfile.0).await;
        let _guard = KillOnDrop(straggler);

        // The shell exits immediately after the handshake; `exited` must not
        // be recorded until the group has been reaped.
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "exited").await;
        await_pid_dead(straggler, "trapped straggler").await;
        let st = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .expect("status");
        assert_eq!(st["status"], "exited");
    }

    /// Regression (monorepo#1155): a `resolve_cwd` failure after the running
    /// reservation must reset the status via `fail()` — the script ends up
    /// `exited` with the error recorded, and a follow-up run hits the same
    /// error instead of the already-running guard.
    #[tokio::test]
    async fn script_run_cwd_failure_resets_reservation_via_fail() {
        let h = harness_with_worktree(true).await;
        let id = create(
            &h,
            ScriptCreateParams {
                name: "bad-cwd".into(),
                command: "echo never".into(),
                mode: ScriptMode::Command,
                cwd: Some("../escape".into()),
                ..Default::default()
            },
        )
        .await;
        let err = h
            .services
            .script_run(h.ws.clone(), id.clone(), None, Some(5))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("escapes workspace root"),
            "got: {err:?}"
        );
        let st = h
            .services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(
            st["status"], "exited",
            "fail() reset the reservation: {st:?}"
        );
        assert!(
            st["error"]
                .as_str()
                .unwrap_or("")
                .contains("escapes workspace root"),
            "error recorded on state: {st:?}"
        );
        let err2 = h
            .services
            .script_run(h.ws.clone(), id, None, Some(5))
            .await
            .unwrap_err();
        assert!(
            err2.to_string().contains("escapes workspace root"),
            "follow-up run hits the cwd error, not the running guard: {err2:?}"
        );
    }

    /// Regression (monorepo#1155): the running reservation is taken under the
    /// same lock acquisition as the guard check, so a concurrent `script.run`
    /// entering during the pre-`mark_running` window (the `resolve_cwd`
    /// await) hits the already-running guard instead of spawning a second
    /// PTY — and a caller-side cancellation inside that window releases the
    /// reservation instead of leaving the script stuck `running`.
    #[tokio::test]
    async fn script_run_reservation_blocks_concurrent_entry_and_cancel_releases_it() {
        let h = harness().await;
        let id = create_simple(&h, "long", "sleep 3600", ScriptMode::Command).await;
        {
            let mut fut = Box::pin(
                h.services
                    .script_run(h.ws.clone(), id.clone(), None, Some(1)),
            );
            // One poll reserves the run (synchronously, before the first
            // await) and parks the future inside the reservation window.
            let pending = tokio::time::timeout(Duration::from_millis(0), &mut fut).await;
            assert!(pending.is_err(), "run should still be in flight");
            // A concurrent entry inside the window warn-and-returns.
            let out = h
                .services
                .script_run(h.ws.clone(), id.clone(), None, Some(5))
                .await
                .expect("concurrent run");
            assert_eq!(out["output"], "", "warn-and-return shape: {out:?}");
            assert!(
                out["warning"]
                    .as_str()
                    .unwrap_or("")
                    .contains("already running"),
                "warning says already running: {out:?}"
            );
            // Dropping `fut` cancels the first run mid-window.
        }
        // Whether the drop landed pre-spawn (silent reservation restore) or
        // post-spawn (the detached task kills at the 1s timeout), the script
        // must not stay `running`…
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            let st = h
                .services
                .script_status(h.ws.clone(), id.clone())
                .await
                .expect("status");
            if st["status"] != "running" {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "script stuck running after cancelled reservation: {st:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // …and a fresh run passes the guard again (real envelope, no warning).
        let out = h
            .services
            .script_run(h.ws.clone(), id, None, Some(1))
            .await
            .expect("fresh run");
        assert_eq!(out["timedOut"], true, "fresh run executed: {out:?}");
        assert!(out.get("warning").is_none(), "no warning: {out:?}");
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
        // Positive run: the timeout is a pure-liveness bound (monorepo#1630)
        // — the run returns as soon as the echo loop exits, so the long bound
        // only has to outlast a login-shell spawn stall under parallel load.
        let out = h
            .services
            .script_run(h.ws.clone(), id, Some(2), Some(LIVENESS.as_secs() as i64))
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
        // Pure-liveness run timeout (monorepo#1630): returns on exit.
        h.services
            .script_run(
                h.ws.clone(),
                id.clone(),
                None,
                Some(LIVENESS.as_secs() as i64),
            )
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
        // Pure-liveness run timeout (monorepo#1630): returns on exit.
        h.services
            .script_run(
                h.ws.clone(),
                id.clone(),
                None,
                Some(LIVENESS.as_secs() as i64),
            )
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
        let id = create_simple(&h, "svc", SERVICE_CMD, ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
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

    // ---- teardown vs. pre-registration window (monorepo#1180) --------------

    /// Removes the pidfile a parked script wrote once the test drops it.
    struct PidFile(PathBuf);
    impl Drop for PidFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// A service script parked in `supervise()`'s pre-registration window:
    /// its PTY is spawned (pid captured via the pidfile) but the registry does
    /// not yet record the `PtyId`.
    struct ParkedScript {
        services: Services,
        park: Arc<SupervisePark>,
        id: String,
        pid: i64,
        _pidfile: PidFile,
    }

    /// Best-effort kill on drop so a test that fails/panics before teardown
    /// completes cannot leak the long-lived child process into CI.
    impl Drop for ParkedScript {
        fn drop(&mut self) {
            let _ = std::process::Command::new("kill")
                .args(["-9", &self.pid.to_string()])
                .stderr(std::process::Stdio::null())
                .status();
        }
    }

    /// Start a service through park-enabled services and hold `supervise()`
    /// inside the window after `pty.spawn`, before `mark_running`.
    async fn start_parked_service(h: &Harness) -> ParkedScript {
        let park = Arc::new(SupervisePark::default());
        let services = h.services.clone().with_script_supervise_park(park.clone());
        let pidfile = std::env::temp_dir().join(format!(
            "intentd-scriptops-pid-{}.txt",
            uuid::Uuid::new_v4()
        ));
        let cmd = format!("echo $$ > \"{}\" && exec sleep 3600", pidfile.display());
        let id = create_simple(h, "parked", &cmd, ScriptMode::Service).await;
        services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        tokio::time::timeout(LIVENESS, park.entered.notified())
            .await
            .expect("supervise entered the pre-registration window");
        let pid = read_pid(&pidfile).await;
        ParkedScript {
            services,
            park,
            id,
            pid,
            _pidfile: PidFile(pidfile),
        }
    }

    /// Poll the pidfile until the spawned shell has written its pid.
    async fn read_pid(path: &std::path::Path) -> i64 {
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            if let Ok(s) = std::fs::read_to_string(path) {
                if let Ok(pid) = s.trim().parse::<i64>() {
                    return pid;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "pidfile never written"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn pid_alive(pid: i64) -> bool {
        std::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .expect("run kill -0")
            .success()
    }

    /// The process spawned inside the window must die — no orphan (kill -0
    /// eventually fails; hard-fails at the liveness bound on a leak).
    async fn assert_reaped(pid: i64, context: &str) {
        let deadline = tokio::time::Instant::now() + LIVENESS;
        while pid_alive(pid) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "script process {pid} still alive {context}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until the teardown under test has taken the registry entry (or,
    /// pre-fix, already finished outright) before releasing the park.
    async fn await_entry_taken<T>(
        h: &Harness,
        p: &ParkedScript,
        task: &tokio::task::JoinHandle<T>,
    ) {
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            let gone = matches!(
                p.services.script_status(h.ws.clone(), p.id.clone()).await,
                Err(Error::NotFound(_))
            );
            if gone || task.is_finished() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "teardown never took the registry entry"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Regression (monorepo#1180): `script.remove` racing `supervise()`'s
    /// pre-registration window (PTY spawned, id not yet recorded) must still
    /// reap the fresh PTY. Pre-fix, `remove()` aborted the supervisor at that
    /// await point and the process leaked forever.
    #[tokio::test]
    async fn script_remove_in_pre_registration_window_reaps_fresh_pty() {
        let h = harness().await;
        let p = start_parked_service(&h).await;
        let services = p.services.clone();
        let ws = h.ws.clone();
        let sid = p.id.clone();
        let rm = tokio::spawn(async move { services.script_remove(ws, sid).await });
        await_entry_taken(&h, &p, &rm).await;
        p.park.release.notify_one();
        let res = tokio::time::timeout(LIVENESS, rm)
            .await
            .expect("remove returned before deadline")
            .expect("join")
            .expect("remove ok");
        assert_eq!(res["ok"], true);
        assert_reaped(p.pid, "after script.remove in the pre-registration window").await;
        assert!(
            h.services.pty().list_scope(h.ws.as_str()).is_empty(),
            "no PTY session left registered for the workspace"
        );
    }

    /// Regression (monorepo#1180): a `script.create` upsert racing the same
    /// window must also reap the old supervisor's fresh PTY before installing
    /// the new definition.
    #[tokio::test]
    async fn script_create_upsert_in_pre_registration_window_reaps_fresh_pty() {
        let h = harness().await;
        let p = start_parked_service(&h).await;
        let services = p.services.clone();
        let ws = h.ws.clone();
        let sid = p.id.clone();
        let up = tokio::spawn(async move {
            services
                .script_create(
                    ws,
                    ScriptCreateParams {
                        name: "replaced".into(),
                        command: "echo hi".into(),
                        mode: ScriptMode::Command,
                        script_id: Some(sid),
                        ..Default::default()
                    },
                )
                .await
        });
        await_entry_taken(&h, &p, &up).await;
        p.park.release.notify_one();
        let v = tokio::time::timeout(LIVENESS, up)
            .await
            .expect("upsert returned before deadline")
            .expect("join")
            .expect("upsert ok");
        assert_eq!(v["id"].as_str(), Some(p.id.as_str()), "upsert keeps the id");
        assert_reaped(p.pid, "after create-upsert in the pre-registration window").await;
        let st = h
            .services
            .script_status(h.ws.clone(), p.id.clone())
            .await
            .expect("status");
        assert_eq!(st["status"], "idle", "fresh entry starts idle");
    }

    /// Regression (monorepo#1194): `script.remove` + `script.create` with the
    /// same `scriptId` racing `script.start`'s spawn-to-registration window
    /// must not let the stale supervisor latch onto the recreated entry. Both
    /// park seams hold the race open deterministically: the supervisor parks
    /// pre-`mark_running` (PTY spawned) and `start()` parks pre-registration,
    /// while the test removes and recreates the entry. Pre-fix, registration
    /// installed the stale handle (entry present under the same key) and
    /// `mark_running` flipped the recreated entry to `running` with the old
    /// command's pid, leaking the old PTY.
    #[tokio::test]
    async fn script_recreate_in_start_registration_window_is_not_adopted() {
        let h = harness().await;
        let supervise_park = Arc::new(SupervisePark::default());
        let start_park = Arc::new(SupervisePark::default());
        let services = h
            .services
            .clone()
            .with_script_supervise_park(supervise_park.clone())
            .with_script_start_registration_park(start_park.clone());
        let old_pidfile = std::env::temp_dir().join(format!(
            "intentd-scriptops-pid-{}.txt",
            uuid::Uuid::new_v4()
        ));
        let _old_pidfile = PidFile(old_pidfile.clone());
        let old_cmd = format!("echo $$ > \"{}\" && exec sleep 3600", old_pidfile.display());
        let id = create(
            &h,
            ScriptCreateParams {
                name: "old".into(),
                command: old_cmd,
                mode: ScriptMode::Service,
                script_id: Some("gen-race".into()),
                ..Default::default()
            },
        )
        .await;

        // Drive `start()` into its parked pre-registration window; the
        // supervisor task independently parks pre-`mark_running` with the old
        // command's PTY already spawned.
        let svc = services.clone();
        let ws = h.ws.clone();
        let sid = id.clone();
        let start_task = tokio::spawn(async move { svc.script_start(ws, sid).await });
        tokio::time::timeout(LIVENESS, start_park.entered.notified())
            .await
            .expect("start parked before registration");
        tokio::time::timeout(LIVENESS, supervise_park.entered.notified())
            .await
            .expect("supervise parked before mark_running");
        let old_pid = read_pid(&old_pidfile).await;

        // Remove + recreate the same id while both windows are held open.
        h.services
            .script_remove(h.ws.clone(), id.clone())
            .await
            .expect("remove");
        let new_pidfile = std::env::temp_dir().join(format!(
            "intentd-scriptops-pid-{}.txt",
            uuid::Uuid::new_v4()
        ));
        let _new_pidfile = PidFile(new_pidfile.clone());
        let new_cmd = format!("echo $$ > \"{}\" && exec sleep 3600", new_pidfile.display());
        let recreated = create(
            &h,
            ScriptCreateParams {
                name: "new".into(),
                command: new_cmd,
                mode: ScriptMode::Service,
                script_id: Some(id.clone()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(recreated, id, "recreate keeps the id");

        // Release both windows. The stale supervisor must fail the generation
        // check, reap its own PTY, and exit; `start()`'s registration must
        // treat the handle as an orphan and await it, so once `start()`
        // returns the stale supervisor is fully drained.
        supervise_park.release.notify_one();
        start_park.release.notify_one();
        tokio::time::timeout(LIVENESS, start_task)
            .await
            .expect("start returned before deadline")
            .expect("join")
            .expect("start ok");
        assert_reaped(old_pid, "after remove+recreate in the registration window").await;

        // The recreated entry never adopted the stale supervisor's PTY/pid.
        let st = h
            .services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["status"], "idle", "recreated entry stays idle: {st:?}");
        assert!(st["pid"].is_null(), "no stale pid latched: {st:?}");

        // A follow-up start on the recreated entry is not blocked by a
        // phantom `running` status and runs the NEW command.
        let mut sub = subscribe(&h);
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start recreated");
        let running = await_state(&mut sub, LIVENESS, |v| v["data"]["status"] == "running").await;
        let new_pid = read_pid(&new_pidfile).await;
        assert_ne!(new_pid, old_pid, "new command's process, not the old one");
        assert_eq!(
            running["data"]["pid"].as_i64(),
            Some(new_pid),
            "running state reports the new command's pid"
        );
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
        assert_reaped(new_pid, "after stopping the recreated script").await;
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
        let ev = await_state(&mut sub, LIVENESS, |v| {
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
