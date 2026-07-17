//! intent-services — the shared business-logic surface (§3.1).
//!
//! Depends on core, store, git, sourcecontrol, acp, context, providers, pty,
//! and search (§3.2). Sibling feature modules never import each other; they
//! communicate through the store and the event bus (§3.2 rule 4). This slice
//! implements the read-only `WorkspaceApi` surface (`workspace.list` /
//! `note.list`) over `intent-store`.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use base64::Engine as _;
use intent_core::events::{
    AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, CHANGES_GIT_STATUS, CHANGES_METRICS_CHANGED,
    COMMENT_ADDED, COMMENT_RESOLVED, GIT_BRANCH, GIT_COMMIT, GIT_PULL, GIT_PUSH,
    LINE_ATTRIBUTION_UPDATED, NOTE_CREATED, NOTE_DELETED, NOTE_UPDATED, PR_LINKED, PR_UNLINKED,
    PR_UPDATED, SEARCH_DONE, SEARCH_RESULT, SETTINGS_CHANGED, TASK_AGENT_LINKED,
    TASK_AGENT_UNLINKED, TASK_READY_TASKS_CHANGED, TASK_STATUS_CHANGED, WORKSPACE_ACTIVITY_CHANGED,
    WORKSPACE_ATTENTION_CHANGED, WORKSPACE_CONTEXT_CHANGED, WORKSPACE_CREATED, WORKSPACE_DELETED,
    WORKSPACE_TOKEN_USAGE_CHANGED, WORKSPACE_UPDATED,
};
use intent_core::AgentReverseDispatch;
use intent_core::{
    chief_workspace, iso_minutes_ago, now_epoch_ms, now_iso, parse_iso, ActorType,
    AgentDelegateInput, AgentId, AgentLite, AgentSession, AuthorType, BoxFuture, ClientId, Comment,
    CommentAddResult, CommentAnchor, CommentAnchorType, CommentDeleteResult,
    CommentGetThreadResult, CommentListResult, CommentLocation, CommentResolveThreadResult,
    CommentRespondResult, CommentRespondThread, CommentStatus, CommentThreadSummary, CommentType,
    CommentWire, ContentType, ContextItem, Draft, Event, EventQueryParams, EventSubscribeResult,
    EventUnsubscribeResult, FileActivity, LineAttributionAuthor, LineAttributionComputeResult,
    LineAttributionData, LineAttributionInfo, Note, NoteAddInput, NoteAddResult, NoteCreate,
    NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult,
    NoteId, NoteMetadata, NoteRestoreVersionResult, NoteSetContentResult, NoteTaskRow,
    NoteUpdateInput, NoteUpdateMetadataResult, NoteVersion, NoteVersionAuthor, NoteVersionSummary,
    NoteVisibility, ProjectType, ReadAssetResult, SaveAssetResult, ScriptCreateParams,
    SessionStats, SetupScript, TaskAgentLink, TaskAssignAgentResult, TaskConvertBlocksResult,
    TaskCreatePrerequisiteResult, TaskGetMyTaskResult, TaskListResult, TaskMarkAsTaskResult,
    TaskMetadata, TaskRemoveAgentFromAllTasksResult, TaskStatus, TaskSubtask,
    TaskUpdateNoteStatusResult, TaskUpdateResult, TaskUpdateStatusResult, TokenUsage, Workspace,
    WorkspaceActivity, WorkspaceAgentInfo, WorkspaceAgentSummary, WorkspaceAttention,
    WorkspaceCreate, WorkspaceCreateResult, WorkspaceDiffSummary, WorkspaceEventSummary,
    WorkspaceId, WorkspaceStatus, WorkspaceTask, WorkspaceTaskStats, WorkspaceUpdate,
};
use intent_store::{EventQuery, NewEvent, Store};

pub use intent_core::{Error, Result, WorkspaceApi};

mod agent_manager;
mod agent_ops;
mod agent_session;
mod agent_subscriptions;
mod auto_commit;
pub mod browser_ops;
mod clone_ops;
mod complete_ops;
#[cfg(test)]
mod completion_interception_tests;
mod crdt_notes;
mod drafts;
mod enhance_ops;
mod event_ops;
pub mod events;
mod file_ops;
mod git_ops;
pub mod host_exec;
pub mod host_exec_stream;

mod github_ops;

mod github_browse_ops;

mod history_xml;
mod line_attribution;
mod linear_ops;
mod note_ops;
mod pagination;
mod pr_ops;
mod primitive_ops;
pub mod repo_config;
mod rtk;
mod sandbox_ops;
mod script_ops;
mod search_ops;
mod sentry_ops;
mod settings;
mod terminal_ops;
pub mod tool_block;

#[cfg(test)]
mod tests;

pub use mcp_servers::McpHub;
pub use sandbox_ops::ProvisionOutcome;
pub use settings::{max_concurrent_agents, InMemorySecretStore, SecretStore};
pub use terminal_ops::PtyTerminalHost;

/// Re-export the auggie discovery surface so the transport layer can reuse the
/// canonical resolver (`find_auggie`, managed binary, enhanced PATH) without
/// taking a direct dependency on `intent-context` (§3.2 rule 2).
pub mod auggie_discovery {
    pub use intent_context::discovery::{
        enhanced_path_dirs, find_auggie, find_in_dirs, managed_binary_path,
    };
}

pub use agent_manager::{
    compute_process_cap, default_process_cap, AgentManager, BusEventSink, ProcessRegistry,
};
// Re-export the permission types the composition root (`INTENTD_PERMISSION_POLICY`)
// and the transport router (`agent.respondPermission` outcome parsing) need.
pub use events::{EventBus, FileWatcher, Subscription, SubscriptionFilter};
pub use intent_acp::{PermissionOutcome, PermissionPolicy, PermissionRequestData};
pub use pr_ops::PrRefreshOutcome;

/// Statuses that the FE treats as in-flight (its `isActiveAgentThread`
/// selector returns `true` for these). After a daemon crash the runtime
/// in-memory `AgentManager` and live-turn slots are empty, so a session
/// persisted in one of these statuses is "stale": it has no worker but the FE
/// would still render a "Thinking" spinner. The heal sweep rewrites them to
/// [`intent_core::AgentStatus::RuntimeIdle`].
fn is_stale_in_flight_status(status: intent_core::AgentStatus) -> bool {
    matches!(
        status,
        intent_core::AgentStatus::Active
            | intent_core::AgentStatus::Processing
            | intent_core::AgentStatus::Waiting
    )
}

/// Aggregate service handle wired by the binary composition root. It implements
/// `WorkspaceApi` so it can be handed to `intent-acp` as `Arc<dyn WorkspaceApi>`
/// (§6.8) and dispatched to by the transport router.
#[derive(Clone)]
pub struct Services {
    store: Store,
    /// Root directory for note assets, laid out as `<root>/<workspaceId>/<assetId>`.
    /// `None` until configured by the composition root; `note.readAsset` errors
    /// when unset.
    assets_root: Option<PathBuf>,
    /// Live ids minted by the deprecated `event.subscribe` alias so that
    /// `event.unsubscribe` can report found/not-found. This is the service-style
    /// surface only; WS streaming is the separate `events.*` fast-path (§6).
    event_subscriptions: Arc<Mutex<HashSet<String>>>,
    /// Shared event bus that CRUD mutations publish change events onto (§10).
    /// `None` until wired by the composition root; when unset, mutations persist
    /// as before but emit no events (keeps read-only/test wiring unchanged).
    event_bus: Option<EventBus>,
    /// Per-agent in-memory send queues backing `agent.queueMessage` /
    /// `agent.getQueue` (and the `agent.sendMessage` auto-queue fallback). The
    /// live-stream coupling (flipping `queued` while a turn is mid-flight) lands
    /// with the end-to-end orchestration flow; the queue surface itself is here.
    agent_queues: Arc<Mutex<HashMap<AgentId, Vec<agent_ops::QueuedMessage>>>>,
    /// Last per-session stats snapshot observed by `agent.getSessionStats`
    /// (PROTOCOL §5.24). The `stats` field on `AgentSession` is derived/not
    /// persisted, so this in-memory cache lets a refresh detect a change and push
    /// `agent:session-stats-changed` only when the rollup actually moved (§6.5).
    session_stats_cache: Arc<Mutex<HashMap<AgentId, SessionStats>>>,
    /// 5-minute success cache for the `models.list` rich catalog (PROTOCOL
    /// §5.30), porting the reference app's provider-model cache. Only a
    /// successful auggie CLI fetch is cached; the static fallback is recomputed
    /// per call. Shared across clones so every handle sees the same window.
    models_cache: agent_ops::ModelsCache,
    /// Test-only override for the auggie binary the one-shot CLI RPCs
    /// (`agent.enhancePrompt` §5.31, `agent.completeOnce` §5.32) spawn.
    /// Production composition leaves this `None` and the ops resolve the CLI
    /// via `intent_context::discovery::find_auggie`; tests point it at a
    /// deterministic fixture script.
    auggie_bin: Option<PathBuf>,
    /// Daemon-owned parent→child completion-watch registry (AS-2), keyed by
    /// workspace. A oneShot watch is registered when an agent delegates with
    /// `waitMode` `immediate` over the MCP front door; the delivery worker (AS-3)
    /// and the `after_all` group fan-in (AS-4) consume it later. Shared across
    /// clones like the other in-memory registries.
    agent_subscriptions: Arc<Mutex<HashMap<WorkspaceId, agent_subscriptions::WorkspaceWatches>>>,
    /// Back-reference to the runtime [`AgentManager`] so the `agent.*` RPC
    /// handlers drive the real spawn/turn/MCP loop (§6.8). Held as a [`Weak`] to
    /// break the `AgentManager → Services` ownership cycle; the composition root
    /// keeps the strong `Arc<AgentManager>` for the daemon's lifetime. `None`
    /// until attached, so read-only/test wiring keeps the store-only behavior.
    agent_manager: Arc<OnceLock<Weak<AgentManager>>>,
    /// Active forge for the `pr.*` methods (§7). `None` until wired by the
    /// composition root or a test; when unset, the `pr.*` handlers build the
    /// provider from default settings (token from env / `gh` / keychain).
    source_control: Option<Arc<dyn intent_sourcecontrol::SourceControl>>,
    /// Active Linear engine for the `linear.*` methods (§5.28). `None` until
    /// wired by the composition root or a test; when unset, the `linear.*`
    /// handlers build the engine from default settings (key from
    /// `LINEAR_API_KEY` / keychain), surfacing a graceful "not configured"
    /// `Internal` error when no key is available.
    linear_engine: Option<Arc<dyn intent_linear::LinearEngine>>,
    /// Active Sentry engine for the `sentry.*` methods (§5.29). `None` until
    /// wired by the composition root or a test; when unset, the `sentry.*`
    /// handlers build the engine from default settings (org/token from
    /// `SENTRY_ORG` / `SENTRY_API_TOKEN` / keychain), surfacing a graceful
    /// "not configured" `Internal` error when no pair is available.
    sentry_engine: Option<Arc<dyn intent_sentry::SentryEngine>>,
    /// Per-worktree async locks (`withGitWorktreeLock` parity, §9.5) so the
    /// `accept-changes.execute` commit/push path never races concurrent agents
    /// or operations on the same worktree.
    worktree_locks: intent_git::worktree::WorktreeLocks,
    /// Root directory `workspace.create` provisions worktrees under
    /// (`<root>/<workspaceId>/<repo-slug>`). `None` resolves the default at
    /// call time: `$INTENTD_WORKSPACES_DIR`, else `~/intent/workspaces` (the
    /// FE's `WorkspaceConfig.WORKSPACES_BASE`). Tests inject a temp dir.
    workspaces_root: Option<PathBuf>,
    /// Per-request cancellation registry for `search.*` (§14.3). Keyed by
    /// `requestId`, it lets `search.cancel` abort an in-flight search. Shares
    /// its inner map across clones so a cancel observed by any handle reaches
    /// the running walk.
    search_cancels: intent_search::CancelRegistry,
    /// Per-workspace count of in-flight agent sessions backing the **derived**
    /// `WorkspaceActivity` green dot (§9.9). Shared across clones so the
    /// [`AgentManager`] (which holds a [`Services`]) and the `WorkspaceApi`
    /// read door observe the same state. Never persisted: `AgentRunning` iff a
    /// workspace's count is non-zero. Transitions to/from zero emit
    /// `workspace:activity-changed`.
    agent_activity: Arc<Mutex<HashMap<WorkspaceId, usize>>>,
    /// The unified PTY host backing `terminal.*` and the ACP `terminal/*`
    /// adapter (§12). Shared across clones so every service handle — and the
    /// [`AgentManager`] that builds the ACP terminal adapter — drives the same
    /// terminals.
    pty: Arc<intent_pty::PtyHost>,
    /// The shared `script.*` registry (definitions + runtime + supervisor tasks),
    /// keyed by script id. Scripts run on the same [`pty`](Self::pty) host as
    /// `terminal.*`, so a terminal can attach to a running script (§12.2).
    scripts: script_ops::ScriptRegistry,
    /// Per-workspace async-mutex map for script bootstrap operations, preventing
    /// concurrent `script.list` calls from creating duplicate repo-config scripts.
    /// Shared across clones so all `ScriptManager` instances serialize bootstrap
    /// per workspace (modeled after `intent-git::WorktreeLocks`).
    script_bootstrap_locks: script_ops::WorkspaceScriptLocks,
    /// Secret persistence for **sensitive** settings (§9.8) — the secret-store
    /// seam behind `settings.*`. Defaults to the file-backed
    /// [`intent_core::FileSecretStore`] (`~/intent/secrets.json`); tests inject
    /// an in-memory store so they never touch the real secrets file.
    /// Wrapped in an [`AsyncSecretStore`](settings::AsyncSecretStore) so every
    /// backing call runs on the blocking pool with a bounded timeout + single-
    /// flight cache, keeping the async runtime free if the backing store stalls.
    secrets: Arc<settings::AsyncSecretStore>,
    /// Override for the **user** specialists directory (§18.2). `None` resolves
    /// to `~/.augment/specialists/`; tests inject a temp dir for hermetic
    /// 3-tier coverage.
    specialists_user_dir: Option<PathBuf>,
    /// Override for the **bundled** (read-only) specialists directory (§18.2).
    /// `None` resolves from `INTENTD_BUNDLED_SPECIALISTS_DIR` or the
    /// exe-relative `resources/specialists/`.
    specialists_bundled_dir: Option<PathBuf>,
    /// Runtime manager for **external** MCP servers (§18.3): spawns/stops/
    /// restarts stdio servers and runs the health monitor, pushing
    /// `mcp.servers:status-changed`. Shared across clones (and with the
    /// composition root, which spawns the monitor + reaps on shutdown).
    mcp_hub: Arc<McpHub>,
    /// Context engine backing `search.codebase` retrieval (§8). Defaults to the
    /// `auggie`-backed engine; `search.codebase` falls back to the ripgrep/symbol
    /// path when the engine is `Unavailable` (§8.3). Shared across clones.
    context_engine: Arc<dyn intent_context::ContextEngine>,
    /// Per-agent in-flight ("live") turn slots (CS-0 D5): the partial assistant
    /// message for an agent currently mid-`run_prompt_turn`, so a `chat.subscribe`
    /// arriving mid-turn can merge it into the seq-0 snapshot. Shared across
    /// clones so the [`AgentManager`]'s turn writer and the `WorkspaceApi` chat
    /// read door observe the same state; populated only while a turn streams.
    live_turns: agent_session::LiveTurns,
    /// Test-only override for [`WorkspaceApi::agent_is_busy`]: lets unit/UDS
    /// tests simulate an in-flight worker without spawning a real
    /// [`AgentManager`]. Production composition always attaches a manager and
    /// reads through it; this set is empty there. Shared across clones so a
    /// `set_test_busy` on one handle is visible to every other handle.
    test_busy: Arc<Mutex<HashSet<AgentId>>>,
    /// Per-note debouncers for `attribute_lines` recomputes (PROTOCOL §5.2.1,
    /// FE parity with `LineAttributionService.scheduleComputation`). Every
    /// content-changing `note.*` mutation schedules a delayed recompute
    /// (`LINE_ATTRIBUTION_DEBOUNCE`); a fresh mutation cancels the previous
    /// timer so bursts coalesce into one persist + one
    /// `line-attribution:updated` emit. Shared across clones so every
    /// service handle observes the same in-flight timers.
    line_attribution_debouncers:
        Arc<Mutex<HashMap<(WorkspaceId, NoteId), tokio::task::AbortHandle>>>,
    /// Session-only CRDT merge engine for note full-content writes (PROTOCOL
    /// `note.setContent` / `note.update` with content, §5.2). Ported from the
    /// reference `CRDTDocumentManager` / `CRDTNotesService` — a yrs `Doc` is
    /// seeded from the note's stored content on first touch and subsequent
    /// full-content writes apply a char-level diff inside a yrs transaction;
    /// the merged text is what the daemon persists, so concurrent writes
    /// converge instead of last-write-wins. Surgical `note.*` / `task.*`
    /// mutations invalidate the cached session so the next full-content write
    /// reseeds from disk. Shared across clones like the other in-memory
    /// registries.
    crdt_notes: Arc<crdt_notes::CrdtNoteManager>,
    /// REV-1 agent-initiated reverse-dispatch seam: when an agent calls
    /// `ws.browser.exec` via the MCP front door there is no ambient client
    /// connection to reverse-dispatch on, so [`WorkspaceApi::browser_exec`]
    /// routes through this handle (the transport crate wires a shared
    /// first-client-sticky registry). `None` when the composition root did
    /// not wire a registry (unit tests, read-only wiring) — `browser_exec`
    /// then short-circuits with `browser.exec: no client connected`
    /// (JSON-RPC `-32603`), matching the closed-channel failure mode the
    /// client-triggered `browser.exec` path already produces. Shared across
    /// clones so every service handle sees the same live-client set.
    reverse_dispatch: Option<Arc<dyn AgentReverseDispatch>>,
    /// Runtime control for the WSS listener (server settings apply
    /// hooks, §5.12). When wired, `settings.update` on `server.wsApi.enabled`
    /// starts/stops the listener at runtime.
    /// Held as `Arc<OnceLock>` so the control can be attached after the `api`
    /// Arc is built (composition-root wiring, §5.12). Shared across clones.
    server_control: Arc<OnceLock<Arc<dyn intent_core::ServerControl>>>,
}

impl Services {
    /// Wire the services surface over a persistence handle.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            assets_root: None,
            event_subscriptions: Arc::new(Mutex::new(HashSet::new())),
            event_bus: None,
            agent_queues: Arc::new(Mutex::new(HashMap::new())),
            session_stats_cache: Arc::new(Mutex::new(HashMap::new())),
            models_cache: Arc::new(Mutex::new(None)),
            auggie_bin: None,
            agent_subscriptions: Arc::new(Mutex::new(HashMap::new())),
            agent_manager: Arc::new(OnceLock::new()),
            source_control: None,
            linear_engine: None,
            sentry_engine: None,
            worktree_locks: intent_git::worktree::WorktreeLocks::new(),
            workspaces_root: None,
            search_cancels: intent_search::CancelRegistry::new(),
            agent_activity: Arc::new(Mutex::new(HashMap::new())),
            pty: Arc::new(intent_pty::PtyHost::new()),
            scripts: Arc::new(Mutex::new(HashMap::new())),
            script_bootstrap_locks: script_ops::WorkspaceScriptLocks::new(),
            secrets: Arc::new(settings::AsyncSecretStore::new(Arc::new(
                intent_core::FileSecretStore::new(),
            ))),
            specialists_user_dir: None,
            specialists_bundled_dir: None,
            mcp_hub: Arc::new(McpHub::new()),
            context_engine: Arc::new(intent_context::AuggieContextEngine::new()),
            live_turns: Arc::new(Mutex::new(HashMap::new())),
            test_busy: Arc::new(Mutex::new(HashSet::new())),
            line_attribution_debouncers: Arc::new(Mutex::new(HashMap::new())),
            crdt_notes: Arc::new(crdt_notes::CrdtNoteManager::new()),
            reverse_dispatch: None,
            server_control: Arc::new(OnceLock::new()),
        }
    }

    /// Override the context engine backing `search.codebase` (§8). The
    /// composition root keeps the `auggie`-backed default; tests inject a fake
    /// engine to exercise the engine-available and graceful-degradation paths
    /// (§8.3).
    pub fn with_context_engine(mut self, engine: Arc<dyn intent_context::ContextEngine>) -> Self {
        self.context_engine = engine;
        self
    }

    /// Override the [`SecretStore`] backing sensitive settings (§9.8). The
    /// composition root keeps the file-backed default
    /// ([`intent_core::FileSecretStore`]); tests inject an in-memory store so
    /// they never read/write the real secrets file. The injected store
    /// is wrapped in an [`AsyncSecretStore`](settings::AsyncSecretStore) so the
    /// same timeout / single-flight guarantees apply in tests.
    pub fn with_secret_store(mut self, secrets: Arc<dyn settings::SecretStore>) -> Self {
        self.secrets = Arc::new(settings::AsyncSecretStore::new(secrets));
        self
    }

    /// Override the **user** and **bundled** specialist directory roots (§18.2).
    /// The composition root keeps the env/HOME defaults; tests inject temp dirs
    /// so the 3-tier resolution is hermetic. The project tier always comes from
    /// each call's `workspacePath`.
    pub fn with_specialist_dirs(
        mut self,
        user_dir: Option<PathBuf>,
        bundled_dir: Option<PathBuf>,
    ) -> Self {
        self.specialists_user_dir = user_dir;
        self.specialists_bundled_dir = bundled_dir;
        self
    }

    /// Build a [`SpecialistsService`](specialists::SpecialistsService) view over
    /// the configured directory roots for one `specialist.*` call.
    fn specialists_service(&self) -> specialists::SpecialistsService {
        specialists::SpecialistsService::new(
            self.specialists_user_dir.clone(),
            self.specialists_bundled_dir.clone(),
        )
    }

    /// Resolve the default `agent_type` declared by a specialist's `agentType`
    /// frontmatter (§18.2 / SP-B). Used at spawn time to engage the matching
    /// internal tool denylist (§18.4) — e.g. the `ralph` specialist →
    /// `ralph-loop`. Returns `None` when the specialist is unknown or declares no
    /// `agentType`, leaving the caller's default agent type intact.
    pub(crate) fn specialist_agent_type(
        &self,
        specialist_id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<String> {
        self.specialists_service()
            .resolve_agent_type(specialist_id, workspace_path)
    }

    /// Resolve the `[Role Reminder: You are a {name}. {reminder}]` prefix to
    /// prepend to a specialist agent's next turn, or `None` when the agent has no
    /// specialist or its specialist yields no reminder (port of acp-provider.ts
    /// role-reminder injection). Rebuilt every turn (interval = 1); the reminder
    /// is added to the outbound provider prompt only and never persisted.
    pub(crate) async fn agent_role_reminder(&self, agent_id: &AgentId) -> Option<String> {
        let session = self.store.get_agent_session(agent_id).await.ok()?;
        let specialist_id = session.specialist.as_deref()?;
        let workspace_path = self
            .store
            .get_workspace(&session.workspace_id)
            .await
            .ok()
            .and_then(|w| w.path.or(w.worktree_path))
            .map(PathBuf::from);
        let (name, reminder) = self
            .specialists_service()
            .resolve_role_reminder(specialist_id, workspace_path.as_deref())?;
        Some(format!("[Role Reminder: You are a {name}. {reminder}]"))
    }

    /// Resolve the spawn-prompt specialist injection for an agent (PP-1): the
    /// behavior prompt (the session's persisted `metadata.behaviorPrompt`
    /// override wins over the specialist file's body — the TS
    /// `resolveSpecialistConfig` precedence) plus the specialist's display name
    /// and role reminder for the footer. `None` for non-specialist agents with
    /// no behavior-prompt override, leaving their prompt unchanged.
    pub(crate) async fn agent_specialist_injection(
        &self,
        agent_id: &AgentId,
        workspace_path: Option<&Path>,
    ) -> Option<crate::rules::SpecialistPromptInjection> {
        let session = self.store.get_agent_session(agent_id).await.ok()?;
        let override_bp = session
            .metadata
            .as_ref()
            .and_then(|m| m.get("behaviorPrompt"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let resolved = session.specialist.as_deref().and_then(|id| {
            self.specialists_service()
                .resolve_prompt_injection(id, workspace_path)
        });
        match (override_bp, resolved) {
            (bp_override, Some((bp_file, name, reminder))) => {
                Some(crate::rules::SpecialistPromptInjection {
                    behavior_prompt: bp_override.or(bp_file),
                    specialist_name: Some(name),
                    role_reminder: reminder,
                })
            }
            (Some(bp), None) => Some(crate::rules::SpecialistPromptInjection {
                behavior_prompt: Some(bp),
                specialist_name: None,
                role_reminder: None,
            }),
            (None, None) => None,
        }
    }

    /// Build a [`SettingsService`](settings::SettingsService) view over the store
    /// and secret store for one `settings.*` call. The secret store is cloned
    /// as an `Arc` so `SettingsService` can move it into `spawn_blocking` for
    /// non-blocking, timeout-guarded secret-store access.
    fn settings_service(&self) -> settings::SettingsService<'_> {
        settings::SettingsService::new(&self.store, &self.secrets)
    }

    /// Borrow the shared PTY host (composition root / ACP terminal-adapter use).
    pub fn pty(&self) -> Arc<intent_pty::PtyHost> {
        self.pty.clone()
    }

    /// Build a [`ScriptManager`](script_ops::ScriptManager) view over the shared
    /// PTY host, event bus, store, script registry, and bootstrap locks for one `script.*` call.
    fn script_manager(&self) -> script_ops::ScriptManager {
        script_ops::ScriptManager::new(
            self.pty.clone(),
            self.event_bus.clone(),
            self.store.clone(),
            self.scripts.clone(),
            self.script_bootstrap_locks.clone(),
        )
    }

    /// Hydrate the in-memory script registry from the persisted definitions
    /// (§5.8; FE `.workspace/scripts.json` parity). Called once by the
    /// composition root on boot so `script.*` survives daemon restarts; runtime
    /// state always starts fresh (idle). Returns the number of scripts loaded.
    pub async fn hydrate_scripts(&self) -> Result<usize> {
        self.script_manager().hydrate().await
    }

    /// Derive the read-only [`WorkspaceActivity`] for a workspace from the live
    /// in-flight agent count (§9.9): `AgentRunning` iff any session is in flight.
    pub(crate) fn workspace_activity(&self, workspace_id: &WorkspaceId) -> WorkspaceActivity {
        let map = self.agent_activity.lock().unwrap();
        match map.get(workspace_id) {
            Some(count) if *count > 0 => WorkspaceActivity::AgentRunning,
            _ => WorkspaceActivity::Idle,
        }
    }

    /// Derive `lastActivity` as the max of the persisted value, `updatedAt`,
    /// `createdAt`, every workspace note's `updated_at`, and every agent
    /// session's `updated_at` (FE `deriveWorkspaceLastActivity` parity, §9.1).
    /// Called on `workspace.*` mutation paths (update/archive/unarchive) that
    /// return a `Workspace` on the wire so clients never have to recompute it;
    /// the `workspace.list`/`workspace.get` read paths derive the same value
    /// inline as part of [`Services::enrich_workspace_aggregates`] to keep the
    /// aggregate scan single-pass. Keep the two in sync when the derivation
    /// rules change. Store failures fall back to the workspace's own
    /// timestamps rather than failing the caller.
    pub(crate) async fn derive_last_activity(&self, ws: &mut Workspace) {
        let mut activity_max = latest_activity_candidate(&[
            ws.last_activity.as_deref(),
            Some(ws.updated_at.as_str()),
            Some(ws.created_at.as_str()),
        ]);
        if let Ok(notes) = self.store.list_notes(&ws.id).await {
            for note in &notes {
                activity_max =
                    latest_activity_candidate(&[activity_max.as_deref(), Some(&note.updated_at)]);
            }
        }
        if let Ok(sessions) = self.store.list_agent_sessions(&ws.id).await {
            for session in &sessions {
                activity_max = latest_activity_candidate(&[
                    activity_max.as_deref(),
                    Some(&session.updated_at),
                ]);
            }
        }
        if activity_max.is_some() {
            ws.last_activity = activity_max;
        }
    }

    /// Populate a workspace's card aggregates (`taskStats`/`agentSummary`/
    /// `diffSummary`) for the `workspace.list` / `workspace.get` emit path (§9.1).
    /// Each is computed from live state (notes / agents / git worktree) and
    /// omitted when not computable; a read failure degrades to an absent
    /// aggregate rather than failing the whole call. `lastActivity` is derived
    /// inline from the same notes/sessions scan (mirrors
    /// [`Services::derive_last_activity`] so list/get callers get both in one
    /// round-trip); keep the two derivations in lock-step when the rules or
    /// underlying store queries change.
    pub(crate) async fn enrich_workspace_aggregates(&self, ws: &mut Workspace) {
        let mut activity_max = latest_activity_candidate(&[
            ws.last_activity.as_deref(),
            Some(ws.updated_at.as_str()),
            Some(ws.created_at.as_str()),
        ]);
        if let Ok(notes) = self.store.list_notes(&ws.id).await {
            for note in &notes {
                activity_max =
                    latest_activity_candidate(&[activity_max.as_deref(), Some(&note.updated_at)]);
            }
            ws.task_stats = Some(compute_task_stats(&notes));
        }
        if let Ok(sessions) = self.store.list_agent_sessions(&ws.id).await {
            for session in &sessions {
                activity_max = latest_activity_candidate(&[
                    activity_max.as_deref(),
                    Some(&session.updated_at),
                ]);
            }
            ws.agent_summary = Some(build_agent_summary(&sessions));
        }
        ws.diff_summary = compute_diff_summary(ws);
        if activity_max.is_some() {
            ws.last_activity = activity_max;
        }
        // Compute cow_supported (Task 5): direct mode + git repo + CoW probe Supported.
        ws.cow_supported = self.compute_cow_supported(ws);
    }

    /// Compute whether CoW agent isolation is supported for this workspace (Task 5).
    /// Returns Some(true) if direct mode + git repo + CoW probe Supported; Some(false)
    /// if worktree mode or unsupported filesystem; None if probe fails.
    fn compute_cow_supported(&self, ws: &Workspace) -> Option<bool> {
        // Only direct-mode workspaces
        let is_direct_mode = ws.skip_worktree || ws.worktree_path.is_none();
        if !is_direct_mode {
            return Some(false);
        }
        // Must have a repository path
        let repo_path = ws.repository_path.as_ref()?;
        // Must have workspaces root
        let workspaces_root = self.workspaces_root.as_ref()?;

        // Probe CoW support for the (repo, workspaces_root) pair
        match intent_git::cow_probe(std::path::Path::new(repo_path), workspaces_root) {
            Ok(intent_git::CowSupport::Supported) => Some(true),
            Ok(intent_git::CowSupport::Unsupported) => Some(false),
            Err(_) => None, // Probe failed; omit the field
        }
    }

    /// Record an agent session entering flight for `workspace_id`. On the
    /// `Idle → AgentRunning` transition (count `0 → 1`) emits a self-sufficient
    /// `workspace:activity-changed { workspaceId, activity }` (§10.1, only-on-change).
    pub(crate) async fn agent_activity_begin(&self, workspace_id: &WorkspaceId) {
        let transitioned = {
            let mut map = self.agent_activity.lock().unwrap();
            let count = map.entry(workspace_id.clone()).or_insert(0);
            *count += 1;
            *count == 1
        };
        if transitioned {
            publish_event(
                &self.event_bus,
                activity_changed_event(workspace_id, WorkspaceActivity::AgentRunning),
            )
            .await;
        }
    }

    /// Record an agent session leaving flight for `workspace_id`. On the
    /// `AgentRunning → Idle` transition (count `1 → 0`) emits a self-sufficient
    /// `workspace:activity-changed` (§10.1, only-on-change). A decrement with no
    /// tracked session is a no-op.
    pub(crate) async fn agent_activity_end(&self, workspace_id: &WorkspaceId) {
        let transitioned = {
            let mut map = self.agent_activity.lock().unwrap();
            match map.get_mut(workspace_id) {
                Some(count) if *count > 0 => {
                    *count -= 1;
                    if *count == 0 {
                        map.remove(workspace_id);
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            }
        };
        if transitioned {
            publish_event(
                &self.event_bus,
                activity_changed_event(workspace_id, WorkspaceActivity::Idle),
            )
            .await;
        }
    }

    /// Raise the server-owned `attention` flag (§9.9) — the BE side of the
    /// blue dot. Persists `attention = level` and emits a self-sufficient
    /// `workspace:attention-changed` only when the value actually changes
    /// (§10.1). Best-effort: a missing workspace surfaces as an error.
    pub(crate) async fn raise_attention(
        &self,
        workspace_id: &WorkspaceId,
        level: WorkspaceAttention,
    ) -> Result<()> {
        let mut ws = self.store.get_workspace(workspace_id).await?;
        if ws.attention == level {
            return Ok(());
        }
        ws.attention = level;
        ws.updated_at = now_iso();
        self.store.update_workspace(&ws).await?;
        publish_event(
            &self.event_bus,
            attention_changed_event(&ws.id, ws.attention),
        )
        .await;
        Ok(())
    }

    /// Wire the active source-control provider used by the `pr.*` methods (§7).
    /// The composition root builds it from settings; tests inject a stub.
    pub fn with_source_control(
        mut self,
        source_control: Arc<dyn intent_sourcecontrol::SourceControl>,
    ) -> Self {
        self.source_control = Some(source_control);
        self
    }

    /// Wire the active Linear engine used by the `linear.*` methods (§5.28).
    /// The composition root builds it from settings; tests inject a stub so the
    /// `linear.*` handlers never touch the network.
    pub fn with_linear_engine(
        mut self,
        linear_engine: Arc<dyn intent_linear::LinearEngine>,
    ) -> Self {
        self.linear_engine = Some(linear_engine);
        self
    }

    /// Wire the active Sentry engine used by the `sentry.*` methods (§5.29).
    /// The composition root builds it from settings; tests inject a stub so the
    /// `sentry.*` handlers never touch the network.
    pub fn with_sentry_engine(
        mut self,
        sentry_engine: Arc<dyn intent_sentry::SentryEngine>,
    ) -> Self {
        self.sentry_engine = Some(sentry_engine);
        self
    }

    /// Attach the runtime [`AgentManager`] so the `agent.*` RPC handlers drive
    /// the live spawn/turn/MCP loop (§6.8). Stores a [`Weak`] (the composition
    /// root owns the strong handle). The `OnceLock` is shared across clones, so
    /// every clone of this handle — including the one the manager holds — sees
    /// the manager once attached. Idempotent: a second call is a no-op.
    pub fn attach_agent_manager(&self, manager: &Arc<AgentManager>) {
        let _ = self.agent_manager.set(Arc::downgrade(manager));
    }

    /// Upgrade the attached [`AgentManager`], or `None` when unattached or the
    /// manager has been dropped (read-only/test wiring).
    pub(crate) fn agent_manager(&self) -> Option<Arc<AgentManager>> {
        self.agent_manager.get().and_then(Weak::upgrade)
    }

    /// Test-only seam: mark `agent_id` as busy (or clear it) so
    /// [`WorkspaceApi::agent_is_busy`] reports `true` without spawning a real
    /// [`AgentManager`]. Production composition always attaches a manager and
    /// never touches this; the seam exists so transport-level chat-snapshot
    /// tests can simulate a mid-turn alongside [`Services::set_live_turn`].
    #[doc(hidden)]
    pub fn set_test_busy(&self, agent_id: &AgentId, busy: bool) {
        if let Ok(mut set) = self.test_busy.lock() {
            if busy {
                set.insert(agent_id.clone());
            } else {
                set.remove(agent_id);
            }
        }
    }

    /// Daemon-startup heal for stale in-flight conversations: sessions left in
    /// an "active" status (`Active`, `Processing`, `Waiting`) across a crash or
    /// hard shutdown have no live worker after restart — the
    /// [`AgentManager`]'s in-memory busy set and the per-agent live-turn slots
    /// start empty — but their persisted [`AgentStatus`] would otherwise drive
    /// the FE's `isActiveAgentThread` selector to true and surface a phantom
    /// "Thinking" indicator. This sweep walks every session and rewrites those
    /// statuses to [`AgentStatus::RuntimeIdle`] (the modern lowercase `"idle"`,
    /// non-terminal and non-active), so opening a chat after restart shows a
    /// settled conversation. Pending sessions and any already-terminal status
    /// (`Completed`/`Error`/`Deleted`) are left untouched. Returns the number
    /// of sessions healed. Errors are surfaced so the composition root can log
    /// and continue: heal is best-effort and never gates `serve`.
    pub async fn heal_stale_agent_sessions(&self) -> Result<usize> {
        let sessions = self.store.list_all_agent_sessions().await?;
        let mut healed = 0usize;
        for session in sessions {
            if !is_stale_in_flight_status(session.status) {
                continue;
            }
            let prev = session.status;
            // Narrow write: `set_agent_session_status` persists only
            // (status, is_active, updated_at) without loading the full
            // message log, keeping the startup sweep O(rows). The write-once
            // `acpSessionId` and immutable `provider` invariants (§9.5) are
            // untouched because those columns are not written here.
            self.store
                .set_agent_session_status(
                    &session.workspace_id,
                    &session.id,
                    intent_core::AgentStatus::RuntimeIdle,
                    false,
                    &now_iso(),
                )
                .await?;
            healed += 1;
            tracing::info!(
                agent_id = %session.id,
                workspace_id = %session.workspace_id,
                from = ?prev,
                "healed stale in-flight agent session"
            );
        }
        Ok(healed)
    }

    /// Configure the note-asset root directory (for `note.readAsset`).
    /// Resolve `noteIds` (PROTOCOL §5.5) to ACP `image` content-block
    /// payloads. Port of the FE extraction in
    /// `agent-backend-handler.service.ts`: for each note, scan the raw
    /// markdown for `![alt](url)` images whose URL is a
    /// `workspace-asset://<workspaceId>/<assetId>` reference in the
    /// requesting workspace, load the asset bytes from disk, and return a
    /// list of `{ data: base64, mimeType }` tuples in document order.
    /// Missing notes, missing assets, and non-workspace-asset URLs are
    /// silently skipped so a partial `noteIds` array never breaks the
    /// turn. External HTTP(S) image URLs are NOT fetched here — that path
    /// is deferred (the FE variant would fetch via `fetch()`).
    pub(crate) async fn load_note_image_blocks(
        &self,
        workspace_id: &WorkspaceId,
        note_ids: &[String],
    ) -> Vec<(String, String)> {
        let Some(root) = self.assets_root.clone() else {
            return Vec::new();
        };
        let mut out: Vec<(String, String)> = Vec::new();
        for note_id in note_ids {
            let note = match self
                .store
                .get_note(workspace_id, &NoteId::from(note_id.as_str()))
                .await
            {
                Ok(n) => n,
                _ => continue,
            };
            for url in extract_markdown_image_urls(&note.content) {
                if !url.starts_with("workspace-asset://") {
                    continue;
                }
                let rest = &url["workspace-asset://".len()..];
                let Some((ws_from_url, asset_id)) = rest.split_once('/') else {
                    continue;
                };
                if ws_from_url != workspace_id.0 || asset_id.is_empty() {
                    continue;
                }
                let asset_path = root.join(&workspace_id.0).join(asset_id);
                let bytes = match tokio::fs::read(&asset_path).await {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(
                            note = %note_id, asset = %asset_id, error = %e,
                            "failed to read note-referenced asset"
                        );
                        continue;
                    }
                };
                let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
                let mime = crate::note_ops::mime_from_extension(asset_id);
                out.push((data, mime));
            }
        }
        out
    }

    pub fn with_assets_root(mut self, root: PathBuf) -> Self {
        self.assets_root = Some(root);
        self
    }

    /// Override the root directory `workspace.create` provisions git worktrees
    /// under. The composition root keeps the default (`$INTENTD_WORKSPACES_DIR`,
    /// else `~/intent/workspaces`); tests inject a temp dir.
    pub fn with_workspaces_root(mut self, root: PathBuf) -> Self {
        self.workspaces_root = Some(root);
        self
    }

    /// Override the auggie binary the one-shot CLI RPCs (`agent.enhancePrompt`
    /// §5.31, `agent.completeOnce` §5.32) spawn. The composition root keeps
    /// the discovery default (`find_auggie`); tests inject a fixture script
    /// so the one-shot CLI path is deterministic.
    pub fn with_auggie_bin(mut self, bin: PathBuf) -> Self {
        self.auggie_bin = Some(bin);
        self
    }

    /// Wire the event bus so CRUD mutations publish change events (§10). The bus
    /// must share the same [`Store`] as this services handle so the broadcast and
    /// the durable log stay consistent.
    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        // The MCP hub publishes `mcp.servers:status-changed` onto the same bus.
        self.mcp_hub.set_event_bus(bus.clone());
        self.event_bus = Some(bus);
        self
    }

    /// REV-1: wire the agent-initiated reverse-dispatch seam so
    /// [`WorkspaceApi::browser_exec`] can forward to the shared
    /// first-client-sticky registry the transport listeners populate. The
    /// composition root builds one [`intent_transport::PrimaryReverseRegistry`]
    /// and hands it to both this method and every listener; test / read-only
    /// wiring leaves it unset and `browser_exec` surfaces `no client connected`.
    pub fn with_reverse_dispatch(mut self, dispatch: Arc<dyn AgentReverseDispatch>) -> Self {
        self.reverse_dispatch = Some(dispatch);
        self
    }

    /// Attach the runtime [`ServerControl`] so `settings.update` can start/stop
    /// the WSS listener on `server.wsApi.enabled` changes (server settings
    /// apply hooks, §5.12).
    /// Idempotent: a second call is a no-op (the `OnceLock` keeps the first).
    pub fn attach_server_control(&self, control: Arc<dyn intent_core::ServerControl>) {
        let _ = self.server_control.set(control);
    }

    /// Borrow the shared [`McpHub`] (composition root: spawn the health monitor
    /// + reap external MCP servers on shutdown, §18.3).
    pub fn mcp_hub(&self) -> Arc<McpHub> {
        self.mcp_hub.clone()
    }

    /// Start every enabled, non-disabled external MCP server on daemon boot
    /// (§18.3). Best-effort; a failed spawn surfaces as an `error` status event.
    pub async fn start_enabled_mcp_servers(&self) {
        self.mcp_servers_service().start_enabled().await;
    }

    /// Build an [`McpServersService`](mcp_servers::McpServersService) view over the
    /// store, secret store, and hub for one `mcp.servers.*` call.
    fn mcp_servers_service(&self) -> mcp_servers::McpServersService<'_> {
        mcp_servers::McpServersService::new(&self.store, &self.secrets, &self.mcp_hub)
    }

    /// Build an [`McpOauthService`](mcp_oauth::McpOauthService) view over the
    /// store for one `mcp.oauth.*` call. Per-server OAuth token bags live in
    /// their own `mcp_oauth_tokens` table (§9.4); wire reads are presence-only,
    /// so no secret store or hub dependency is needed here.
    fn mcp_oauth_service(&self) -> mcp_oauth::McpOauthService<'_> {
        mcp_oauth::McpOauthService::new(&self.store)
    }

    /// Borrow the underlying store (composition-root / diagnostics use).
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Refresh one workspace's PR linkage against the forge (§7.6), persisting
    /// any change and emitting the matching `pr:*` event. Used both on demand
    /// and by the background loop. The matching rule is `pr.head.ref ==
    /// workspace.branch` (NOT baseRef). When the workspace is already linked the
    /// PR is re-fetched and its snapshot diffed (clearing a stale link on a
    /// positive branch mismatch); when unlinked, an open PR whose head ref
    /// equals the branch is discovered and linked. Remote/archived workspaces,
    /// and those lacking repo/branch info, are skipped. A missing forge token
    /// (no injected/registry provider) surfaces as `Internal`.
    pub async fn refresh_workspace_pr(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<pr_ops::PrRefreshOutcome> {
        // Check eligibility before resolving provider (PRRT_kwDOS9Wxuc6QZ0zr):
        // avoids errors/warnings for remote/archived/ineligible workspaces when
        // source control is unconfigured.
        use pr_ops::PrRefreshOutcome;
        let ws = self.store.get_workspace(workspace_id).await?;
        if ws.is_remote || ws.archived {
            return Ok(PrRefreshOutcome::Skipped);
        }
        if pr_ops::repo_of(&ws).is_err() {
            return Ok(PrRefreshOutcome::Skipped);
        }

        let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
        self.refresh_workspace_pr_with_sc(workspace_id, &sc).await
    }

    /// Refresh one workspace's PR linkage with a pre-resolved [`SourceControl`]
    /// provider (§7.6). Same semantics as [`refresh_workspace_pr`], but avoids
    /// re-resolving the provider on every call — used by the background sweep to
    /// resolve once per cycle and reuse across all workspaces, avoiding spamming
    /// warnings when unconfigured.
    async fn refresh_workspace_pr_with_sc(
        &self,
        workspace_id: &WorkspaceId,
        sc: &Arc<dyn intent_sourcecontrol::SourceControl>,
    ) -> Result<pr_ops::PrRefreshOutcome> {
        use pr_ops::PrRefreshOutcome;

        let mut ws = self.store.get_workspace(workspace_id).await?;
        if ws.is_remote || ws.archived {
            return Ok(PrRefreshOutcome::Skipped);
        }
        let (owner, repo) = match pr_ops::repo_of(&ws) {
            Ok(pair) => pair,
            Err(_) => return Ok(PrRefreshOutcome::Skipped),
        };
        let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);

        match ws.pr_number {
            Some(number) => {
                let pr = sc
                    .get_pr(&repo_ref, number)
                    .await
                    .map_err(pr_ops::map_sc_err)?;
                // Clear a stale link only on a positive branch mismatch.
                if pr_ops::pr_branch_mismatch(&pr, &ws.branch) {
                    ws.pr_number = None;
                    ws.pr_url = None;
                    ws.pr_status = None;
                    ws.active_pull_request = None;
                    ws.updated_at = now_iso();
                    self.store.update_workspace(&ws).await?;
                    publish_event(&self.event_bus, pr_unlinked_event(workspace_id)).await;
                    return Ok(PrRefreshOutcome::Unlinked);
                }
                let info = pr_ops::build_pr_info(&pr);
                let changed = ws.pr_status != Some(info.status)
                    || ws.active_pull_request.as_ref() != Some(&info)
                    || ws.pr_url.as_deref() != Some(pr.url.as_str());
                if !changed {
                    return Ok(PrRefreshOutcome::Unchanged);
                }
                ws.pr_status = Some(info.status);
                ws.pr_url = Some(pr.url.clone());
                ws.active_pull_request = Some(info);
                ws.updated_at = now_iso();
                self.store.update_workspace(&ws).await?;
                publish_event(&self.event_bus, pr_updated_event(&ws)).await;
                Ok(PrRefreshOutcome::Updated)
            }
            None => {
                // Discovery: link an open PR whose head ref equals the branch.
                if ws.branch.is_empty() {
                    return Ok(PrRefreshOutcome::Skipped);
                }
                let query = intent_sourcecontrol::PrQuery {
                    state: Some(intent_sourcecontrol::PrState::Open),
                    head: Some(ws.branch.clone()),
                    ..Default::default()
                };
                let prs = sc
                    .list_prs(&repo_ref, query)
                    .await
                    .map_err(pr_ops::map_sc_err)?
                    .items;
                match prs
                    .into_iter()
                    .find(|p| pr_ops::pr_matches_branch(p, &ws.branch))
                {
                    Some(pr) => {
                        let info = pr_ops::build_pr_info(&pr);
                        ws.pr_number = Some(pr.number);
                        ws.pr_url = Some(pr.url.clone());
                        ws.pr_status = Some(info.status);
                        ws.active_pull_request = Some(info);
                        ws.updated_at = now_iso();
                        self.store.update_workspace(&ws).await?;
                        publish_event(&self.event_bus, pr_linked_event(&ws)).await;
                        Ok(PrRefreshOutcome::Linked)
                    }
                    None => Ok(PrRefreshOutcome::Unchanged),
                }
            }
        }
    }

    /// Refresh every active workspace's PR linkage: existing links are
    /// re-fetched (persisting deltas + emitting `pr:updated`/`pr:unlinked`), and
    /// unlinked workspaces discover a matching open PR by head ref (branch-only
    /// matching per §7.6 — `pr.head.ref == workspace.branch`), persisting the
    /// link + emitting `pr:linked` on first match. Remote/archived workspaces and
    /// those lacking repo/branch info are skipped. Errors are logged per
    /// workspace and never abort the sweep.
    async fn refresh_all_workspace_prs(&self) {
        // Resolve the SourceControl provider once per sweep to avoid spamming
        // warnings when unconfigured and hitting keychain/gh on the blocking pool
        // once per workspace (Copilot review comment on PR #131).
        let sc = match pr_ops::resolve_source_control(self.source_control.clone()).await {
            Ok(sc) => sc,
            Err(e) => {
                tracing::warn!(error = %e, "pr refresh: source control unavailable, skipping sweep");
                return;
            }
        };
        let workspaces = match self.store.list_workspaces(false).await {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!(error = %e, "pr refresh: listing workspaces failed");
                return;
            }
        };
        for ws in workspaces {
            // STAB-3 fix: refresh all workspaces (discovery + update), not just
            // those already linked. `refresh_workspace_pr_with_sc` skips
            // ineligible workspaces internally.
            if let Err(e) = self.refresh_workspace_pr_with_sc(&ws.id, &sc).await {
                tracing::warn!(
                    workspace = %ws.id.as_str(),
                    error = %e,
                    "pr refresh: workspace refresh failed"
                );
            }
        }
    }

    /// Spawn the background PR refresh loop (§7.6): every `interval` it refreshes
    /// all active workspaces — discovering open PRs by head-ref match for
    /// unlinked workspaces (emitting `pr:linked`) and updating linked PRs
    /// (emitting `pr:updated`/`pr:unlinked`). The first sweep runs after one
    /// `interval`. Missed ticks are skipped (no pile-up). No-op-safe when source
    /// control is unconfigured (the sweep logs a single warning and returns).
    /// Returns the task handle so the composition root can hold/abort it.
    pub fn spawn_pr_refresh_loop(
        &self,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Consume the immediate first tick so the loop waits one interval.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                services.refresh_all_workspace_prs().await;
            }
        })
    }

    /// Spawn the CRDT session-sweeper loop (A5, reference parity with the
    /// `CRDTNotesService` idle sweep): every [`crdt_notes::SESSION_SWEEP_INTERVAL`]
    /// tick, drop cached `yrs` docs whose last access is older than
    /// [`crdt_notes::SESSION_IDLE_TIMEOUT`] so long-lived daemons do not hold
    /// per-note session state indefinitely. The first sweep runs after one
    /// interval; missed ticks are skipped (no pile-up). Returns the task handle
    /// so the composition root can hold/abort it.
    pub fn spawn_crdt_session_sweep_loop(&self) -> tokio::task::JoinHandle<()> {
        let crdt_notes = self.crdt_notes.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(crdt_notes::SESSION_SWEEP_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Consume the immediate first tick so the loop waits one interval.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let removed = crdt_notes.sweep_stale(crdt_notes::SESSION_IDLE_TIMEOUT);
                if removed > 0 {
                    tracing::info!(removed, "crdt session sweep evicted stale sessions");
                }
            }
        })
    }

    /// Recompute one workspace's durable `tokenUsage` by tallying its agent
    /// sessions per agent and per model (§5.23 / §19.1), persisting the snapshot
    /// and emitting `workspace:tokenUsage-changed` only when the materialized
    /// tally (ignoring `lastScanAt`) actually changed. Daemon-internal — there is
    /// no scan RPC. Returns whether a change was written. `NotFound` if the
    /// workspace is absent.
    pub async fn scan_workspace_token_usage(&self, workspace_id: &WorkspaceId) -> Result<bool> {
        let sessions = self.store.list_agent_sessions(workspace_id).await?;
        let tallies: Vec<token_usage::AgentTokenTally> = sessions
            .iter()
            .map(token_usage::session_token_tally)
            .collect();
        let mut usage = token_usage::aggregate_token_usage(&tallies);
        usage.last_scan_at = Some(now_iso());

        let mut ws = self.store.get_workspace(workspace_id).await?;
        let changed = match &ws.token_usage {
            Some(prev) => {
                prev.by_agent_id != usage.by_agent_id
                    || prev.by_model != usage.by_model
                    || prev.totals != usage.totals
            }
            None => true,
        };
        if !changed {
            return Ok(false);
        }
        ws.token_usage = Some(usage.clone());
        ws.updated_at = now_iso();
        self.store.update_workspace(&ws).await?;
        publish_event(
            &self.event_bus,
            token_usage_changed_event(workspace_id, &usage),
        )
        .await;
        Ok(true)
    }

    /// Re-scan token usage for every non-archived workspace. Errors are logged
    /// per workspace and never abort the sweep.
    async fn scan_all_token_usage(&self) {
        let workspaces = match self.store.list_workspaces(false).await {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!(error = %e, "token usage scan: listing workspaces failed");
                return;
            }
        };
        for ws in workspaces {
            if let Err(e) = self.scan_workspace_token_usage(&ws.id).await {
                tracing::warn!(
                    workspace = %ws.id.as_str(),
                    error = %e,
                    "token usage scan: workspace scan failed"
                );
            }
        }
    }

    /// Spawn the daemon-internal periodic token-usage scan loop (§5.23 / §19.1):
    /// every `interval` it re-tallies each workspace's usage, persisting deltas
    /// and pushing `workspace:tokenUsage-changed`. The first sweep runs after one
    /// `interval`; missed ticks are skipped (no pile-up). Returns the task handle
    /// so the composition root can hold/abort it.
    pub fn spawn_token_usage_scan_loop(
        &self,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Consume the immediate first tick so the loop waits one interval.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                services.scan_all_token_usage().await;
            }
        })
    }

    /// Spawn the AS-3 completion-delivery worker: subscribe to the AGENT
    /// completion event set (agent:idle / agent:failed / agent:deleted) across
    /// every workspace and, on each child completion, wake every parent holding
    /// a oneShot completion watch for that child (the same agent_send_message_op
    /// path reportToParent uses), removing the oneShot watch after delivery.
    /// after_all delegation-group watches (group_id = Some) are left in place for
    /// AS-4. No-op-safe when no event bus is wired. Returns the task handle so the
    /// composition root can hold it for the process lifetime.
    pub fn spawn_completion_delivery_loop(&self) -> tokio::task::JoinHandle<()> {
        let Some(bus) = self.event_bus.clone() else {
            tracing::info!("completion delivery loop disabled: no event bus");
            return tokio::spawn(async {});
        };
        let services = self.clone();
        tokio::spawn(async move {
            // Span every workspace (workspace_id = None) and deliver each matched
            // event immediately (batch_window = None) so wakes are never coalesced.
            let filter = SubscriptionFilter {
                event_types: vec![
                    AGENT_IDLE.to_string(),
                    AGENT_FAILED.to_string(),
                    AGENT_DELETED.to_string(),
                ],
                ..Default::default()
            };
            let mut sub = bus.subscribe(filter);
            while let Some(events) = sub.recv().await {
                for event in events {
                    services.handle_completion_event(&event).await;
                }
            }
        })
    }

    /// Resolve the completed child + workspace from a completion event and fan
    /// the wake out to matching watches. A malformed event (no child id) is
    /// logged and skipped so the worker loop never panics.
    pub(crate) async fn handle_completion_event(&self, event: &Event) {
        let Some(child_id) = completion_event_child_id(event) else {
            tracing::warn!(
                event_type = %event.event_type,
                "completion event missing child agent id; skipping"
            );
            return;
        };
        let child = AgentId::from(child_id.as_str());

        // BEFORE waking the coordinator: if this is a sandboxed agent completion,
        // attempt the merge-back. On clean merge, proceed normally. On conflict,
        // suppress completion propagation and bounce the agent. On blocked or
        // retry-exhausted, propagate with merge-pending status.
        if event.event_type == AGENT_IDLE {
            if let Ok(session) = self.store.get_agent_session(&child).await {
                if session.sandbox_path.is_some() {
                    let should_propagate = self
                        .handle_sandbox_merge_on_completion(&event.workspace_id, &child, &session)
                        .await;
                    if !should_propagate {
                        // Conflict bounce: agent was woken with instructions; do NOT
                        // wake coordinator yet. The agent's next completion will retry.
                        return;
                    }
                }
            }
        }

        self.deliver_completion_to_watches(&event.workspace_id, &child, event)
            .await;
        // An agent going idle ends its delegating turn, so seal that parent's
        // open after_all group (the expected set is now final) and try to fire it
        // — covers the case where every child finished before the parent idled.
        if event.event_type == AGENT_IDLE {
            if let Some(gid) = self.seal_group_for_parent(&event.workspace_id, &child) {
                self.try_fire_group(&event.workspace_id, &gid).await;
            }
        }
    }

    /// Wake every parent whose oneShot watch matches child_id, then drop that
    /// watch. group_id = Some watches defer to the AS-4 delegation-group fan-in
    /// and are left untouched. A single failed delivery is logged and skipped so
    /// the remaining watches still fire. Wave B: remove oneShot watches BEFORE
    /// delivery to prevent duplicate wakes if the same event is reprocessed or
    /// the delivery loop is reentrant.
    pub(crate) async fn deliver_completion_to_watches(
        &self,
        workspace_id: &WorkspaceId,
        child_id: &AgentId,
        event: &Event,
    ) {
        for watch in self.find_watches_for_child(workspace_id, child_id) {
            if let Some(gid) = watch.group_id.clone() {
                // Route the child's completion into the parent's after_all
                // delegation group instead of waking immediately. The group's own
                // fire path removes these watches once it settles (AS-4).
                let deleted = event.event_type == AGENT_DELETED;
                // Prefer the child's persisted completionReport (set by
                // `agent.reportToParent`, whose immediate send is suppressed for
                // grouped children) over the event's lastResponseSummary,
                // mirroring the TS event-notification formatter.
                let report = self
                    .store
                    .get_agent_session(child_id)
                    .await
                    .ok()
                    .and_then(|s| s.completion_report);
                let summary = format_group_child_line(child_id, event, report.as_deref());
                self.record_group_child_completion(
                    workspace_id,
                    &gid,
                    child_id,
                    deleted,
                    summary,
                    event.clone(),
                );
                self.try_fire_group(workspace_id, &gid).await;
                continue;
            }
            // Wave B (STAB-18 fix): remove oneShot watch BEFORE delivery so a
            // reprocessed event or reentrant loop cannot deliver the same
            // completion twice. The watch is atomically removed from the registry;
            // if delivery fails the parent misses the wake, but that's safer than
            // duplicate delivery (which we observed in production: STAB-5). Group
            // watches are still removed AFTER group settlement as before.
            if watch.one_shot {
                let removed = self.remove_watch(workspace_id, &watch.id);
                if !removed {
                    // Watch was concurrently removed (e.g. by another event or
                    // cancelSubscriptions); skip delivery to avoid a duplicate.
                    tracing::debug!(
                        child = %child_id.0,
                        parent = %watch.parent_agent_id.0,
                        "oneShot watch already removed, skipping delivery"
                    );
                    continue;
                }
            }
            let wake = format_completion_wake(child_id, event);
            let metadata = build_event_notification_metadata(&[event]);
            if let Err(e) = self
                .deliver_parent_wake(
                    workspace_id,
                    watch.parent_agent_id.clone(),
                    wake,
                    Some(metadata),
                )
                .await
            {
                tracing::warn!(
                    error = %e,
                    parent = %watch.parent_agent_id.0,
                    "failed to deliver completion wake to parent"
                );
                continue;
            }
            if watch.one_shot {
                self.publish_subscriptions_changed(workspace_id, &watch.parent_agent_id)
                    .await;
            }
        }
    }

    /// Fire a delegation group's single aggregated wake if it is ready (sealed,
    /// complete, undelivered). `take_group_if_ready` flips `delivered` and removes
    /// the group atomically, so this fires at most once even under concurrent
    /// completions; on a send error we log and accept the dropped wake (mirroring
    /// the immediate path's best-effort delivery).
    pub(crate) async fn try_fire_group(&self, workspace_id: &WorkspaceId, group_id: &str) {
        let Some(group) = self.take_group_if_ready(workspace_id, group_id) else {
            return;
        };
        let wake = format_group_wake(&group);
        let event_refs: Vec<&Event> = group.raw_events.iter().map(|e| e.as_ref()).collect();
        let metadata = build_event_notification_metadata(&event_refs);
        if let Err(e) = self
            .deliver_parent_wake(
                workspace_id,
                group.parent_agent_id.clone(),
                wake,
                Some(metadata),
            )
            .await
        {
            tracing::warn!(
                error = %e,
                parent = %group.parent_agent_id.0,
                group = %group_id,
                "failed to deliver aggregated after_all wake to parent"
            );
        }
        self.remove_group_watches(workspace_id, group_id);
        self.publish_subscriptions_changed(workspace_id, &group.parent_agent_id)
            .await;
    }

    /// Handle sandbox merge-back on agent completion.
    /// Returns `true` if completion should propagate normally (clean merge or blocked/retry-exhausted).
    /// Returns `false` if completion was suppressed (conflict bounce).
    async fn handle_sandbox_merge_on_completion(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        session: &AgentSession,
    ) -> bool {
        use crate::sandbox_ops::{merge_sandbox, MergeOutcome};
        use intent_store::SandboxStatus;

        // Load sandbox record
        let _sandbox = match self.store.get_sandbox(workspace_id, agent_id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    "agent has sandbox_path but no sandbox record; skipping merge"
                );
                return true;
            }
            Err(e) => {
                tracing::error!(
                    agent = %agent_id.0,
                    error = %e,
                    "failed to load sandbox record; propagating completion"
                );
                return true;
            }
        };

        // Check retry count (cap at 2 bounces)
        const MAX_RETRIES: i64 = 2;
        let retry_count = self.get_sandbox_retry_count(workspace_id, agent_id).await;

        // Update sandbox status to Merging
        let now = now_iso();
        if let Err(e) = self
            .store
            .update_sandbox_status(workspace_id, agent_id, SandboxStatus::Merging, &now)
            .await
        {
            tracing::error!(
                agent = %agent_id.0,
                error = %e,
                "failed to update sandbox status to merging"
            );
        }

        // Attempt merge
        let outcome = match merge_sandbox(&self.store, workspace_id, agent_id).await {
            Ok(outcome) => outcome,
            Err(e) => {
                tracing::error!(
                    agent = %agent_id.0,
                    error = %e,
                    "sandbox merge failed with error; marking merge-pending"
                );
                let _ = self
                    .store
                    .update_sandbox_status(
                        workspace_id,
                        agent_id,
                        SandboxStatus::MergePending,
                        &now_iso(),
                    )
                    .await;
                // Propagate completion with error status
                return true;
            }
        };

        match outcome {
            MergeOutcome::Merged {
                commit_range,
                canonical_head,
            } => {
                // Success! Mark merged, discard sandbox, emit event
                let _ = self
                    .store
                    .update_sandbox_status(
                        workspace_id,
                        agent_id,
                        SandboxStatus::Merged,
                        &now_iso(),
                    )
                    .await;

                // Discard the sandbox
                if let Err(e) =
                    crate::sandbox_ops::discard_sandbox(&self.store, workspace_id, agent_id).await
                {
                    tracing::warn!(
                        agent = %agent_id.0,
                        error = %e,
                        "failed to discard sandbox after successful merge"
                    );
                }

                // Emit sandbox:merged event
                let event = NewEvent {
                    workspace_id: workspace_id.clone(),
                    timestamp: now_iso(),
                    event_type: "sandbox:merged".to_string(),
                    actor: intent_core::EventActor {
                        actor_type: ActorType::System,
                        id: Some("intentd".to_string()),
                        name: Some("intentd".to_string()),
                        ..Default::default()
                    },
                    session_id: Some(agent_id.0.clone()),
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data: serde_json::json!({
                        "workspaceId": workspace_id.0,
                        "agentId": agent_id.0,
                        "commitRange": commit_range,
                        "canonicalHead": canonical_head,
                    }),
                };
                crate::publish_event(&self.event_bus, event).await;

                tracing::info!(
                    agent = %agent_id.0,
                    range = %commit_range,
                    "sandbox merged successfully"
                );

                // Clear retry count
                self.clear_sandbox_retry_count(workspace_id, agent_id).await;

                // Propagate completion normally
                true
            }
            MergeOutcome::Conflict {
                conflicting_paths,
                canonical_head,
            } => {
                // Conflict: check retry limit
                if retry_count >= MAX_RETRIES {
                    // Exhausted retries; mark merge-pending and propagate
                    let _ = self
                        .store
                        .update_sandbox_status(
                            workspace_id,
                            agent_id,
                            SandboxStatus::MergePending,
                            &now_iso(),
                        )
                        .await;

                    tracing::warn!(
                        agent = %agent_id.0,
                        retries = retry_count,
                        "conflict retry limit exhausted; marking merge-pending"
                    );

                    self.clear_sandbox_retry_count(workspace_id, agent_id).await;
                    return true;
                }

                // Bounce: update status, increment retry, fetch canonical, wake agent
                let _ = self
                    .store
                    .update_sandbox_status(
                        workspace_id,
                        agent_id,
                        SandboxStatus::ConflictBounced,
                        &now_iso(),
                    )
                    .await;

                self.increment_sandbox_retry_count(workspace_id, agent_id)
                    .await;

                // Fetch canonical state into sandbox
                if let Err(e) = self
                    .fetch_canonical_to_sandbox(
                        workspace_id,
                        agent_id,
                        session.sandbox_path.as_ref().unwrap(),
                        &canonical_head,
                    )
                    .await
                {
                    tracing::error!(
                        agent = %agent_id.0,
                        error = %e,
                        "failed to fetch canonical to sandbox; marking merge-pending"
                    );
                    let _ = self
                        .store
                        .update_sandbox_status(
                            workspace_id,
                            agent_id,
                            SandboxStatus::MergePending,
                            &now_iso(),
                        )
                        .await;
                    return true;
                }

                // Wake agent with conflict instructions
                let message = format!(
                    "⚠️ Merge conflict detected. Your sandbox branch has conflicts with the canonical repository.\n\n\
                    **Conflicting files:**\n{}\n\n\
                    **Instructions:**\n\
                    1. The canonical state has been fetched to your sandbox as `refs/remotes/canonical/HEAD`\n\
                    2. Resolve conflicts by rebasing or merging: `git rebase canonical/HEAD` or `git merge canonical/HEAD`\n\
                    3. Fix conflicts in the listed files\n\
                    4. Commit the resolution: `git add <files> && git commit`\n\
                    5. **Do not switch branches or touch other checkouts**\n\
                    6. End your turn when ready - the system will retry the merge\n\n\
                    You are working in an isolated sandbox at: `{}`\n\
                    Retry {}/{} - fix conflicts in your sandbox only.",
                    conflicting_paths.join("\n- "),
                    session.sandbox_path.as_ref().unwrap(),
                    retry_count + 1,
                    MAX_RETRIES
                );

                if let Err(e) = self
                    .agent_send_message_op(
                        agent_id.clone(),
                        message,
                        None, // messageId
                        None, // imageBlocks
                        None, // fileBlocks
                    )
                    .await
                {
                    tracing::error!(
                        agent = %agent_id.0,
                        error = %e,
                        "failed to send conflict bounce message to agent"
                    );
                }

                tracing::info!(
                    agent = %agent_id.0,
                    paths = ?conflicting_paths,
                    retry = retry_count + 1,
                    "conflict detected; agent bounced with instructions"
                );

                // Suppress completion propagation
                false
            }
            MergeOutcome::Blocked {
                reason,
                overlapping_paths,
            } => {
                // Blocked: mark merge-pending and propagate with status
                let _ = self
                    .store
                    .update_sandbox_status(
                        workspace_id,
                        agent_id,
                        SandboxStatus::MergePending,
                        &now_iso(),
                    )
                    .await;

                tracing::warn!(
                    agent = %agent_id.0,
                    reason = %reason,
                    paths = ?overlapping_paths,
                    "merge blocked; marking merge-pending"
                );

                self.clear_sandbox_retry_count(workspace_id, agent_id).await;

                // Propagate completion (coordinator/user will see merge-pending status)
                true
            }
        }
    }

    /// Fetch canonical repository state into sandbox for conflict resolution.
    async fn fetch_canonical_to_sandbox(
        &self,
        workspace_id: &WorkspaceId,
        _agent_id: &AgentId,
        sandbox_path: &str,
        _canonical_head: &str,
    ) -> Result<()> {
        // Load workspace to get canonical repo path
        let workspace = self.store.get_workspace(workspace_id).await?;
        let canonical_path = workspace
            .repository_path
            .as_ref()
            .ok_or_else(|| Error::InvalidParams("workspace has no repository_path".to_string()))?;

        // Open sandbox repository
        let sandbox_repo = git2::Repository::open(sandbox_path)
            .map_err(|e| Error::Internal(format!("open sandbox repo failed: {e}")))?;

        // Fetch canonical HEAD into sandbox as canonical/HEAD
        sandbox_repo
            .remote_anonymous(canonical_path)
            .and_then(|mut remote| remote.fetch(&["HEAD:refs/remotes/canonical/HEAD"], None, None))
            .map_err(|e| Error::Internal(format!("fetch canonical to sandbox failed: {e}")))?;

        Ok(())
    }

    async fn get_sandbox_retry_count(&self, workspace_id: &WorkspaceId, agent_id: &AgentId) -> i64 {
        self.store
            .get_sandbox_retry_count(workspace_id, agent_id)
            .await
            .unwrap_or(0)
    }

    async fn increment_sandbox_retry_count(&self, workspace_id: &WorkspaceId, agent_id: &AgentId) {
        let _ = self
            .store
            .increment_sandbox_retry_count(workspace_id, agent_id)
            .await;
    }

    async fn clear_sandbox_retry_count(&self, workspace_id: &WorkspaceId, agent_id: &AgentId) {
        let _ = self
            .store
            .clear_sandbox_retry_count(workspace_id, agent_id)
            .await;
    }

    /// Deliver an internal parent wake through the runtime [`AgentManager`]
    /// when attached — the proven `agent.sendMessage` path: persist + spawn the
    /// turn worker, so the parent runs a REAL turn with the normal stream/idle
    /// event lifecycle (`agent:stream:*` chunks, `agent:idle` at end); a busy
    /// parent gets the wake queued and drained at turn end. Read-only/test
    /// wiring falls back to a store-only persist that carries the same metadata
    /// so the FE `EventWakeupBanner` reads a real `eventCount` / `eventTypes`
    /// instead of the fallback "Subscription update — 0 events".
    pub(crate) async fn deliver_parent_wake(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        content: String,
        message_metadata: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        match self.agent_manager() {
            Some(manager) => {
                manager
                    .send_message(
                        parent_agent_id,
                        workspace_id.clone(),
                        content,
                        None,
                        crate::agent_manager::TurnOptions {
                            message_metadata,
                            ..Default::default()
                        },
                    )
                    .await
            }
            None => {
                let blocks = serde_json::json!([{ "type": "text", "text": content }]);
                self.store
                    .append_agent_message_with_metadata(
                        &parent_agent_id,
                        "user",
                        &blocks,
                        message_metadata.as_ref(),
                        &now_iso(),
                    )
                    .await?;
                Ok(serde_json::json!({ "success": true, "queued": false }))
            }
        }
    }
}

/// Build the FE `event_notification` message metadata for a parent wake, matching
/// the reference `delivery-saga.sendBackendMessage` shape: `type`, `eventCount`,
/// unique `eventTypes` (order-preserving), and a compact per-event array
/// carrying only `id`, `type`, `data`, `timestamp`, `actor`. Feeds
/// `EventWakeupBanner` so it can render a real count / label / per-agent cards.
fn build_event_notification_metadata(events: &[&Event]) -> serde_json::Value {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut event_types: Vec<String> = Vec::new();
    for e in events {
        if seen.insert(e.event_type.as_str()) {
            event_types.push(e.event_type.clone());
        }
    }
    let events_json: Vec<serde_json::Value> = events
        .iter()
        .map(|e| {
            serde_json::json!({
                "id": e.id,
                "type": e.event_type,
                "data": e.data,
                "timestamp": e.timestamp,
                "actor": e.actor,
            })
        })
        .collect();
    serde_json::json!({
        "type": "event_notification",
        "eventCount": events.len(),
        "eventTypes": event_types,
        "events": events_json,
    })
}

/// Fetch a note scoped to `workspace_id`; `NotFound` if absent. Note identity
/// is composite (`(id, workspace_id)`, migration 0030), so the store call is
/// already workspace-safe — this helper stays for call-site clarity.
async fn fetch_note(store: &Store, workspace_id: &WorkspaceId, note_id: &NoteId) -> Result<Note> {
    store.get_note(workspace_id, note_id).await
}

/// Author stamp for daemon-internal note-version writes (the workspace-seed
/// spec note snapshot). All user- and agent-originated writes resolve the
/// author from the caller instead (see [`resolve_note_version_author`]).
fn system_version_author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "system".to_string(),
        name: "intentd".to_string(),
        author_type: "system".to_string(),
    }
}

/// FE/user-originated note-version author. Reference parity with
/// `notes.service.ts` (`{ id: 'user', name: 'User', type: 'user' }`) — used
/// when a note write carries no agent caller.
fn user_version_author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "user".to_string(),
        name: "User".to_string(),
        author_type: "user".to_string(),
    }
}

/// Resolve the version author for a note write. `Some(agent_id)` → an
/// agent-typed author with the caller's session `name` (best-effort; falls
/// back to the id when the session lookup fails). `None` → the user author.
/// Mirrors `notes.service.ts` provenance (`currentActor?.type === 'agent'`
/// path) so `note.listVersions`/`note.getVersion` carry the acting agent.
async fn resolve_note_version_author(
    store: &Store,
    caller_agent_id: Option<&AgentId>,
) -> NoteVersionAuthor {
    match caller_agent_id {
        Some(agent_id) => {
            // Name-only lookup avoids pulling the full message log on every
            // version stamp; the id string is used as the display fallback
            // when the session row is missing.
            let name = store
                .get_agent_session_name(agent_id)
                .await
                .ok()
                .flatten()
                .unwrap_or_else(|| agent_id.0.clone());
            NoteVersionAuthor {
                id: agent_id.0.clone(),
                name,
                author_type: "agent".to_string(),
            }
        }
        None => user_version_author(),
    }
}

/// Append a full-snapshot version of `note`'s *current* (post-mutation) state,
/// stamped with `author` and the note's `updated_at` (PROTOCOL §5.2
/// version-history extensions). The store prunes to the newest 50 on append.
async fn capture_note_version(
    store: &Store,
    note: &Note,
    author: &NoteVersionAuthor,
) -> Result<i64> {
    store
        .append_note_version(note, author, &note.updated_at)
        .await
}

/// Comment anchor recovery pass, called by every note-content mutation before
/// the new content is persisted. Reference `NotesService.updateNote` calls
/// `recoverAllPartialAnchors` on the incoming markdown and marks unrecoverable
/// comments `isOrphaned: true`; we mirror that here using the anchor context
/// captured at `comment.add` time (`anchor_before` / `anchor_after`) instead of
/// version history, since the pass runs before the new version is stored.
///
/// Returns a [`ReanchorPlan`]: the possibly-rewritten markdown plus the set
/// of comments whose `is_orphaned` flag needs to be flipped to `true`.
/// Already-orphaned comments are left as-is. The plan does **no** store
/// writes — callers apply the note-content change first via `update_note`
/// and then call [`ReanchorPlan::apply_orphaned`] so a failed note write
/// cannot leave comment rows flagged orphaned while the persisted markdown
/// still contains the original anchors.
///
/// The lookup is scoped to `workspace_id` because `note_id` (e.g. the
/// well-known `spec` id) is not globally unique — see
/// `comment_repo::list_comments_in_workspace`.
async fn reanchor_note_comments(
    store: &Store,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    content: String,
) -> Result<ReanchorPlan> {
    // Propagate the list_comments error: a failed lookup must not leave the
    // note content persisted with broken anchors while the comment row still
    // claims to be healthy. Callers already treat `reanchor_note_comments` as
    // fallible and roll back the surrounding mutation.
    let comments = store
        .list_comments_in_workspace(workspace_id, note_id)
        .await?;
    let mut current = content;
    let mut orphaned: Vec<Comment> = Vec::new();
    for comment in comments {
        // Only root-level anchored comments carry markers; replies inherit the
        // parent's anchor and never inject their own into the note body.
        if comment.parent_id.is_some() {
            continue;
        }
        if comment.is_orphaned == Some(true) {
            continue;
        }
        let state = note_ops::classify_anchor_state(&current, &comment.id);
        match state {
            note_ops::AnchorState::Healthy => {}
            note_ops::AnchorState::Missing => {
                let mut updated = comment.clone();
                updated.is_orphaned = Some(true);
                updated.updated_at = now_iso();
                orphaned.push(updated);
            }
            note_ops::AnchorState::Degenerate => {
                current = note_ops::remove_anchor_markers(&current, &comment.id);
                let mut updated = comment.clone();
                updated.is_orphaned = Some(true);
                updated.updated_at = now_iso();
                orphaned.push(updated);
            }
            note_ops::AnchorState::PartialStartOnly | note_ops::AnchorState::PartialEndOnly => {
                let outcome = note_ops::recover_partial_anchor(
                    &current,
                    &comment.id,
                    comment.anchor_before.as_deref(),
                    comment.anchor_after.as_deref(),
                );
                match outcome {
                    note_ops::RecoveryOutcome::Recovered(new_md) => {
                        current = new_md;
                    }
                    note_ops::RecoveryOutcome::Failed(_) => {
                        current = note_ops::remove_anchor_markers(&current, &comment.id);
                        let mut updated = comment.clone();
                        updated.is_orphaned = Some(true);
                        updated.updated_at = now_iso();
                        orphaned.push(updated);
                    }
                }
            }
        }
    }
    Ok(ReanchorPlan {
        content: current,
        orphaned,
    })
}

/// Result of [`reanchor_note_comments`]: the rewritten note markdown and the
/// set of comment rows whose `is_orphaned` flag needs to flip to `true`.
struct ReanchorPlan {
    content: String,
    orphaned: Vec<Comment>,
}

impl ReanchorPlan {
    /// Persist the queued orphan flips. Callers **must** run this only after
    /// the note-content write has succeeded so a note-write failure cannot
    /// leave comment rows marked orphaned while the persisted markdown still
    /// contains the original anchors. If a comment update fails after the
    /// note has been written the anchors on disk are still a valid pointer
    /// (either intact or scrubbed) and the next mutation's reanchor pass
    /// will finish the job.
    async fn apply_orphaned(self, store: &Store, workspace_id: &WorkspaceId) -> Result<()> {
        for c in self.orphaned {
            store.update_comment(workspace_id, &c).await?;
        }
        Ok(())
    }
}

/// FE `LineAttributionService.DEBOUNCE_MS`. Coalesces bursts of note
/// mutations into a single recompute + `line-attribution:updated` emit.
const LINE_ATTRIBUTION_DEBOUNCE: std::time::Duration = std::time::Duration::from_secs(5);

/// Convert an RFC-3339 timestamp to milliseconds since the Unix epoch, matching
/// the FE `new Date(version.createdAt).getTime()` used for the gutter age.
fn iso_to_epoch_ms(iso: &str) -> i64 {
    parse_iso(iso)
        .and_then(|dt| i64::try_from(dt.unix_timestamp_nanos() / 1_000_000).ok())
        .unwrap_or(0)
}

/// Return the latest RFC-3339 timestamp among the supplied candidates, ignoring
/// unparsable or absent entries. Powers the `lastActivity` derivation on every
/// `workspace.*` path that returns a `Workspace` on the wire (§9.1) — the
/// list/get read path via [`Services::enrich_workspace_aggregates`] and the
/// update/archive/unarchive mutation paths via [`Services::derive_last_activity`].
/// FE `getLatestActivityCandidate` parity.
fn latest_activity_candidate(candidates: &[Option<&str>]) -> Option<String> {
    let mut best: Option<(i64, String)> = None;
    for c in candidates.iter().copied().flatten() {
        let ms =
            parse_iso(c).and_then(|dt| i64::try_from(dt.unix_timestamp_nanos() / 1_000_000).ok());
        if let Some(ms) = ms {
            if best.as_ref().map(|(b, _)| ms > *b).unwrap_or(true) {
                best = Some((ms, c.to_string()));
            }
        }
    }
    best.map(|(_, s)| s)
}

/// Conservative allowlist of remote names whose first path segment may be
/// stripped when canonicalising a workspace `baseRef` on write. Mirrors the
/// FE `baseref-matching.ts` allowlist so a persisted `baseRef` compares raw-
/// equal to a PR `sourceBranch` (§7.6). Slashed local branches like
/// `feature/foo` are never stripped.
const CANONICAL_BASE_REF_REMOTES: &[&str] = &["origin/", "upstream/", "fork/"];

/// Strip a known remote prefix (`origin/`, `upstream/`, `fork/`) from a raw
/// `baseRef` string, returning the canonical plain branch name. Values without
/// a known prefix, and empty stripped remainders, are returned unchanged.
fn canonicalise_base_ref(raw: &str) -> String {
    for prefix in CANONICAL_BASE_REF_REMOTES {
        if let Some(rest) = raw.strip_prefix(prefix) {
            if !rest.is_empty() {
                return rest.to_string();
            }
        }
    }
    raw.to_string()
}

/// Build a `line-attribution:updated` change event with the FE-parity payload
/// `{ workspaceId, noteId, attributions }` (`line-attribution.service.ts`
/// `mainDispatch(lineAttributionUpdated(...))`).
fn line_attribution_updated_event(data: &LineAttributionData) -> NewEvent {
    NewEvent {
        workspace_id: data.workspace_id.clone(),
        timestamp: now_iso(),
        event_type: LINE_ATTRIBUTION_UPDATED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": data.workspace_id.as_str(),
            "noteId": data.note_id.as_str(),
            "attributions": data.attributions,
        }),
    }
}

impl Services {
    /// Run `attribute_lines` over the note's current content + full version
    /// history, persist the result, and emit `line-attribution:updated`.
    /// The write is idempotent (upsert), so a race between two computes leaves
    /// the store consistent with the *latest* one to complete.
    async fn compute_and_persist_line_attribution(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<LineAttributionData> {
        let note = fetch_note(&self.store, workspace_id, note_id).await?;
        let summaries = self.store.list_note_versions(workspace_id, note_id).await?;
        let mut versions: Vec<NoteVersion> = Vec::with_capacity(summaries.len());
        for summary in &summaries {
            versions.push(
                self.store
                    .get_note_version(workspace_id, note_id, summary.v)
                    .await?,
            );
        }
        let attributions = line_attribution::attribute_lines(&note.content, &versions);
        let mut map: std::collections::BTreeMap<String, LineAttributionInfo> =
            std::collections::BTreeMap::new();
        for attr in &attributions {
            let Some(version) = &attr.version else {
                continue;
            };
            let timestamp = iso_to_epoch_ms(&version.date);
            let author = Some(LineAttributionAuthor {
                id: version.author.id.clone(),
                name: version.author.name.clone(),
                author_type: version.author.author_type.clone(),
                // The daemon does not yet carry per-turn context on note
                // versions (§5.2 version-history extensions still stamps every
                // version with the system author); wired through on future
                // author-aware mutations.
                turn_number: None,
            });
            map.insert(
                attr.line_number.to_string(),
                LineAttributionInfo { timestamp, author },
            );
        }
        let data = LineAttributionData {
            note_id: note.id.clone(),
            workspace_id: note.workspace_id.clone(),
            computed_at: now_iso(),
            attributions: map,
        };
        self.store.upsert_note_line_attribution(&data).await?;
        publish_event(&self.event_bus, line_attribution_updated_event(&data)).await;
        Ok(data)
    }

    /// Schedule a debounced recompute of `note_id` attributions (FE
    /// `LineAttributionService.scheduleComputation`). A subsequent call for
    /// the same key cancels the prior timer, so a burst of mutations settles
    /// into a single recompute + emit after [`LINE_ATTRIBUTION_DEBOUNCE`].
    pub(crate) fn schedule_line_attribution_recompute(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) {
        let key = (workspace_id.clone(), note_id.clone());
        let services = self.clone();
        let key_for_task = key.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(LINE_ATTRIBUTION_DEBOUNCE).await;
            let (ws, nid) = key_for_task;
            if let Err(err) = services
                .compute_and_persist_line_attribution(&ws, &nid)
                .await
            {
                tracing::warn!(
                    workspace = %ws.as_str(),
                    note = %nid.as_str(),
                    error = %err,
                    "line-attribution recompute failed",
                );
            }
            if let Ok(mut map) = services.line_attribution_debouncers.lock() {
                let existing = map.get(&(ws.clone(), nid.clone()));
                if existing.map(|h| h.is_finished()).unwrap_or(false) {
                    map.remove(&(ws, nid));
                }
            }
        });
        let abort = handle.abort_handle();
        if let Ok(mut map) = self.line_attribution_debouncers.lock() {
            if let Some(prev) = map.insert(key, abort) {
                prev.abort();
            }
        }
    }

    /// Drop any cached CRDT session for `(workspace, note)` after a surgical
    /// content mutation (`note.add` / `note.edit` / `note.editLines`,
    /// `task.updateStatus` / `task.update`, `note.restoreVersion`,
    /// `note.convertBlocks`, `comment.add`) writes directly to storage. The
    /// next full-content write (`note.setContent`) will reseed the yrs doc
    /// from the fresh persisted content so the merge baseline stays coherent.
    pub(crate) fn invalidate_crdt_note(&self, workspace_id: &WorkspaceId, note_id: &NoteId) {
        self.crdt_notes.invalidate(workspace_id, note_id);
    }
}

/// Seed the well-known `spec` note for a workspace (reference parity with
/// `notes.service.ts ensureSpecExists`). Called from both `workspace.create`
/// (initial seed) and `note.list` (lazy self-heal for workspaces whose spec
/// went missing, e.g. rows created before the composite-PK migration).
/// Idempotent: if a spec note already exists in this workspace, this is a
/// no-op; otherwise it inserts the default Spec (empty markdown, pinned,
/// default, workspace visibility), captures the initial version, and publishes
/// `note:created` exactly once. Note identity is composite (`(id,
/// workspace_id)`, migration 0030), so every workspace owns its own spec — no
/// cross-workspace collision is possible.
async fn ensure_spec_note(
    store: &Store,
    bus: &Option<EventBus>,
    workspace_id: &WorkspaceId,
) -> Result<()> {
    let spec_id = NoteId::from("spec");
    match fetch_note(store, workspace_id, &spec_id).await {
        Ok(_) => return Ok(()),
        Err(Error::NotFound(_)) => {}
        Err(e) => return Err(e),
    }
    // Self-heal for workspaces damaged by the pre-#110 global-note-identity
    // bug: agents that hit "no `spec` yet" fell back to `note.create` and
    // captured a random UUID id, leaving `isSpecNote()` (id-based) blind to
    // real spec content. Rather than seed an empty spec beside the stray,
    // rewrite the stray to the reserved id (children, versions, attribution,
    // and comments follow in the same tx) and emit delete+create events so
    // live FE clients reconcile. Nothing to adopt (zero or ≥2 candidates)
    // falls through to the empty seed below.
    if let Some((old_id, adopted_title)) = store.adopt_stray_spec_note(workspace_id).await? {
        publish_event(
            bus,
            note_change_event(
                workspace_id,
                &old_id,
                &adopted_title,
                NOTE_DELETED,
                "delete",
            ),
        )
        .await;
        publish_event(
            bus,
            note_change_event(
                workspace_id,
                &spec_id,
                &adopted_title,
                NOTE_CREATED,
                "create",
            ),
        )
        .await;
        return Ok(());
    }
    let now = now_iso();
    let note = Note {
        id: spec_id,
        workspace_id: workspace_id.clone(),
        title: "Spec".to_string(),
        content: String::new(),
        content_type: ContentType::Markdown,
        tags: vec!["spec".to_string()],
        is_pinned: true,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: now.clone(),
        rev: 0,
        updated_at: now,
    };
    store.insert_note(&note).await?;
    // Workspace-seed spec is daemon-internal; no caller agent applies.
    capture_note_version(store, &note, &system_version_author()).await?;
    publish_event(
        bus,
        note_change_event(
            &note.workspace_id,
            &note.id,
            &note.title,
            NOTE_CREATED,
            "create",
        ),
    )
    .await;
    Ok(())
}

/// Like [`fetch_note`] but surfaces the TS peer's `Note not found: <id>` message
/// as [`Error::Internal`] (→ `-32603`), matching the `ws.note.*` edit peers.
async fn fetch_note_peer(
    store: &Store,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
) -> Result<Note> {
    match store.get_note(workspace_id, note_id).await {
        Ok(n) => Ok(n),
        Err(Error::NotFound(_)) => Err(Error::Internal(format!("Note not found: {note_id}"))),
        Err(e) => Err(e),
    }
}

/// Resolve a sibling workspace for the `crossWorkspace.*` reads, enforcing that
/// the caller and target share the same `repositoryPath`. Mirrors the TS
/// `getSiblingWorkspaceOrThrow` messages; all failures surface as
/// [`Error::Internal`] (→ `-32603`) to match the TS handler.
async fn sibling_workspace_or_throw(
    store: &Store,
    current_workspace_id: &WorkspaceId,
    target_workspace_id: &WorkspaceId,
) -> Result<Workspace> {
    let current = match store.get_workspace(current_workspace_id).await {
        Ok(w) => w,
        Err(Error::NotFound(_)) => {
            return Err(Error::Internal("Current workspace not found".to_string()));
        }
        Err(e) => return Err(e),
    };
    let repo_path = match current.repository_path.as_deref() {
        Some(p) if !p.is_empty() => p.to_string(),
        _ => {
            return Err(Error::Internal(
                "Current workspace is not associated with a repository".to_string(),
            ));
        }
    };
    let target = match store.get_workspace(target_workspace_id).await {
        Ok(w) => w,
        Err(Error::NotFound(_)) => {
            return Err(Error::Internal(format!(
                "Target workspace not found: {target_workspace_id}"
            )));
        }
        Err(e) => return Err(e),
    };
    if target.repository_path.as_deref() != Some(repo_path.as_str()) {
        return Err(Error::Internal(
            "Access denied: Can only access workspaces in the same repository".to_string(),
        ));
    }
    Ok(target)
}

/// Prefix each line with a right-aligned 1-based line number (`"   1 | text"`),
/// matching the TS `numberLines` helper used by `crossWorkspace.readNote`.
fn number_lines(content: &str) -> String {
    content
        .split('\n')
        .enumerate()
        .map(|(i, line)| format!("{:>4} | {line}", i + 1))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Fresh v4 uuid string for an agent-authored primitive id (TS `uuidv4()`).
fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Whitespace-only strings collapse to `None` (TS truthy-string parity for
/// caller-supplied identifiers/names in `workspace.create` /
/// `initialAgent`).
fn nonempty_owned(s: Option<String>) -> Option<String> {
    s.filter(|v| !v.trim().is_empty())
}

/// Shared `primitive.*` glue: append the fenced `ws-block:<block_type>` JSON of
/// `primitive` to the note, persist it, emit `note:updated`, and return the TS
/// `appendPrimitiveBlock` response `{ ok, primitiveId, noteId, content }`. A
/// missing note surfaces as `Error::Internal` (→ `-32603`), matching the TS
/// builder which throws `Note <id> not found`.
async fn append_primitive(
    store: &Store,
    bus: &Option<EventBus>,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    primitive: &serde_json::Value,
    block_type: &str,
    primitive_id: &str,
) -> Result<serde_json::Value> {
    let mut note = fetch_note_peer(store, workspace_id, note_id).await?;
    let new_content = primitive_ops::append_block(&note.content, primitive, block_type);
    note.content = new_content.clone();
    note.updated_at = now_iso();
    store.update_note(&note).await?;
    publish_event(
        bus,
        note_change_event(
            &note.workspace_id,
            &note.id,
            &note.title,
            NOTE_UPDATED,
            "update",
        ),
    )
    .await;
    Ok(serde_json::json!({
        "ok": true,
        "primitiveId": primitive_id,
        "noteId": note_id.as_str(),
        "content": new_content,
    }))
}

/// Extract the spec-linked task-note ids from a spec note's markdown body
/// (`[text](intent://local/task/{id})`), mirroring the TS `extractSpecTaskIds`
/// (`TASK_LINK_REGEX_FLEXIBLE`).
fn extract_spec_task_ids(content: &str) -> HashSet<String> {
    const MARKER: &str = "(intent://local/task/";
    let mut ids = HashSet::new();
    let mut rest = content;
    while let Some(pos) = rest.find(MARKER) {
        let after = &rest[pos + MARKER.len()..];
        match after.find(')') {
            Some(end) => {
                let id = &after[..end];
                if !id.is_empty() {
                    ids.insert(id.to_string());
                }
                rest = &after[end + 1..];
            }
            None => break,
        }
    }
    ids
}

/// Compute a workspace's `taskStats` card aggregate from its notes, porting the
/// canonical `computeTaskStats` (`task-stats.ts`) over the spec-linked direct
/// child task notes: `cancelled` is excluded from `total`, `complete` counts as
/// `completed`, and `in_progress`/`review_required` count as `inProgress`. When
/// the spec body has no task links, all direct children with task metadata count
/// (TS backward-compat fallback).
fn compute_task_stats(notes: &[Note]) -> WorkspaceTaskStats {
    let linked = notes
        .iter()
        .find(|n| n.id.as_str() == "spec")
        .map(|n| extract_spec_task_ids(&n.content))
        .unwrap_or_default();
    let has_links = !linked.is_empty();

    let mut seen = HashSet::new();
    let mut stats = WorkspaceTaskStats::default();
    for note in notes {
        let Some(task) = &note.metadata.task else {
            continue;
        };
        let id = note.id.as_str();
        if id == "spec" {
            continue;
        }
        if note.parent_id.as_ref().map(|p| p.as_str()) != Some("spec") {
            continue;
        }
        if has_links && !linked.contains(id) {
            continue;
        }
        if !seen.insert(id.to_string()) {
            continue;
        }
        match task.status {
            TaskStatus::Cancelled => continue,
            TaskStatus::Complete => {
                stats.total += 1;
                stats.completed += 1;
            }
            TaskStatus::InProgress | TaskStatus::ReviewRequired => {
                stats.total += 1;
                stats.in_progress += 1;
            }
            _ => stats.total += 1,
        }
    }
    stats
}

/// Project a workspace's notes into the canonical `WorkspaceTask` list, porting
/// `getWorkspaceTasks` (`workspace-summaries.ts`) over the `getSpecTaskNotes`
/// (`task-stats.ts`) filter: spec-linked direct child task notes, **including
/// cancelled** (renderer selectors derive counts/groupings). When the spec body
/// has no task links, all direct children with task metadata count (TS
/// backward-compat fallback). Order follows the stored note order; the title
/// falls back to `Untitled task` to match the TS `note.title || 'Untitled task'`.
fn workspace_task_list(notes: &[Note]) -> Vec<WorkspaceTask> {
    let linked = notes
        .iter()
        .find(|n| n.id.as_str() == "spec")
        .map(|n| extract_spec_task_ids(&n.content))
        .unwrap_or_default();
    let has_links = !linked.is_empty();

    let mut seen = HashSet::new();
    let mut tasks = Vec::new();
    for note in notes {
        let Some(task) = &note.metadata.task else {
            continue;
        };
        let id = note.id.as_str();
        if id == "spec" {
            continue;
        }
        if note.parent_id.as_ref().map(|p| p.as_str()) != Some("spec") {
            continue;
        }
        if has_links && !linked.contains(id) {
            continue;
        }
        if !seen.insert(id.to_string()) {
            continue;
        }
        tasks.push(WorkspaceTask {
            id: note.id.clone(),
            title: if note.title.is_empty() {
                "Untitled task".to_string()
            } else {
                note.title.clone()
            },
            status: task.status,
            updated_at: note.updated_at.clone(),
        });
    }
    tasks
}

/// Project a single note into a [`WorkspaceTask`], applying the TS
/// `Untitled task` title fallback. Returns `Internal` when the note is not a
/// task (mirrors `task.getMyTask`'s "Note is not a task" guard).
fn note_to_workspace_task(note: &Note) -> Result<WorkspaceTask> {
    let task = match &note.metadata.task {
        Some(t) => t,
        None => return Err(Error::Internal("Note is not a task".to_string())),
    };
    Ok(WorkspaceTask {
        id: note.id.clone(),
        title: if note.title.is_empty() {
            "Untitled task".to_string()
        } else {
            note.title.clone()
        },
        status: task.status,
        updated_at: note.updated_at.clone(),
    })
}

/// Project a workspace's agent sessions into the `agentSummary` card aggregate
/// (`{ count, agents, agentIds }`). `isStreaming`/`isResponding` are always
/// `false` (the headless backend has no live stream state; `status` carries
/// liveness). `agentIds` lists the same agents (forward-compat TS parity).
fn build_agent_summary(sessions: &[AgentSession]) -> WorkspaceAgentSummary {
    let agents: Vec<WorkspaceAgentInfo> = sessions
        .iter()
        .map(|s| WorkspaceAgentInfo {
            id: s.id.clone(),
            name: s.name.clone(),
            status: s.status,
            specialist: s.specialist.clone(),
            last_activity: Some(s.updated_at.clone()),
            is_streaming: false,
            is_responding: false,
        })
        .collect();
    let agent_ids: Vec<_> = sessions.iter().map(|s| s.id.clone()).collect();
    WorkspaceAgentSummary {
        count: agents.len(),
        agents,
        agent_ids,
    }
}

/// Compute a workspace's `diffSummary` card aggregate from its git worktree,
/// porting the on-demand `computeWorkspaceDiffSummary`. Returns `None` when the
/// workspace has no worktree, the worktree is not a git repo, or there are no
/// changes (matching the TS `undefined` fallback).
fn compute_diff_summary(ws: &Workspace) -> Option<WorkspaceDiffSummary> {
    let worktree = ws.worktree_path.as_deref().filter(|p| !p.is_empty())?;
    let path = Path::new(worktree);
    if !path.join(".git").exists() {
        return None;
    }
    let (total_files, total_additions, total_deletions) =
        intent_git::diff::head_diff_rollup(path).ok()?;
    if total_files == 0 {
        return None;
    }
    Some(WorkspaceDiffSummary {
        schema_version: 1,
        updated_at: now_iso(),
        total_files,
        total_additions,
        total_deletions,
        files: Vec::new(),
    })
}

/// The seven valid task-note statuses, in the order the TS validator lists them.
const TASK_STATUS_WORDS: [&str; 7] = [
    "not_started",
    "waiting",
    "discussion_needed",
    "in_progress",
    "review_required",
    "complete",
    "cancelled",
];

/// Parse a `TaskStatus`, validating against the canonical set with the TS
/// `updateNoteStatus` error message.
fn parse_task_status_strict(s: &str) -> Result<TaskStatus> {
    if !TASK_STATUS_WORDS.contains(&s) {
        return Err(Error::Internal(format!(
            "Invalid status: {s}. Must be one of: {}",
            TASK_STATUS_WORDS.join(", ")
        )));
    }
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| Error::Internal(format!("Invalid status: {s} ({e})")))
}

/// Parse a comment type word, defaulting to `comment` on absent/unknown values
/// (the TS handler passes the raw value straight through with that default).
fn parse_comment_type(opt: Option<&str>) -> CommentType {
    match opt {
        Some(s) => serde_json::from_value(serde_json::Value::String(s.to_string()))
            .unwrap_or(CommentType::Comment),
        None => CommentType::Comment,
    }
}

/// Validate an `agent-{uuid}` id, mirroring the TS `agentIdPattern`.
fn is_valid_agent_id(s: &str) -> bool {
    let rest = match s.strip_prefix("agent-") {
        Some(r) => r,
        None => return false,
    };
    let groups = [8usize, 4, 4, 4, 12];
    let parts: Vec<&str> = rest.split('-').collect();
    if parts.len() != groups.len() {
        return false;
    }
    parts
        .iter()
        .zip(groups.iter())
        .all(|(part, &len)| part.len() == len && part.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// Reconstruct the TS `markId` for a comment anchor
/// (`"{startId}:start|{endId}:end"` for ranges, the `pointId` for points).
fn derive_mark_id(anchor: &CommentAnchor) -> Option<String> {
    match anchor.kind {
        CommentAnchorType::Range => match (&anchor.start_id, &anchor.end_id) {
            (Some(start), Some(end)) => Some(format!("{start}:start|{end}:end")),
            _ => None,
        },
        CommentAnchorType::Point => anchor.point_id.clone(),
    }
}

/// Apply the `updateTaskStatus` timestamp transitions in place.
fn apply_status_transition(task: &mut TaskMetadata, status: TaskStatus, now: &str) {
    task.status = status;
    if status == TaskStatus::InProgress && task.started_at.is_none() {
        task.started_at = Some(now.to_string());
    }
    if status == TaskStatus::Complete && task.completed_at.is_none() {
        task.completed_at = Some(now.to_string());
    }
    if status != TaskStatus::Complete && task.completed_at.is_some() {
        task.completed_at = None;
    }
}

/// Build a fresh task-note metadata with the markAsTask enrichment timestamps.
fn fresh_task_metadata(status: TaskStatus, now: &str, peer_order: Option<i64>) -> TaskMetadata {
    let mut task = TaskMetadata {
        status,
        peer_order,
        ..Default::default()
    };
    apply_status_transition(&mut task, status, now);
    task
}

/// The `system` actor used for change events emitted by daemon-side mutations.
/// Mirrors the TS fallback `{ type: 'system', id: 'system', name: 'System' }`
/// used by `createWorkspaceEvent` when no provenance actor is present; agent
/// provenance attribution is wired in a later milestone.
pub(crate) fn system_actor() -> intent_core::EventActor {
    intent_core::EventActor {
        actor_type: ActorType::System,
        id: Some("system".to_string()),
        name: Some("System".to_string()),
        ..Default::default()
    }
}

/// Process-level guard mirroring the TS `repoRegistrySynced` flag: the
/// workspace→registry sync runs at most once per daemon lifetime, on the first
/// `repo.list` call.
static REPO_REGISTRY_SYNCED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Resolve a known-repo display name from an optional explicit name plus the
/// repo path, mirroring TS `repositoryName || path.split('/').pop() || 'Unknown'`
/// (an empty explicit name falls through to the basename).
fn known_repo_name(explicit: Option<&str>, path: &str) -> String {
    if let Some(n) = explicit {
        if !n.is_empty() {
            return n.to_string();
        }
    }
    let base = path.rsplit('/').next().unwrap_or("");
    if base.is_empty() {
        "Unknown".to_string()
    } else {
        base.to_string()
    }
}

/// Derive a repository display name from a local `repositoryPath` basename,
/// mirroring the `known_repo_name` fallback — but returning `None` instead of
/// an `"Unknown"` placeholder when the path has no usable basename, so an
/// underivable name stays NULL on the workspace row. Splits on both `/` and
/// `\` so Windows-style paths (`C:\src\repo`) derive correctly.
fn derive_repo_name_from_path(path: &str) -> Option<String> {
    let base = path.rsplit(['/', '\\']).next().unwrap_or("");
    (!base.is_empty()).then(|| base.to_string())
}

/// Default root for daemon-provisioned worktrees: `$INTENTD_WORKSPACES_DIR`
/// override, else `~/intent/workspaces` — the FE's
/// `WorkspaceConfig.WORKSPACES_BASE` layout (`<root>/<workspaceId>/<repo-slug>`).
///
/// The `$HOME` fallback is guarded by [`assert_hermetic_root_absent`] so
/// `cargo test` can never leak workspace dirs into the developer's real
/// `~/intent/workspaces` (crate unit tests panic unconditionally; the intentd
/// binary panics only when spawned with `INTENTD_ASSERT_HERMETIC_ROOT=1`).
fn default_workspaces_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("INTENTD_WORKSPACES_DIR") {
        return PathBuf::from(dir);
    }
    assert_hermetic_root_absent();
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("intent")
        .join("workspaces")
}

/// Structural guard against `~/intent/workspaces` leaks from tests. Fires only
/// inside crate unit tests (`cfg(test)`) or when the harness explicitly opts
/// in via `INTENTD_ASSERT_HERMETIC_ROOT`; production `intentd` binaries never
/// see it.
#[cfg(test)]
fn assert_hermetic_root_absent() {
    panic!(
        "hermetic-tests: intent-services unit test reached default_workspaces_root() \
         with no INTENTD_WORKSPACES_DIR or .with_workspaces_root(); tests must never \
         provision under $HOME/intent/workspaces"
    );
}

#[cfg(not(test))]
fn assert_hermetic_root_absent() {
    if std::env::var_os("INTENTD_ASSERT_HERMETIC_ROOT").is_some() {
        panic!(
            "hermetic-tests: intentd process reached default_workspaces_root() with \
             INTENTD_ASSERT_HERMETIC_ROOT set but no INTENTD_WORKSPACES_DIR — the \
             spawning test harness must set INTENTD_WORKSPACES_DIR to a tempdir"
        );
    }
}

/// Resolve a workspace-id slug for `workspace.create`, porting the TS
/// `generateLocalSlug` behavior (`workspace.service.ts`): extract from the
/// `initialAgent.prompt` when possible, else fall back to a random
/// adjective-animal pair. Uniquified with a `-N` suffix whenever the candidate
/// id was **ever** used — a live row, a delete tombstone
/// (`deleted_workspace_id`), or a leftover `<workspaces_root>/<id>` directory
/// on disk — so ids are never recycled across delete/recreate (FE
/// `recentlyDeletedWorkspaces` parity: reuse would collide the old
/// workspace's agent streams and file paths with the new one's).
async fn derive_workspace_id(
    store: &Store,
    input: &WorkspaceCreate,
    workspaces_root: &Path,
) -> WorkspaceId {
    let base = input
        .initial_agent
        .as_ref()
        .and_then(|a| a.prompt.as_deref())
        .and_then(intent_core::slug::extract_local_slug)
        .unwrap_or_else(intent_core::slug::generate_workspace_slug);
    let candidate = WorkspaceId::from_string(base.clone());
    if workspace_id_available(store, workspaces_root, &candidate).await {
        return candidate;
    }
    let mut n: u32 = 2;
    loop {
        let candidate = WorkspaceId::from_string(intent_core::slug::append_slug_suffix(&base, n));
        if workspace_id_available(store, workspaces_root, &candidate).await {
            return candidate;
        }
        n += 1;
    }
}

/// A workspace id is available for `workspace.create` only when it was never
/// used before: no live row, no `deleted_workspace_id` tombstone, and no
/// existing `<workspaces_root>/<id>` directory (covers pre-tombstone deletes
/// and orphaned dirs). A store error counts as "used" — colliding is safer
/// than recycling.
async fn workspace_id_available(store: &Store, workspaces_root: &Path, id: &WorkspaceId) -> bool {
    match store.workspace_id_ever_used(id).await {
        Ok(false) => !workspaces_root.join(id.as_str()).exists(),
        _ => false,
    }
}

/// Write the legacy `<workspaces_root>/<id>/.workspace/workspace.json` file
/// that renderer code paths (FE `FileSystemWorkspaceRepository.findById`)
/// still read for per-workspace metadata. Adds the FE-only
/// `changesets`/`timeline`/`conversationInfo` arrays required by the FE
/// `WorkspaceSchema`; every other field flows from the serialized `Workspace`.
fn write_workspace_metadata_file(root: &Path, ws: &Workspace) -> Result<()> {
    let dir = root.join(&ws.id.0).join(".workspace");
    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::Internal(format!("create metadata dir failed: {e}")))?;
    let mut value = serde_json::to_value(ws)
        .map_err(|e| Error::Internal(format!("serialize workspace failed: {e}")))?;
    if let Some(obj) = value.as_object_mut() {
        obj.entry("changesets").or_insert(serde_json::json!([]));
        obj.entry("timeline").or_insert(serde_json::json!([]));
        obj.entry("conversationInfo")
            .or_insert(serde_json::json!([]));
    }
    let bytes = serde_json::to_vec_pretty(&value)
        .map_err(|e| Error::Internal(format!("serialize workspace failed: {e}")))?;
    let path = dir.join("workspace.json");
    std::fs::write(&path, &bytes)
        .map_err(|e| Error::Internal(format!("write workspace.json failed: {e}")))?;
    Ok(())
}

/// Worktree folder name for a repo, porting the FE
/// `WorkspaceConfigConstants.slugify` + `generateWorktreeFolderName`:
/// lowercase, non-alphanumeric runs collapse to `-`, trimmed, ≤50 chars,
/// `"repo"` fallback.
fn worktree_folder_slug(repo_name: &str) -> String {
    let mut slug = String::new();
    for c in repo_name.chars().flat_map(char::to_lowercase) {
        if slug.len() >= 50 {
            break;
        }
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug.trim_end_matches('-');
    if slug.is_empty() {
        "repo".to_string()
    } else {
        slug.to_string()
    }
}

/// Recursively scan `dir` for git repositories (a directory that contains a
/// `.git` child — either a `.git/` directory *or* a `.git` file whose first
/// non-empty line starts with `gitdir:`, matching the worktree pointer form
/// that `host_ops::has_git_marker` already recognizes). Ports the FE
/// `workspace.repository.scanDirectory` semantics: depth capped at 3, hidden
/// entries skipped, git roots emitted without descending further, and I/O
/// errors on individual entries are silently swallowed. Absolute paths are
/// pushed into `out` as UTF-8 strings; entries whose paths cannot be
/// represented as UTF-8 are skipped.
fn scan_for_repositories(dir: &Path, depth: usize, out: &mut Vec<String>) {
    if depth > 3 {
        return;
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    let entries: Vec<_> = entries.flatten().collect();
    let is_git_repo = entries.iter().any(|e| {
        if e.file_name() != *".git" {
            return false;
        }
        match e.file_type() {
            Ok(t) if t.is_dir() => true,
            Ok(t) if t.is_file() => std::fs::read_to_string(e.path())
                .map(|s| s.trim_start().starts_with("gitdir:"))
                .unwrap_or(false),
            _ => false,
        }
    });
    if is_git_repo {
        if let Some(s) = dir.to_str() {
            out.push(s.to_string());
        }
        return;
    }
    for entry in entries {
        let ft = match entry.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !ft.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name_str = match name.to_str() {
            Some(s) => s,
            None => continue,
        };
        if name_str.starts_with('.') {
            continue;
        }
        scan_for_repositories(&dir.join(name_str), depth + 1, out);
    }
}

/// Blocking body for `WorkspaceApi::initialize_repository`. Mirrors the FE
/// `initializeNewRepository` reference: `mkdir -p`, quiet no-op when the
/// target already carries a git repo with at least one commit, otherwise
/// `git init -b main`, seed `.gitignore` + `README.md`, and land an initial
/// commit. Git user identity falls back to `Intent <intent@local>` when no
/// global identity is configured. Every failure surfaces as
/// [`Error::Internal`].
fn initialize_repository_blocking(repo_path: &Path) -> Result<()> {
    use std::process::Command;
    std::fs::create_dir_all(repo_path).map_err(|e| {
        Error::Internal(format!(
            "workspace.initializeRepository: mkdir {} failed: {e}",
            repo_path.display()
        ))
    })?;
    let git_dir = repo_path.join(".git");
    if git_dir.exists() {
        let head = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo_path)
            .output();
        if let Ok(out) = head {
            if out.status.success() && !out.stdout.is_empty() {
                return Ok(());
            }
        }
    }
    let run = |args: &[&str]| -> Result<()> {
        let out = Command::new("git")
            .args(args)
            .current_dir(repo_path)
            .output()
            .map_err(|e| {
                Error::Internal(format!(
                    "workspace.initializeRepository: git {} failed to spawn: {e}",
                    args.join(" ")
                ))
            })?;
        if !out.status.success() {
            return Err(Error::Internal(format!(
                "workspace.initializeRepository: git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(())
    };
    run(&["init", "-b", "main"])?;
    let read_global = |key: &str| -> Option<String> {
        let out = Command::new("git")
            .args(["config", "--global", key])
            .current_dir(repo_path)
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let value = String::from_utf8(out.stdout).ok()?.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    };
    let user_name = read_global("user.name").unwrap_or_else(|| "Intent".to_string());
    let user_email = read_global("user.email").unwrap_or_else(|| "intent@local".to_string());
    run(&["config", "user.name", &user_name])?;
    run(&["config", "user.email", &user_email])?;
    let gitignore = "# Dependencies\nnode_modules/\n.pnpm-store/\n\n# Build outputs\ndist/\nbuild/\nout/\n\n# IDE\n.idea/\n.vscode/\n*.swp\n*.swo\n\n# OS\n.DS_Store\nThumbs.db\n\n# Environment\n.env\n.env.local\n.env.*.local\n\n# Logs\n*.log\nnpm-debug.log*\n";
    std::fs::write(repo_path.join(".gitignore"), gitignore).map_err(|e| {
        Error::Internal(format!(
            "workspace.initializeRepository: write .gitignore failed: {e}"
        ))
    })?;
    let repo_name = repo_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("workspace");
    let readme = format!("# {repo_name}\n\nA new project created with Intent by Augment.\n");
    std::fs::write(repo_path.join("README.md"), readme).map_err(|e| {
        Error::Internal(format!(
            "workspace.initializeRepository: write README.md failed: {e}"
        ))
    })?;
    run(&["add", "."])?;
    // Initial commit tolerates "nothing to commit" (reruns after a manual
    // seed), matching the FE reference. Every other non-zero exit propagates.
    let commit = Command::new("git")
        .args(["commit", "-m", "Initial commit"])
        .current_dir(repo_path)
        .output()
        .map_err(|e| {
            Error::Internal(format!(
                "workspace.initializeRepository: git commit failed to spawn: {e}"
            ))
        })?;
    if !commit.status.success() {
        let out = String::from_utf8_lossy(&commit.stdout);
        if !out.contains("nothing to commit") {
            return Err(Error::Internal(format!(
                "workspace.initializeRepository: git commit failed: {}",
                String::from_utf8_lossy(&commit.stderr).trim()
            )));
        }
    }
    Ok(())
}

/// Blocking `workspace.delete` cleanup (ports the TS `removeGitWorktree`
/// body): capture the checked-out branch, remove the linked worktree (with
/// the manual-rm fallback and prune inside [`intent_git::worktree::remove_worktree`]),
/// drop the now-empty `<root>/<workspaceId>` parent directory, and delete the
/// workspace branch only when it passes every guard — it must be the branch
/// currently checked out there, it must be the branch the daemon
/// auto-generated at create time (never a caller-supplied branch), and never
/// `main`/`master` or a detached HEAD. Best-effort throughout.
fn cleanup_workspace_worktree(
    repo: &Path,
    worktree: &Path,
    branch: &str,
    branch_auto_generated: bool,
) {
    let checked_out = intent_git::worktree::worktree_branch(worktree);
    if let Err(e) = intent_git::worktree::remove_worktree(repo, worktree) {
        tracing::warn!(
            error = %e,
            worktree = %worktree.display(),
            "failed to remove git worktree"
        );
    }
    // The provisioned layout is `<root>/<workspaceId>/<repo-slug>` alongside
    // the daemon-written `<root>/<workspaceId>/.workspace/` metadata dir.
    // Remove the metadata dir first so the subsequent parent `remove_dir`
    // (which only deletes empty directories) can succeed. A caller-supplied
    // path shared with other content is still never destroyed.
    if let Some(parent) = worktree.parent() {
        let _ = std::fs::remove_dir_all(parent.join(".workspace"));
        let _ = std::fs::remove_dir(parent);
    }
    let deletable = branch_auto_generated
        && checked_out.as_deref() == Some(branch)
        && branch != "main"
        && branch != "master";
    if deletable {
        if let Err(e) = intent_git::branches::delete_local_branch(repo, branch) {
            tracing::debug!(error = %e, branch, "could not delete workspace branch");
        }
    } else if let Some(checked_out) = checked_out {
        tracing::info!(
            branch = %checked_out,
            "skipping branch deletion - not the auto-generated workspace branch"
        );
    }
}

/// Upsert into the registry every workspace that carries a `repository_path`
/// (TS `repo.list` one-time sync). Best-effort: callers ignore the result so a
/// sync failure never blocks/fails the `repo.list` response.
async fn sync_repos_from_workspaces(store: &Store) -> Result<()> {
    let workspaces = store.list_workspaces(true).await?;
    for ws in workspaces {
        let Some(repo_path) = ws.repository_path.as_deref() else {
            continue;
        };
        if repo_path.is_empty() {
            continue;
        }
        let name = known_repo_name(ws.repository_name.as_deref(), repo_path);
        store
            .upsert_known_repo(repo_path, &name, ws.repository_owner.as_deref())
            .await?;
    }
    Ok(())
}

/// Build a `note:created`/`note:updated`/`note:deleted` change event with the
/// TS-parity payload `{ noteId, title, action }` (`notes.service.ts`).
fn note_change_event(
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    title: &str,
    event_type: &str,
    action: &str,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "noteId": note_id.as_str(),
            "title": title,
            "action": action,
        }),
    }
}

/// Build a `task:status-changed` change event with the TS-parity payload
/// `{ noteId, noteTitle, previousStatus, newStatus, changedAt, agentId? }`
/// (`notes.service.ts`). When the change is agent-attributed (`agent` carries
/// the invoking agent's id + display name), the event uses an agent actor and
/// the payload includes `agentId`; otherwise the system actor leaves `agentId`
/// undefined, so it is omitted.
fn task_status_changed_event(
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    note_title: &str,
    previous_status: TaskStatus,
    new_status: TaskStatus,
    changed_at: &str,
    agent: Option<(String, Option<String>)>,
) -> NewEvent {
    let mut data = serde_json::json!({
        "noteId": note_id.as_str(),
        "noteTitle": note_title,
        "previousStatus": status_word(previous_status),
        "newStatus": status_word(new_status),
        "changedAt": changed_at,
    });
    let actor = match agent {
        Some((agent_id, name)) => {
            data["agentId"] = serde_json::json!(agent_id);
            intent_core::EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id),
                name,
                ..Default::default()
            }
        }
        None => system_actor(),
    };
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: TASK_STATUS_CHANGED.to_string(),
        actor,
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

/// True for the TS `TERMINAL_STATUSES` (`notes/utils/task-tree-utils.ts`): a
/// terminal task is excluded from the flattened ready-task traversal.
fn is_terminal_task_status(status: TaskStatus) -> bool {
    matches!(status, TaskStatus::Complete | TaskStatus::Cancelled)
}

/// Compute the ordered ready task note IDs for a workspace, porting
/// `flattenTaskTree` + `findReadyTasks` (`notes/utils/task-tree-utils.ts`):
/// non-terminal task notes in leaves-first post-order (by `peerOrder`, then
/// `createdAt`), keeping only those whose task children are all `complete`.
fn compute_ready_task_ids(notes: &[Note]) -> Vec<String> {
    // flattenTaskTree: only non-terminal task notes participate, keyed by parent.
    let mut children: HashMap<Option<&str>, Vec<&Note>> = HashMap::new();
    for n in notes {
        if n.metadata
            .task
            .as_ref()
            .is_some_and(|t| !is_terminal_task_status(t.status))
        {
            let parent = n.parent_id.as_ref().map(|p| p.as_str());
            children.entry(parent).or_default().push(n);
        }
    }
    if children.is_empty() {
        return Vec::new();
    }
    // Sort each sibling level by peerOrder (default 0), then createdAt (older first).
    for level in children.values_mut() {
        level.sort_by(|a, b| {
            let ao = a
                .metadata
                .task
                .as_ref()
                .and_then(|t| t.peer_order)
                .unwrap_or(0);
            let bo = b
                .metadata
                .task
                .as_ref()
                .and_then(|t| t.peer_order)
                .unwrap_or(0);
            ao.cmp(&bo).then_with(|| a.created_at.cmp(&b.created_at))
        });
    }
    // Post-order DFS (children before parent), starting at the root level.
    fn traverse<'a>(
        parent: Option<&'a str>,
        children: &HashMap<Option<&'a str>, Vec<&'a Note>>,
        out: &mut Vec<&'a Note>,
    ) {
        if let Some(level) = children.get(&parent) {
            for child in level {
                traverse(Some(child.id.as_str()), children, out);
                out.push(child);
            }
        }
    }
    let mut flattened: Vec<&Note> = Vec::new();
    traverse(None, &children, &mut flattened);

    // findReadyTasks: ready iff every task child (over ALL notes, including
    // terminal ones) is `complete`.
    let mut all_children: HashMap<&str, Vec<&Note>> = HashMap::new();
    for n in notes {
        if n.metadata.task.is_some() {
            if let Some(parent) = n.parent_id.as_ref() {
                all_children.entry(parent.as_str()).or_default().push(n);
            }
        }
    }
    flattened
        .into_iter()
        .filter(|n| {
            all_children
                .get(n.id.as_str())
                .map(|kids| {
                    kids.iter().all(|c| {
                        c.metadata
                            .task
                            .as_ref()
                            .map(|t| t.status == TaskStatus::Complete)
                            .unwrap_or(true)
                    })
                })
                .unwrap_or(true)
        })
        .map(|n| n.id.0.clone())
        .collect()
}

/// Build a `task:ready-tasks-changed` change event with the TS-parity payload
/// `{ readyTaskIds, triggeredBy: { noteId, previousStatus, newStatus },
/// computedAt }` (`notes.service.ts` `emitReadyTasksChanged`).
fn ready_tasks_changed_event(
    workspace_id: &WorkspaceId,
    ready_task_ids: Vec<String>,
    triggered_by_note: &NoteId,
    previous_status: TaskStatus,
    new_status: TaskStatus,
    computed_at: &str,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: TASK_READY_TASKS_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "readyTaskIds": ready_task_ids,
            "triggeredBy": {
                "noteId": triggered_by_note.as_str(),
                "previousStatus": status_word(previous_status),
                "newStatus": status_word(new_status),
            },
            "computedAt": computed_at,
        }),
    }
}

/// Build a `workspace:activity-changed` change event with the self-sufficient
/// payload `{ workspaceId, activity }` (PROTOCOL §6.5 / IMPLEMENTATION_SPEC §10.1).
fn activity_changed_event(workspace_id: &WorkspaceId, activity: WorkspaceActivity) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_ACTIVITY_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "activity": activity,
        }),
    }
}

/// Build a `workspace:attention-changed` change event with the self-sufficient
/// payload `{ workspaceId, attention }` (PROTOCOL §6.5 / IMPLEMENTATION_SPEC §10.1).
fn attention_changed_event(workspace_id: &WorkspaceId, attention: WorkspaceAttention) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_ATTENTION_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "attention": attention,
        }),
    }
}

/// Build a `settings:changed` event with the self-sufficient payload
/// `{ changes: [{ path, value }] }` carrying the **redacted** applied pairs
/// (PROTOCOL §6.5 / §9.8). Settings are global, so the event carries the empty
/// workspace id; subscribers that omit a `workspaceId` filter still receive it.
fn settings_changed_event(changes: Vec<serde_json::Value>) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId::from_string(String::new()),
        timestamp: now_iso(),
        event_type: SETTINGS_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "changes": changes }),
    }
}

/// Build a `comment:added` change event with the self-sufficient payload
/// `{ noteId, commentId }` (PROTOCOL §6.5; intentd carries the ids so a client
/// can locate/fetch the new comment).
fn comment_added_event(workspace_id: &WorkspaceId, note_id: &NoteId, comment_id: &str) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: COMMENT_ADDED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "noteId": note_id.as_str(),
            "commentId": comment_id,
        }),
    }
}

/// Build a `comment:resolved` event for a thread that was (un)resolved.
fn comment_resolved_event(
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    thread_id: &str,
    resolved: bool,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: COMMENT_RESOLVED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "noteId": note_id.as_str(),
            "threadId": thread_id,
            "resolved": resolved,
        }),
    }
}

/// Build a `workspace:created` event for a freshly inserted workspace row
/// (§6.5). Self-sufficient payload `{ workspaceId, workspace }` (§6.7) so a
/// client can render the new workspace without a follow-up read.
fn workspace_created_event(ws: &Workspace) -> NewEvent {
    NewEvent {
        workspace_id: ws.id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_CREATED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": ws.id.as_str(),
            "workspace": ws,
        }),
    }
}

/// Build a `workspace:updated` event for a workspace whose row was just
/// mutated (§6.5). Payload `{ workspaceId, changes }` mirrors the reference
/// FE emitter — `changes` is the caller-supplied `WorkspaceUpdate` diff (the
/// applied delta, not the full row) so clients see exactly which fields
/// moved.
fn workspace_updated_event(workspace_id: &WorkspaceId, changes: serde_json::Value) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_UPDATED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "changes": changes,
        }),
    }
}

/// Build a `workspace:deleted` event for a workspace whose row was just
/// removed (§6.5). Minimal payload `{ workspaceId }` matches the reference FE
/// emitter — subscribers drop caches keyed on the id without needing more.
fn workspace_deleted_event(workspace_id: &WorkspaceId) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_DELETED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
        }),
    }
}

/// Build a `workspace:context-changed` event carrying the new authoritative
/// context-item list (PROTOCOL §5.1 / §6.5). Self-sufficient payload
/// `{ workspaceId, items }` (§6.7) so subscribers refresh without a
/// follow-up `workspace.getContext`.
fn workspace_context_changed_event(workspace_id: &WorkspaceId, items: &[ContextItem]) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_CONTEXT_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "items": items,
        }),
    }
}

/// Build a `task:agent-linked` event for a freshly-persisted task↔agent
/// row (PROTOCOL §5.4 / §6.5). Self-sufficient payload
/// `{ workspaceId, noteId, taskKey, link }` (§6.7) so subscribers rebuild
/// the `byNoteId → byTaskKey` map without a follow-up read.
fn task_agent_linked_event(link: &TaskAgentLink) -> NewEvent {
    NewEvent {
        workspace_id: link.workspace_id.clone(),
        timestamp: now_iso(),
        event_type: TASK_AGENT_LINKED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": link.workspace_id.as_str(),
            "noteId": link.note_id.as_str(),
            "taskKey": link.task_key,
            "link": link,
        }),
    }
}

/// Build a `task:agent-unlinked` event for a task↔agent row that was
/// removed (PROTOCOL §5.4 / §6.5). Payload `{ workspaceId, noteId,
/// taskKey }` — enough for subscribers to drop the entry from the
/// `byNoteId → byTaskKey` map.
fn task_agent_unlinked_event(
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    task_key: &str,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: TASK_AGENT_UNLINKED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "noteId": note_id.as_str(),
            "taskKey": task_key,
        }),
    }
}

/// Build a `git:commit` event for a freshly-recorded git commit inside a
/// workspace worktree (§6.5). Payload `{ workspaceId, operation: "commit",
/// commit, message, files }` mirrors the reserved `GitOperationEvent` shape
/// in the FE `events/types.ts` — clients can render a commit-created marker
/// without a follow-up `git.log`.
fn git_commit_event(
    workspace_id: &WorkspaceId,
    commit_hash: &str,
    message: &str,
    files: &[String],
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: GIT_COMMIT.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "operation": "commit",
            "commit": commit_hash,
            "message": message,
            "files": files,
        }),
    }
}

/// Build a `pr:linked` event for a workspace that just gained a PR link (§7.6).
/// Self-sufficient payload `{ workspaceId, prNumber, prUrl, prStatus,
/// activePullRequest }` so a client can render the link without a follow-up read.
fn pr_linked_event(ws: &Workspace) -> NewEvent {
    NewEvent {
        workspace_id: ws.id.clone(),
        timestamp: now_iso(),
        event_type: PR_LINKED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": ws.id.as_str(),
            "prNumber": ws.pr_number,
            "prUrl": ws.pr_url,
            "prStatus": ws.pr_status,
            "activePullRequest": ws.active_pull_request,
        }),
    }
}

/// Build a `pr:updated` event for a linked PR whose persisted snapshot changed
/// (§7.6). Payload `{ workspaceId, prNumber, prStatus, activePullRequest }`.
fn pr_updated_event(ws: &Workspace) -> NewEvent {
    NewEvent {
        workspace_id: ws.id.clone(),
        timestamp: now_iso(),
        event_type: PR_UPDATED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": ws.id.as_str(),
            "prNumber": ws.pr_number,
            "prStatus": ws.pr_status,
            "activePullRequest": ws.active_pull_request,
        }),
    }
}

/// Build a `pr:unlinked` event for a workspace whose stale PR link was cleared
/// (§7.6). Self-sufficient payload `{ workspaceId }`.
fn pr_unlinked_event(workspace_id: &WorkspaceId) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: PR_UNLINKED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "workspaceId": workspace_id.as_str() }),
    }
}

/// Build a `workspace:tokenUsage-changed` event carrying the recomputed
/// `TokenUsage` snapshot (§5.23 / §6.5). Self-sufficient payload
/// `{ workspaceId, tokenUsage }` so the FE re-renders without a follow-up read.
fn token_usage_changed_event(workspace_id: &WorkspaceId, usage: &TokenUsage) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_TOKEN_USAGE_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "tokenUsage": usage,
        }),
    }
}

/// Wall-clock bound for a single `git.pull` operation before the daemon
/// short-circuits with a structured failure. Matches the TS handler's
/// `timeout: 120_000` on `git pull --rebase` (`git.ipc.ts` PULL_BRANCH), so
/// FE behaviour on a hung remote is unchanged.
const GIT_PULL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Run a blocking `git.pull`-style closure off the async runtime with an
/// operation timeout, returning the structured `GitPullResult { ok: false,
/// error }` shape on timeout so the FE contract (expected pull failures are
/// never JSON-RPC errors — PROTOCOL §5.6) is preserved.
///
/// When the outer timeout fires, `tokio::time::timeout` drops the `JoinHandle`
/// but the underlying blocking task keeps running on Tokio's blocking pool
/// until it observes something (network failure, callback `Err`, child exit).
/// For the normal shell-fetch path this is bounded: `intent_git::fetch::fetch`
/// enforces its own inner deadline (100s) below [`GIT_PULL_TIMEOUT`] and kills
/// the git child via `Child::kill`, so the blocking task terminates before the
/// outer timeout fires in the fetch-hang scenario. The remaining libgit2 code
/// paths inside `pull_branch` (stash/rebase/pop) are all local disk ops that
/// complete in milliseconds; the bounded credential callback in
/// `intent_git::auth` is defence-in-depth for the auth-loop shape.
async fn git_pull_bounded<F>(
    op: F,
    timeout: std::time::Duration,
) -> Result<intent_core::GitPullResult>
where
    F: FnOnce() -> Result<intent_core::GitPullResult> + Send + 'static,
{
    let handle = tokio::task::spawn_blocking(op);
    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(res)) => res,
        Ok(Err(join_err)) => Err(Error::Internal(format!("git.pull task failed: {join_err}"))),
        Err(_elapsed) => Ok(intent_core::GitPullResult {
            ok: false,
            error: Some(format!("git.pull timed out after {}s", timeout.as_secs())),
        }),
    }
}

/// Run [`intent_git::fetch::fetch`] on the blocking pool so the accept-changes
/// async flows (`ac_undo_push`, `ac_reset_to_trunk`, `ac_rebase_onto_trunk`,
/// `ac_merge_to_trunk`) do not pin a Tokio runtime worker for the 100s inner
/// fetch deadline on a hung remote — the same runtime-saturation vector
/// [`git_pull_bounded`] closes for `git.pull`. `intent_git::fetch::fetch` owns
/// its own wall-clock deadline + `Child::kill`, so no outer timeout is needed
/// here; the wrapper just moves the blocking call off the runtime worker.
async fn git_fetch_bounded(worktree: &std::path::Path, remote: &str, branch: &str) -> Result<()> {
    let worktree = worktree.to_path_buf();
    let remote = remote.to_string();
    let branch = branch.to_string();
    tokio::task::spawn_blocking(move || intent_git::fetch::fetch(&worktree, &remote, &branch))
        .await
        .map_err(|e| Error::Internal(format!("git.fetch task failed: {e}")))?
}

/// Build a `git:pull` event for a completed pull inside a workspace worktree
/// (§6.5). Payload `{ workspaceId, operation: "pull", branch }` mirrors the
/// reserved `GitOperationEvent` shape in the FE `events/types.ts` so a client
/// can render a pull marker without a follow-up read. Only emitted on success
/// (the FE's structured `{ ok: false, error }` return already carries the
/// failure detail — no event fires in that case).
fn git_pull_event(workspace_id: &WorkspaceId, branch: &str) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: GIT_PULL.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "operation": "pull",
            "branch": branch,
        }),
    }
}

/// Build a `git:push` event for a completed push inside a workspace worktree
/// (§6.5). Payload `{ workspaceId, operation: "push", branch, commit, force }`
/// mirrors the reserved `GitOperationEvent` shape. Only emitted on success.
fn git_push_event(
    workspace_id: &WorkspaceId,
    branch: &str,
    pushed_sha: &str,
    force: bool,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: GIT_PUSH.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "operation": "push",
            "branch": branch,
            "commit": pushed_sha,
            "force": force,
        }),
    }
}

/// Build a `git:branch` event for a completed branch operation inside a
/// workspace worktree (§6.5). Payload `{ workspaceId, operation: "branch",
/// branchOp, branch, oldBranch? }` — `operation` mirrors the reserved
/// `GitOperationEvent.data.operation` union (`commit|push|pull|branch|merge`),
/// while `branchOp` disambiguates between `"create"`, `"checkout"`, and
/// `"rename"` for clients that render per-sub-op markers.
fn git_branch_event(
    workspace_id: &WorkspaceId,
    branch_op: &str,
    branch: &str,
    old_branch: Option<&str>,
) -> NewEvent {
    let mut data = serde_json::json!({
        "workspaceId": workspace_id.as_str(),
        "operation": "branch",
        "branchOp": branch_op,
        "branch": branch,
    });
    if let Some(old) = old_branch {
        data["oldBranch"] = serde_json::json!(old);
    }
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: GIT_BRANCH.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

/// Build a `changes:git-status` event carrying the refreshed `WorkspaceGitStatus`
/// (§5.18, §6.5). Self-sufficient payload `{ workspaceId, status }`.
fn changes_git_status_event(workspace_id: &WorkspaceId, status: serde_json::Value) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: CHANGES_GIT_STATUS.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "status": status,
        }),
    }
}

/// Build a `changes:metrics-changed` event carrying the recomputed workspace
/// `Metrics` (§5.20, §6.5). Self-sufficient payload `{ workspaceId, metrics }`.
fn changes_metrics_changed_event(
    workspace_id: &WorkspaceId,
    metrics: serde_json::Value,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: CHANGES_METRICS_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "metrics": metrics,
        }),
    }
}

/// Delay between streamed `search:result` batches. Small enough to stay
/// imperceptible, large enough that an in-flight `search.cancel` reliably halts
/// further batches mid-stream (§14.3).
const STREAM_BATCH_DELAY: std::time::Duration = std::time::Duration::from_millis(20);

/// Serialize a list of typed search matches into the wire `Vec<Value>` used for
/// inline results and streamed batches.
fn to_value_vec<T: serde::Serialize>(items: Vec<T>) -> Result<Vec<serde_json::Value>> {
    items
        .into_iter()
        .map(|m| {
            serde_json::to_value(m)
                .map_err(|e| Error::Internal(format!("serialize search match failed: {e}")))
        })
        .collect()
}

/// Build a `search:result` streaming event carrying one batch of matches
/// (`data: { requestId, matches }`), correlated by `requestId` (§5.15 / §6.5).
fn search_result_event(
    workspace_id: &WorkspaceId,
    request_id: &str,
    matches: &[serde_json::Value],
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: SEARCH_RESULT.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "requestId": request_id,
            "matches": matches,
        }),
    }
}

/// Build the terminal `search:done` streaming event (`data: { requestId, total,
/// truncated }`), correlated by `requestId` (§5.15 / §6.5).
fn search_done_event(
    workspace_id: &WorkspaceId,
    request_id: &str,
    total: usize,
    truncated: bool,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: SEARCH_DONE.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "requestId": request_id,
            "total": total,
            "truncated": truncated,
        }),
    }
}

/// Publish a change event onto the bus when one is wired, logging (not failing)
/// on error — the durable mutation has already succeeded by this point.
pub(crate) async fn publish_event(bus: &Option<EventBus>, event: NewEvent) {
    let Some(bus) = bus else {
        return;
    };
    if let Err(e) = bus.publish(&event).await {
        tracing::warn!(error = %e, "failed to publish change event");
    }
}

/// Best-effort workspace lookup by worktree path (§6.5). Path-scoped git
/// methods (e.g. `git.pull` during workspace-create auto-pull) reach this to
/// resolve an owning workspace for change-event emission; missing rows are a
/// normal path (auto-pull runs before workspace insert) so `None` is returned
/// without an error. Compares canonicalized paths so trailing-slash / `.`
/// segments don't cause a false miss. Includes archived rows so a pull inside
/// an archived workspace's worktree still resolves.
async fn find_workspace_by_worktree_path(store: &Store, repo_path: &str) -> Option<Workspace> {
    let canon_target = std::fs::canonicalize(repo_path).ok()?;
    let workspaces = store.list_workspaces(true).await.ok()?;
    workspaces.into_iter().find(|ws| {
        ws.worktree_path
            .as_deref()
            .and_then(|p| std::fs::canonicalize(p).ok())
            .map(|p| p == canon_target)
            .unwrap_or(false)
    })
}

/// Idempotency wrapper for create/commit/PR-merge methods (design note TB-0 §5).
///
/// When `key` is present and already recorded for `(workspace_id, key)`, returns
/// the original stored result without running `op` (so no second event emission).
/// On a miss, runs `op`, persists the serialized success result under the key,
/// and returns it (errors are not cached). When `key` is absent this is a
/// SOFT-LAUNCH (R5): log a warn and execute normally — never reject.
///
/// `workspace_id` is the `""` sentinel for global methods that carry no
/// workspaceId (e.g. `workspace.create`).
pub(crate) async fn with_idempotency<T, F, Fut>(
    store: &Store,
    workspace_id: &str,
    key: Option<String>,
    method: &str,
    op: F,
) -> Result<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let Some(key) = key else {
        tracing::warn!(
            method,
            "idempotencyKey missing on idempotent method; executing without dedupe (soft-launch)"
        );
        return op().await;
    };
    if let Some(stored) = store.get_idempotent(workspace_id, &key).await? {
        let value: T = serde_json::from_str(&stored)
            .map_err(|e| Error::Internal(format!("decode idempotent result failed: {e}")))?;
        return Ok(value);
    }
    let result = op().await?;
    let result_json = serde_json::to_string(&result)
        .map_err(|e| Error::Internal(format!("encode idempotent result failed: {e}")))?;
    store
        .put_idempotent(workspace_id, &key, method, &result_json)
        .await?;
    Ok(result)
}

/// Extract the completed child agent id from a completion event: the canonical
/// data.agentId, falling back to the event actor id when present.
fn completion_event_child_id(event: &Event) -> Option<String> {
    event
        .data
        .get("agentId")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| event.actor.id.clone())
}

/// Build a concise, human-readable wake string describing a child agent's
/// completion for its parent. A minimal port of the TS formatEventNotification
/// intent: it names the child, the completion kind, and any completion
/// `report` (persisted by `agent.reportToParent` and forwarded on
/// `agent:idle`) or `lastResponseSummary` / error carried on the event. A
/// non-empty completion report wins over `lastResponseSummary` (SUB-2,
/// mirroring `format_group_child_line`) so the single agent:idle-driven wake
/// carries the child's `reportToParent` text end-to-end.
fn format_completion_wake(child_id: &AgentId, event: &Event) -> String {
    let kind = match event.event_type.as_str() {
        AGENT_IDLE => "completed",
        AGENT_FAILED => "failed",
        AGENT_DELETED => "was deleted",
        other => other,
    };
    let label = event
        .data
        .get("agentName")
        .and_then(|v| v.as_str())
        .or(event.actor.name.as_deref())
        .map(|name| format!("{name} ({})", child_id.0))
        .unwrap_or_else(|| child_id.0.clone());
    let mut msg = format!("[WORKSPACE EVENTS] Child agent {label} {kind}.");
    if let Some(report) = event
        .data
        .get("report")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        msg.push_str(&format!(" Report: {report}"));
    } else if let Some(summary) = event
        .data
        .get("lastResponseSummary")
        .and_then(|v| v.as_str())
    {
        if !summary.is_empty() {
            msg.push_str(&format!(" Summary: {summary}"));
        }
    }
    if let Some(err) = event.data.get("error").and_then(|v| v.as_str()) {
        if !err.is_empty() {
            msg.push_str(&format!(" Error: {err}"));
        }
    }
    msg
}

/// Build one per-child summary line for a delegation group's aggregated wake.
/// A compact sibling of [`format_completion_wake`] without the standalone
/// `[WORKSPACE EVENTS]` framing, since the group header carries that. A
/// non-empty `completion_report` (the child's persisted
/// `metadata.completionReport`) wins over the event's `lastResponseSummary`,
/// mirroring the TS event-notification formatter.
fn format_group_child_line(
    child_id: &AgentId,
    event: &Event,
    completion_report: Option<&str>,
) -> String {
    let kind = match event.event_type.as_str() {
        AGENT_IDLE => "completed",
        AGENT_FAILED => "failed",
        AGENT_DELETED => "was deleted",
        other => other,
    };
    let label = event
        .data
        .get("agentName")
        .and_then(|v| v.as_str())
        .or(event.actor.name.as_deref())
        .map(|name| format!("{name} ({})", child_id.0))
        .unwrap_or_else(|| child_id.0.clone());
    let mut line = format!("- {label} {kind}.");
    if let Some(report) = completion_report.filter(|r| !r.is_empty()) {
        line.push_str(&format!(" Report: {report}"));
    } else if let Some(summary) = event
        .data
        .get("lastResponseSummary")
        .and_then(|v| v.as_str())
    {
        if !summary.is_empty() {
            line.push_str(&format!(" Summary: {summary}"));
        }
    }
    if let Some(err) = event.data.get("error").and_then(|v| v.as_str()) {
        if !err.is_empty() {
            line.push_str(&format!(" Error: {err}"));
        }
    }
    line
}

/// Build the single aggregated wake for a settled after_all delegation group: a
/// header with the child count and completionStatus (`partial` when any child was
/// deleted, else `completed`) followed by the accumulated per-child lines.
fn format_group_wake(group: &agent_subscriptions::DelegationGroup) -> String {
    let total = group.expected_agent_ids.len();
    let partial = !group.deleted_agent_ids.is_empty();
    let status = if partial { "partial" } else { "completed" };
    let mut msg = format!(
        "[WORKSPACE EVENTS] All {total} delegated child agent(s) settled (completionStatus: {status})."
    );
    for line in &group.event_summaries {
        msg.push('\n');
        msg.push_str(line);
    }
    msg
}

/// Outcome of the best-effort auto-conversion of `@@@task` blocks that runs
/// after every note content-write path (reference `notes.service.ts` parity,
/// L633-647): populated when the just-written content contained at least one
/// `@@@task` fence and the conversion succeeded.
#[derive(Default)]
struct AutoConvertOutcome {
    converted_count: i64,
    created_note_ids: Vec<String>,
    /// The full note refetched from the store after the conversion ran.
    /// `convert_task_blocks` performs a second store write (bumping `rev` and
    /// `updated_at`), so callers must return this refetched note — not the
    /// pre-conversion in-flight value — to match the reference, which
    /// refetches the note after conversion.
    refetched_note: Option<Note>,
}

impl Services {
    /// Best-effort auto-conversion of `@@@task` blocks after a successful
    /// note content write, matching the reference `notes.service.ts` update
    /// path (L633-647): if the just-written content contains a `@@@task`
    /// fence, invoke `convert_task_blocks` and refetch the stored note so
    /// callers can return it in place of their in-flight (now stale) copy.
    /// Never fails the write — errors are logged and treated as no-op.
    ///
    /// `caller_agent_id` is threaded through so the conversion's version
    /// snapshot inherits the outer write's author (reference parity).
    async fn auto_convert_task_blocks_after_write(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        content_after_write: &str,
        caller_agent_id: Option<&AgentId>,
    ) -> AutoConvertOutcome {
        if !note_ops::has_task_blocks(content_after_write) {
            return AutoConvertOutcome::default();
        }
        match self
            .convert_task_blocks_op(workspace_id.clone(), note_id.clone(), caller_agent_id)
            .await
        {
            Ok(r) => {
                let refetched_note = self.store.get_note(workspace_id, note_id).await.ok();
                AutoConvertOutcome {
                    converted_count: r.converted_count,
                    created_note_ids: r.created_note_ids,
                    refetched_note,
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    workspace_id = %workspace_id,
                    note_id = %note_id,
                    "auto-conversion of task blocks after note write failed"
                );
                AutoConvertOutcome::default()
            }
        }
    }

    /// Shared body for the `task.convertBlocks` trait method and the
    /// `auto_convert_task_blocks_after_write` post-write hook. Threading
    /// `caller_agent_id` here lets both the explicit RPC call (currently
    /// `None`) and the note-write auto-conversion (which forwards the outer
    /// mutation's caller) attribute the resulting "Converted task blocks"
    /// version snapshot to the acting agent when applicable.
    async fn convert_task_blocks_op(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        caller_agent_id: Option<&AgentId>,
    ) -> Result<TaskConvertBlocksResult> {
        let store = &self.store;
        let mut note = fetch_note_peer(store, &workspace_id, &note_id).await?;
        if note.content.is_empty() {
            return Ok(TaskConvertBlocksResult {
                ok: true,
                converted_count: 0,
                created_note_ids: Vec::new(),
            });
        }
        // Mirror the TS guard: only parse when a `@@@task` fence exists.
        let parsed = if note_ops::has_task_blocks(&note.content) {
            note_ops::extract_task_blocks(&note.content)
        } else {
            note_ops::TaskBlocksResult {
                tasks: Vec::new(),
                content_without_blocks: note.content.clone(),
            }
        };
        // Idempotency: map existing child note titles (normalized) → id.
        let all = store.list_notes(&workspace_id).await?;
        let mut existing_by_title: std::collections::HashMap<String, NoteId> = all
            .iter()
            .filter(|n| n.parent_id.as_ref() == Some(&note.id))
            .map(|n| (n.title.trim().to_lowercase(), n.id.clone()))
            .collect();

        // Start from the placeholder-substituted content; each valid block
        // is `<!-- task-block-placeholder-{i} -->` to be replaced below.
        let mut working = parsed.content_without_blocks.clone();
        let mut created_note_ids: Vec<String> = Vec::new();
        let mut peer_order = 100i64;
        for (i, task) in parsed.tasks.iter().enumerate() {
            let body = if task.content.is_empty() {
                format!("# {}\n\nCreated as a prerequisite task.", task.title)
            } else {
                format!("# {}\n\n{}", task.title, task.content)
            };
            let normalized = task.title.trim().to_lowercase();
            let task_note_id = match existing_by_title.get(&normalized) {
                Some(existing_id) => existing_id.clone(),
                None => {
                    let child = self
                        .create_child_task_note(
                            &workspace_id,
                            &note.id,
                            &task.title,
                            body,
                            TaskStatus::NotStarted,
                            Some(peer_order),
                        )
                        .await?;
                    existing_by_title.insert(normalized, child.id.clone());
                    created_note_ids.push(child.id.0.clone());
                    child.id
                }
            };
            let placeholder = format!("<!-- task-block-placeholder-{i} -->");
            let linked = format!(
                "- [ ] [{}](intent://local/task/{})",
                task.title, task_note_id.0
            );
            working = working.replace(&placeholder, &linked);
            peer_order += 100;
        }

        let content_changed = working != note.content;
        if !content_changed && created_note_ids.is_empty() {
            return Ok(TaskConvertBlocksResult {
                ok: true,
                converted_count: 0,
                created_note_ids: Vec::new(),
            });
        }
        note.content = working;
        note.updated_at = now_iso();
        store.update_note(&note).await?;
        // TS parity: the reference pushes a version snapshot ("Converted
        // task blocks to linked Task Notes") as part of the conversion
        // save, so the newest stored version matches the fence-free
        // content that line-attribution/history consumers diff against.
        let author = resolve_note_version_author(store, caller_agent_id).await;
        capture_note_version(store, &note, &author).await?;
        self.invalidate_crdt_note(&note.workspace_id, &note.id);
        self.schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
        // Emit `note:updated` for the rewritten parent so subscribers
        // refresh the fence-free content live (TS parity: the reference
        // emits `note:updated` after saving the converted note).
        publish_event(
            &self.event_bus,
            note_change_event(
                &note.workspace_id,
                &note.id,
                &note.title,
                NOTE_UPDATED,
                "update",
            ),
        )
        .await;
        Ok(TaskConvertBlocksResult {
            ok: true,
            converted_count: created_note_ids.len() as i64,
            created_note_ids,
        })
    }

    /// Create a child task note nested under `parent_id`, marking it a task with
    /// `status` (and optional `peer_order`). Shared by `createPrerequisite` and
    /// `convertBlocks`.
    async fn create_child_task_note(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &NoteId,
        title_raw: &str,
        content: String,
        status: TaskStatus,
        peer_order: Option<i64>,
    ) -> Result<Note> {
        let now = now_iso();
        let note = Note {
            id: NoteId::new(),
            workspace_id: workspace_id.clone(),
            title: note_ops::strip_markdown_formatting(title_raw),
            content,
            content_type: ContentType::Markdown,
            tags: Vec::new(),
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: Some(parent_id.clone()),
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata {
                task: Some(fresh_task_metadata(status, &now, peer_order)),
            },
            created_at: now.clone(),
            rev: 0,
            updated_at: now,
        };
        self.store.insert_note(&note).await?;
        // Emit `note:created` so task-channel subscribers (spec UI) pick up
        // the new child task note live (TS parity: `createPrerequisiteNote`
        // routes through `createNote`, which emits `note:created`).
        publish_event(
            &self.event_bus,
            note_change_event(
                &note.workspace_id,
                &note.id,
                &note.title,
                NOTE_CREATED,
                "create",
            ),
        )
        .await;
        Ok(note)
    }

    /// Deliver a store-adapter search result (§5.15 / §6.5). Small sets (or any
    /// search with no event bus / no `workspaceId` to stream over) are returned
    /// inline as `{ requestId, matches }`. Larger sets return a prompt
    /// `{ requestId, matches: [] }` ack and spawn a background task that pushes
    /// `search:result` batches followed by a terminal `search:done`, checking the
    /// cancellation token before each batch so an in-flight `search.cancel` stops
    /// further batches mid-stream. The token is always unregistered once settled.
    fn deliver_search(
        &self,
        request_id: String,
        workspace_id: Option<WorkspaceId>,
        matches: Vec<serde_json::Value>,
        token: intent_search::CancelToken,
    ) -> serde_json::Value {
        let registry = self.search_cancels.clone();
        let stream_target = match (&self.event_bus, &workspace_id) {
            (Some(bus), Some(ws)) if matches.len() > search_ops::INLINE_THRESHOLD => {
                Some((bus.clone(), ws.clone()))
            }
            _ => None,
        };
        let Some((bus, ws)) = stream_target else {
            registry.unregister(&request_id);
            return serde_json::json!({ "requestId": request_id, "matches": matches });
        };
        let stream_request_id = request_id.clone();
        tokio::spawn(async move {
            let mut emitted = 0usize;
            let mut cancelled = false;
            for chunk in matches.chunks(search_ops::STREAM_BATCH_SIZE) {
                if token.is_cancelled() {
                    cancelled = true;
                    break;
                }
                if let Err(e) = bus
                    .publish(&search_result_event(&ws, &stream_request_id, chunk))
                    .await
                {
                    tracing::warn!(error = %e, "failed to publish search:result");
                }
                emitted += chunk.len();
                tokio::time::sleep(STREAM_BATCH_DELAY).await;
            }
            if let Err(e) = bus
                .publish(&search_done_event(
                    &ws,
                    &stream_request_id,
                    emitted,
                    cancelled,
                ))
                .await
            {
                tracing::warn!(error = %e, "failed to publish search:done");
            }
            registry.unregister(&stream_request_id);
        });
        serde_json::json!({ "requestId": request_id, "matches": [] })
    }

    /// Apply server runtime control hooks after `settings.update` persists
    /// `server.wsApi.*` changes (§5.12): `server.wsApi.enabled` starts/stops the
    /// WSS listener; `server.wsApi.port` restarts it when running.
    /// Returns an error if the operation fails (e.g., TCP client trying to disable
    /// the WSS listener, or listener start failure), allowing the caller to rollback.
    ///
    /// **Ordering rule**: Hooks are applied in dependency-aware, deterministic order:
    /// - Value-setting keys (server.wsApi.port, server.locality) apply FIRST (priority 0).
    /// - server.wsApi.enabled applies SECOND (priority 10) — starts/stops the WSS listener.
    /// - Within the same priority tier, keys are processed in lexicographic order.
    /// - Single-key updates have zero behavior change.
    /// - This ensures batches like {server.wsApi.port=NEW, server.wsApi.enabled=true}
    ///   start the listener on the NEW port, not the old one.
    async fn apply_server_setting_hooks(
        &self,
        applied: &[serde_json::Value],
        control: &Arc<dyn intent_core::ServerControl>,
    ) -> Result<()> {
        /// Priority for hook application order: lower values apply first.
        fn hook_priority(path: &str) -> u8 {
            match path {
                // Listener enable/disable applies after value-setting keys
                "server.wsApi.enabled" => 10,
                // Value-setting keys (port, locality) apply first
                _ if path.starts_with("server.") => 0,
                // Non-server keys (no hooks, sorted lexicographically within this tier)
                _ => 255,
            }
        }

        // Sort changes by (priority, path) to ensure deterministic, dependency-aware order.
        // Use stable sort so duplicate paths maintain their input order (tie-breaker).
        let mut sorted_changes: Vec<&serde_json::Value> = applied.iter().collect();
        sorted_changes.sort_by(|a, b| {
            let path_a = a.get("path").and_then(|v| v.as_str()).unwrap_or("");
            let path_b = b.get("path").and_then(|v| v.as_str()).unwrap_or("");
            (hook_priority(path_a), path_a).cmp(&(hook_priority(path_b), path_b))
        });

        for change in sorted_changes {
            if let Some(path) = change.get("path").and_then(|v| v.as_str()) {
                match path {
                    "server.wsApi.port" => {
                        if let Some(new_port) = change.get("value").and_then(|v| v.as_f64()) {
                            let port = new_port as u16;
                            // Check if listener is running
                            if let Some(current_port) = control.ws_listener_port().await {
                                // Listener is running: restart it on the new port
                                tracing::info!(
                                    old_port = current_port,
                                    new_port = port,
                                    "server.wsApi.port → {}: restarting WSS listener",
                                    port
                                );
                                control.stop_ws_listener().await;
                                let bound_port = control.start_ws_listener().await.map_err(|e| {
                                    tracing::error!(
                                        error = ?e,
                                        port = port,
                                        "server.wsApi.port → {}: failed to restart WSS listener", port
                                    );
                                    Error::Internal(format!("failed to restart WSS listener on port {}: {}", port, e))
                                })?;
                                tracing::info!(
                                    port = bound_port,
                                    "server.wsApi.port → {}: restarted WSS listener",
                                    port
                                );
                            } else {
                                // Listener is not running: persisting the value is enough
                                tracing::info!(
                                    port = port,
                                    "server.wsApi.port → {}: persisted (listener not running)",
                                    port
                                );
                            }
                        }
                    }
                    "server.wsApi.enabled" => {
                        if let Some(enabled) = change.get("value").and_then(|v| v.as_bool()) {
                            if enabled {
                                // Attempt to start the listener
                                let port = control.start_ws_listener().await.map_err(|e| {
                                    tracing::error!(
                                        error = ?e,
                                        "server.wsApi.enabled → true: failed to start WSS listener"
                                    );
                                    Error::Internal(format!("failed to start WSS listener: {}", e))
                                })?;
                                tracing::info!(
                                    port,
                                    "server.wsApi.enabled → true: started WSS listener"
                                );
                            } else {
                                // Guard: refuse to stop the listener from a TCP connection
                                if control.is_tcp_connection() {
                                    return Err(Error::InvalidParams(
                                        "cannot disable server.wsApi.enabled from a TCP connection (would self-terminate)".to_string()
                                    ));
                                }
                                control.stop_ws_listener().await;
                                tracing::info!(
                                    "server.wsApi.enabled → false: stopped WSS listener"
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

impl WorkspaceApi for Services {
    fn settings_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.settings_service().list().await })
    }

    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.settings_service().get(&path).await })
    }

    fn settings_update(
        &self,
        changes: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // Capture old values for ALL settings in the batch so we can rollback on hook failure.
            // Store holds non-sensitive settings; secrets holds sensitive ones (§9.8).
            // Fail closed: any read error during snapshot capture aborts the whole batch.
            let old_values = if let Some(entries) = changes.as_array() {
                let mut old = Vec::new();
                for entry in entries {
                    if let Some(path) = entry.get("path").and_then(|v| v.as_str()) {
                        // Look up the definition to check if this setting is sensitive
                        if let Some(def) = crate::settings::find_definition(path) {
                            if def.sensitive {
                                // Sensitive setting: capture from secrets store.
                                // Fail closed: timeout/backing-error -> abort before applying anything.
                                match self.secrets.load(path).await {
                                    Ok(Some(secret_val)) => {
                                        old.push((path.to_string(), Some(secret_val), true));
                                    }
                                    Ok(None) => {
                                        // Confirmed absent; mark for deletion on rollback.
                                        old.push((path.to_string(), None, true));
                                    }
                                    Err(e) => {
                                        return Err(Error::Internal(format!(
                                            "settings.update: failed to read secret {path} during snapshot capture: {e}"
                                        )));
                                    }
                                }
                            } else {
                                // Non-sensitive setting: capture from DB
                                match self.store.get_setting(path).await {
                                    Ok(Some(raw)) => {
                                        old.push((path.to_string(), Some(raw), false));
                                    }
                                    Ok(None) => {
                                        // Confirmed absent; mark for deletion on rollback.
                                        old.push((path.to_string(), None, false));
                                    }
                                    Err(e) => {
                                        return Err(Error::Internal(format!(
                                            "settings.update: failed to read setting {path} during snapshot capture: {e}"
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
                old
            } else {
                Vec::new()
            };

            let applied = self.settings_service().update(&changes).await?;
            if !applied.is_empty() {
                // Apply server runtime hooks (§5.12): start/stop the WSS listener
                // when server.wsApi.enabled changes, restart it when
                // server.wsApi.port changes while running.
                if let Some(control) = self.server_control.get() {
                    if let Err(e) = self.apply_server_setting_hooks(&applied, control).await {
                        // Rollback: restore old values for ALL settings in the batch.
                        // Log rollback failures but don't let them mask the original hook error.
                        let mut rollback_failed = false;
                        let mut compensating_changes = Vec::new();
                        for (path, old_val, is_sensitive) in old_values {
                            let rollback_result = if is_sensitive {
                                // Sensitive setting: restore to secrets store or delete
                                if let Some(val) = &old_val {
                                    self.secrets.store(&path, val).await
                                } else {
                                    self.secrets.delete(&path).await
                                }
                            } else {
                                // Non-sensitive setting: restore to DB or delete
                                if let Some(val) = &old_val {
                                    self.store.set_setting(&path, val).await
                                } else {
                                    self.store.delete_setting(&path).await.map(|_| ())
                                }
                            };
                            if let Err(rollback_err) = rollback_result {
                                tracing::error!(
                                    path = %path,
                                    error = %rollback_err,
                                    "settings.update rollback failed for key"
                                );
                                rollback_failed = true;
                            } else {
                                // Build a change record for compensating hook application.
                                // For settings with a prior persisted value, use that value.
                                // For unset settings (old_val == None), use the schema default
                                // so the compensating hook can restore runtime state (e.g., if
                                // server.wsApi.enabled was unset/defaulting to false, and the
                                // batch temporarily enabled it before failing, compensating hook
                                // with default=false will stop the listener).
                                let val_json = if let Some(val) = old_val {
                                    serde_json::from_str(&val).unwrap_or(serde_json::Value::Null)
                                } else if let Some(def) = crate::settings::find_definition(&path) {
                                    def.default_value.unwrap_or(serde_json::Value::Null)
                                } else {
                                    serde_json::Value::Null
                                };
                                compensating_changes.push(serde_json::json!({
                                    "path": path,
                                    "value": val_json
                                }));
                            }
                        }

                        // Apply compensating hooks: re-apply the prior values through the same
                        // hook path to restore runtime state (e.g., stop a listener that was
                        // started before a later hook in the batch failed). This reuses the
                        // deterministic priority-sorted dispatch from apply_server_setting_hooks.
                        if !compensating_changes.is_empty() {
                            if let Err(compensating_err) = self
                                .apply_server_setting_hooks(&compensating_changes, control)
                                .await
                            {
                                tracing::error!(
                                    error = ?compensating_err,
                                    "settings.update compensating hook application failed during rollback"
                                );
                                // Log but don't fail — the persistence rollback succeeded
                            }
                        }

                        // Return an error that indicates incomplete rollback if any writes failed
                        if rollback_failed {
                            return Err(Error::Internal(format!(
                                "settings.update hook failed ({e}), and rollback was incomplete (see logs)"
                            )));
                        }
                        // Return the hook error to the caller
                        return Err(e);
                    }
                }
                publish_event(&self.event_bus, settings_changed_event(applied.clone())).await;
            }
            Ok(serde_json::json!({ "applied": applied }))
        })
    }

    fn settings_reset(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let result = self.settings_service().reset(&path).await?;
            publish_event(
                &self.event_bus,
                settings_changed_event(vec![result.clone()]),
            )
            .await;
            Ok(result)
        })
    }

    fn rules_list(
        &self,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let path = match &workspace_id {
                Some(ws) => ft_worktree(&self.store, ws).await,
                None => None,
            };
            rules::RulesService::new(&self.store)
                .list(
                    workspace_id.as_ref().map(WorkspaceId::as_str),
                    path.as_deref(),
                )
                .await
        })
    }

    fn rules_get(
        &self,
        _workspace_id: WorkspaceId,
        rule_type: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { rules::RulesService::new(&self.store).get(&rule_type).await })
    }

    fn rules_update(
        &self,
        workspace_id: WorkspaceId,
        rule_type: String,
        content: String,
        enabled: Option<bool>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let path = ft_worktree(&self.store, &workspace_id).await;
            let (rules, changed) = rules::RulesService::new(&self.store)
                .update(
                    &rule_type,
                    &content,
                    enabled,
                    Some(workspace_id.as_str()),
                    path.as_deref(),
                )
                .await?;
            publish_event(&self.event_bus, settings_changed_event(vec![changed])).await;
            Ok(rules)
        })
    }

    fn specialist_list(
        &self,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.specialists_service()
                .list(workspace_path.as_deref().map(Path::new))
        })
    }

    fn specialist_get(
        &self,
        id: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.specialists_service()
                .get(&id, workspace_path.as_deref().map(Path::new))
        })
    }

    fn specialist_create(
        &self,
        id: String,
        spec: serde_json::Value,
        scope: Option<String>,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.specialists_service().create(
                &id,
                &spec,
                scope.as_deref(),
                workspace_path.as_deref().map(Path::new),
            )
        })
    }

    fn specialist_edit(
        &self,
        id: String,
        spec: serde_json::Value,
        scope: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.specialists_service().edit(
                &id,
                &spec,
                &scope,
                workspace_path.as_deref().map(Path::new),
            )
        })
    }

    fn specialist_delete(
        &self,
        id: String,
        scope: String,
        workspace_path: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.specialists_service()
                .delete(&id, &scope, workspace_path.as_deref().map(Path::new))
        })
    }

    fn mcp_servers_list(
        &self,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.mcp_servers_service()
                .list(workspace_id.as_ref().map(WorkspaceId::as_str))
                .await
        })
    }

    fn mcp_servers_create(
        &self,
        config: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_servers_service().create(config).await })
    }

    fn mcp_servers_update(
        &self,
        server_id: String,
        config: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_servers_service().update(&server_id, config).await })
    }

    fn mcp_servers_delete(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_servers_service().delete(&server_id).await })
    }

    fn mcp_servers_toggle(
        &self,
        server_id: String,
        enabled: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_servers_service().toggle(&server_id, enabled).await })
    }

    fn mcp_servers_restart(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_servers_service().restart(&server_id).await })
    }

    fn mcp_servers_get_status(
        &self,
        server_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_servers_service().get_status(&server_id).await })
    }

    fn mcp_oauth_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_oauth_service().list().await })
    }

    fn mcp_oauth_get(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_oauth_service().get(&server_id).await })
    }

    fn mcp_oauth_set(
        &self,
        server_id: String,
        token_bag: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_oauth_service().set(&server_id, token_bag).await })
    }

    fn mcp_oauth_delete(&self, server_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.mcp_oauth_service().delete(&server_id).await })
    }

    fn search_in_files(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        opts: Option<serde_json::Value>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let registry = self.search_cancels.clone();
        Box::pin(async move {
            let opts = search_ops::parse_opts(opts)?;
            let request_id = request_id.unwrap_or_else(intent_search::mint_request_id);
            let root = match search_ops::search_root(&store, &workspace_id, None).await? {
                Some(root) => root,
                None => {
                    return Ok(serde_json::json!({
                        "requestId": request_id,
                        "matches": [],
                        "truncated": false,
                    }))
                }
            };
            // Validate opts (incl. regex) before registering, so a bad regex is
            // surfaced as InvalidParams without leaving a stale cancel token.
            let token = registry.register(&request_id);
            let outcome = {
                let token = token.clone();
                tokio::task::spawn_blocking(move || {
                    intent_search::search_in_files(&root, &query, &opts, &token)
                })
                .await
            };
            registry.unregister(&request_id);
            let outcome =
                outcome.map_err(|e| Error::Internal(format!("search task failed: {e}")))??;
            Ok(serde_json::json!({
                "requestId": request_id,
                "matches": outcome.matches,
                "truncated": outcome.truncated,
            }))
        })
    }

    fn search_file_names(
        &self,
        workspace_id: WorkspaceId,
        pattern: String,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let registry = self.search_cancels.clone();
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(intent_search::mint_request_id);
            let limit = limit.and_then(|n| usize::try_from(n).ok());
            let root = match search_ops::search_root(&store, &workspace_id, None).await? {
                Some(root) => root,
                None => {
                    return Ok(serde_json::json!({
                        "requestId": request_id,
                        "files": [],
                        "truncated": false,
                    }))
                }
            };
            let token = registry.register(&request_id);
            let outcome = {
                let token = token.clone();
                tokio::task::spawn_blocking(move || {
                    intent_search::search_file_names(&root, &pattern, limit, &token)
                })
                .await
            };
            registry.unregister(&request_id);
            let outcome =
                outcome.map_err(|e| Error::Internal(format!("search task failed: {e}")))??;
            Ok(serde_json::json!({
                "requestId": request_id,
                "files": outcome.files,
                "truncated": outcome.truncated,
            }))
        })
    }

    fn search_cancel(&self, request_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let registry = self.search_cancels.clone();
        Box::pin(async move {
            // Idempotent: cancelling an unknown/finished id is a no-op success.
            registry.cancel(&request_id);
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    fn search_messages(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        agent_id: Option<String>,
        role: Option<String>,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let registry = self.search_cancels.clone();
        let services = self.clone();
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(intent_search::mint_request_id);
            let token = registry.register(&request_id);
            let limit = limit.and_then(|n| usize::try_from(n).ok());
            let sessions = store.list_agent_sessions(&workspace_id).await?;
            let matches = search_ops::message_matches(
                &sessions,
                &query,
                agent_id.as_deref(),
                role.as_deref(),
                limit,
            );
            let matches = to_value_vec(matches)?;
            Ok(services.deliver_search(request_id, Some(workspace_id), matches, token))
        })
    }

    fn search_events(
        &self,
        query: String,
        workspace_id: Option<WorkspaceId>,
        limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let registry = self.search_cancels.clone();
        let services = self.clone();
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(intent_search::mint_request_id);
            let token = registry.register(&request_id);
            let limit = limit.and_then(|n| usize::try_from(n).ok());
            let events = store
                .query_events(&EventQuery {
                    workspace_id: workspace_id.clone(),
                    ..Default::default()
                })
                .await?;
            let matches = search_ops::event_matches(&events, &query, limit);
            let matches = to_value_vec(matches)?;
            Ok(services.deliver_search(request_id, workspace_id, matches, token))
        })
    }

    fn search_memories(
        &self,
        query: String,
        workspace_id: Option<WorkspaceId>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let registry = self.search_cancels.clone();
        let services = self.clone();
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(intent_search::mint_request_id);
            let token = registry.register(&request_id);
            // `workspaceId` is optional: scope to it when present, else span all.
            let memories = memories::list(&store, workspace_id.as_ref()).await?;
            let matches = search_ops::memory_matches(&memories, &query);
            let matches = to_value_vec(matches)?;
            Ok(services.deliver_search(request_id, workspace_id, matches, token))
        })
    }

    fn search_notes(
        &self,
        query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let registry = self.search_cancels.clone();
        let services = self.clone();
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(intent_search::mint_request_id);
            let token = registry.register(&request_id);
            let notes = store.list_all_notes().await?;
            let matches = search_ops::note_matches(&notes, &query);
            let matches = to_value_vec(matches)?;
            // Global search (no workspaceId) → always inline (notes sets are small).
            Ok(services.deliver_search(request_id, None, matches, token))
        })
    }

    fn search_codebase(
        &self,
        workspace_id: WorkspaceId,
        query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let registry = self.search_cancels.clone();
        let engine = self.context_engine.clone();
        let services = self.clone();
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(intent_search::mint_request_id);
            let root = match search_ops::search_root(&store, &workspace_id, None).await? {
                Some(root) => root,
                None => return Ok(serde_json::json!({ "requestId": request_id, "matches": [] })),
            };
            let token = registry.register(&request_id);

            // Prefer the context engine when it is available, mapping its hits
            // to `CodebaseMatch` (§5.15 parity). When the engine is `Unavailable`
            // — or a retrieval errors — degrade to the ripgrep/symbol path rather
            // than failing the search (§8.3).
            if let intent_core::EngineAvailability::Available { .. } = engine.availability().await {
                let req = intent_core::RetrieveRequest {
                    workspace_id: workspace_id.clone(),
                    workspace_path: root.clone(),
                    query: query.clone(),
                    max_results: None,
                };
                match engine.retrieve(req).await {
                    Ok(result) => {
                        let matches = search_ops::engine_matches(&result);
                        let matches = to_value_vec(matches)?;
                        return Ok(services.deliver_search(
                            request_id,
                            Some(workspace_id),
                            matches,
                            token,
                        ));
                    }
                    Err(intent_core::ContextError::Unavailable { .. }) => {}
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "context engine retrieval failed; falling back to ripgrep"
                        );
                    }
                }
            }

            let opts = intent_search::SearchOpts::default();
            let outcome = {
                let token = token.clone();
                let query = query.clone();
                tokio::task::spawn_blocking(move || {
                    intent_search::search_in_files(&root, &query, &opts, &token)
                })
                .await
            };
            let outcome =
                match outcome.map_err(|e| Error::Internal(format!("search task failed: {e}")))? {
                    Ok(o) => o,
                    Err(e) => {
                        registry.unregister(&request_id);
                        return Err(e);
                    }
                };
            let matches = search_ops::codebase_matches(&outcome);
            let matches = to_value_vec(matches)?;
            Ok(services.deliver_search(request_id, Some(workspace_id), matches, token))
        })
    }

    fn terminal_create(
        &self,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
        env: Option<std::collections::BTreeMap<String, String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let pty = self.pty.clone();
        let bus = self.event_bus.clone();
        let store = self.store.clone();
        Box::pin(async move {
            terminal_ops::create(
                pty,
                bus,
                Some(store),
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

    fn terminal_write(
        &self,
        terminal_id: String,
        data: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let pty = self.pty.clone();
        Box::pin(async move { terminal_ops::write(&pty, &terminal_id, &data) })
    }

    fn terminal_resize(
        &self,
        terminal_id: String,
        cols: u16,
        rows: u16,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let pty = self.pty.clone();
        Box::pin(async move { terminal_ops::resize(&pty, &terminal_id, cols, rows) })
    }

    fn terminal_kill(&self, terminal_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let pty = self.pty.clone();
        Box::pin(async move { terminal_ops::kill(&pty, &terminal_id).await })
    }

    fn terminal_get_buffer(
        &self,
        terminal_id: String,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let pty = self.pty.clone();
        Box::pin(async move { terminal_ops::get_buffer(&pty, &terminal_id, max_bytes) })
    }

    fn terminal_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let pty = self.pty.clone();
        Box::pin(async move { terminal_ops::list(&pty, &workspace_id) })
    }

    fn terminal_read_output(
        &self,
        workspace_id: WorkspaceId,
        terminal_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            terminal_ops::read_output(
                &pty,
                &workspace_id,
                &terminal_id,
                max_lines,
                paginate.unwrap_or(false),
                page_token,
            )
        })
    }

    fn file_read(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root =
                file_ops::resolve_root(&store, &workspace_id, caller_agent_id.as_ref()).await;
            file_ops::read(&root, &path)
        })
    }

    fn file_write(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        content: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root =
                file_ops::resolve_root(&store, &workspace_id, caller_agent_id.as_ref()).await;
            file_ops::write(&root, &path, &content)
        })
    }

    fn file_list(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root =
                file_ops::resolve_root(&store, &workspace_id, caller_agent_id.as_ref()).await;
            file_ops::list(&root, &path)
        })
    }

    fn file_delete(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root =
                file_ops::resolve_root(&store, &workspace_id, caller_agent_id.as_ref()).await;
            file_ops::delete(&root, &path)
        })
    }

    fn file_mkdir(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root =
                file_ops::resolve_root(&store, &workspace_id, caller_agent_id.as_ref()).await;
            file_ops::mkdir(&root, &path)
        })
    }

    fn file_rename(
        &self,
        workspace_id: WorkspaceId,
        old_path: String,
        new_path: String,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root =
                file_ops::resolve_root(&store, &workspace_id, caller_agent_id.as_ref()).await;
            file_ops::rename(&root, &old_path, &new_path)
        })
    }

    fn file_tree(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root = file_ops::resolve_root(&store, &workspace_id, None).await;
            file_ops::tree(&root, &path)
        })
    }

    fn file_exists(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root = file_ops::resolve_root(&store, &workspace_id, None).await;
            file_ops::exists(&root, &path)
        })
    }

    fn file_stat(
        &self,
        workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let root = file_ops::resolve_root(&store, &workspace_id, None).await;
            file_ops::stat(&root, &path)
        })
    }

    fn primitive_add_reference(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        semantic_id: String,
        description: String,
        snapshot: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let id = new_uuid();
            let created_at = now_iso();
            let primitive = primitive_ops::reference(
                &id,
                &created_at,
                &semantic_id,
                &description,
                snapshot.as_deref(),
            );
            append_primitive(
                &store,
                &bus,
                &workspace_id,
                &note_id,
                &primitive,
                "reference",
                &id,
            )
            .await
        })
    }

    fn primitive_add_cli(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        command: String,
        description: String,
        working_directory: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let id = new_uuid();
            let created_at = now_iso();
            let primitive = primitive_ops::cli(
                &id,
                &created_at,
                &command,
                &description,
                working_directory.as_deref(),
            );
            append_primitive(
                &store,
                &bus,
                &workspace_id,
                &note_id,
                &primitive,
                "cli",
                &id,
            )
            .await
        })
    }

    fn primitive_add_patch(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        file_path: String,
        diff: String,
        description: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let id = new_uuid();
            let created_at = now_iso();
            let primitive = primitive_ops::patch(&id, &created_at, &file_path, &diff, &description);
            append_primitive(
                &store,
                &bus,
                &workspace_id,
                &note_id,
                &primitive,
                "patch",
                &id,
            )
            .await
        })
    }

    fn primitive_add_agent_action(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
        goal: String,
        description: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let id = new_uuid();
            let created_at = now_iso();
            let primitive =
                primitive_ops::agent_action(&id, &created_at, &agent_id, &goal, &description);
            append_primitive(
                &store,
                &bus,
                &workspace_id,
                &note_id,
                &primitive,
                "agent_action",
                &id,
            )
            .await
        })
    }

    fn cross_workspace_list_siblings(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let current = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => {
                    return Err(Error::Internal("Current workspace not found".to_string()));
                }
                Err(e) => return Err(e),
            };
            let repo_path = match current.repository_path.as_deref() {
                Some(p) if !p.is_empty() => p.to_string(),
                _ => {
                    return Err(Error::Internal(
                        "Current workspace is not associated with a repository".to_string(),
                    ));
                }
            };
            let all = store.list_workspaces(true).await?;
            let siblings: Vec<serde_json::Value> = all
                .into_iter()
                .filter(|w| {
                    w.id != workspace_id && w.repository_path.as_deref() == Some(repo_path.as_str())
                })
                .map(|w| {
                    serde_json::json!({
                        "id": w.id,
                        "title": if w.title.is_empty() { "Untitled".to_string() } else { w.title },
                        "branch": w.branch,
                        "status": w.status,
                        "createdAt": w.created_at,
                        "updatedAt": w.updated_at,
                    })
                })
                .collect();
            Ok(serde_json::Value::Array(siblings))
        })
    }

    fn cross_workspace_list_notes(
        &self,
        workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            sibling_workspace_or_throw(&store, &workspace_id, &target_workspace_id).await?;
            let notes = store.list_notes(&target_workspace_id).await?;
            let out: Vec<serde_json::Value> = notes
                .into_iter()
                .map(|n| {
                    serde_json::json!({
                        "id": n.id,
                        "title": n.title,
                        "createdAt": n.created_at,
                        "updatedAt": n.updated_at,
                    })
                })
                .collect();
            Ok(serde_json::Value::Array(out))
        })
    }

    fn cross_workspace_read_note(
        &self,
        workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let target =
                sibling_workspace_or_throw(&store, &workspace_id, &target_workspace_id).await?;
            let note = match store.get_note(&target_workspace_id, &note_id).await {
                Ok(n) => n,
                Err(Error::NotFound(_)) => {
                    return Err(Error::Internal(format!(
                        "Note not found: {note_id} in workspace {target_workspace_id}"
                    )));
                }
                Err(e) => return Err(e),
            };
            let content = note.content;
            let line_count = content.split('\n').count();
            Ok(serde_json::json!({
                "id": note.id,
                "title": note.title,
                "content": content,
                "numberedContent": number_lines(&content),
                "sourceWorkspaceId": target_workspace_id,
                "sourceWorkspaceTitle": target.title,
                "branch": target.branch,
                "lineCount": line_count,
            }))
        })
    }

    fn script_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move { mgr.list(&workspace_id).await })
    }

    fn script_create(
        &self,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move { mgr.create(workspace_id, params).await })
    }

    fn script_remove(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move { mgr.remove(&workspace_id, &script_id).await })
    }

    fn script_start(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move { mgr.start(&workspace_id, &script_id).await })
    }

    fn script_stop(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move { mgr.stop(&workspace_id, &script_id).await })
    }

    fn script_restart(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move { mgr.restart(&workspace_id, &script_id).await })
    }

    fn script_output(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        paginate: Option<bool>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move {
            mgr.output(
                &workspace_id,
                &script_id,
                max_lines,
                paginate.unwrap_or(false),
                page_token,
            )
        })
    }

    fn script_status(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move { mgr.status(&workspace_id, &script_id) })
    }

    fn script_run(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let mgr = self.script_manager();
        Box::pin(async move {
            mgr.run(&workspace_id, &script_id, max_lines, timeout_seconds)
                .await
        })
    }

    fn list_workspaces(&self, include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        let store = self.store.clone();
        let this = self.clone();
        Box::pin(async move {
            let mut list = store.list_workspaces(include_archived).await?;
            // `activity` is derived from live agent state, never persisted (§9.9);
            // the card aggregates are computed fresh on the emit path (§9.1).
            for ws in &mut list {
                ws.activity = this.workspace_activity(&ws.id);
                this.enrich_workspace_aggregates(ws).await;
            }
            Ok(list)
        })
    }

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let this = self.clone();
        Box::pin(async move {
            // Chief is synthesized on read (TS `workspace.service.getWorkspace`
            // parity): return the canonical `chief_workspace()` shape rather
            // than whatever the seeded row holds. `activity` still tracks live
            // Chief-of-Staff agents; card aggregates stay omitted (Chief has
            // no notes/tasks/PRs/diff to roll up).
            if id.is_chief() {
                let mut ws = chief_workspace();
                ws.activity = this.workspace_activity(&id);
                return Ok(ws);
            }
            let mut ws = store.get_workspace(&id).await?;
            // `activity` is derived from live agent state, never persisted (§9.9);
            // the card aggregates are computed fresh on the emit path (§9.1).
            ws.activity = this.workspace_activity(&id);
            this.enrich_workspace_aggregates(&mut ws).await;
            Ok(ws)
        })
    }

    fn create_workspace(
        &self,
        input: WorkspaceCreate,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<WorkspaceCreateResult>> {
        let store = self.store.clone();
        let worktree_locks = self.worktree_locks.clone();
        let workspaces_root = self.workspaces_root.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            // workspace.create carries no workspaceId → "" sentinel scope (§5.1).
            let op_store = store.clone();
            with_idempotency(
                &store,
                "",
                idempotency_key,
                "workspace.create",
                move || async move {
                    let store = op_store;
                    let now = now_iso();
                    let mut input = input;
                    let workspaces_root =
                        workspaces_root.unwrap_or_else(default_workspaces_root);
                    // Workspace id derivation (TS `generateLocalSlug` parity):
                    // slug from the initial-agent prompt when possible, else a
                    // random adjective-animal pair; uniquified with a `-N`
                    // suffix whenever the id was ever used (live row, delete
                    // tombstone, or leftover directory) so the on-disk
                    // directory reflects intent and deleted ids are never
                    // recycled.
                    let id = derive_workspace_id(&store, &input, &workspaces_root).await;
                    // Clone orchestration (PROTOCOL §5.1): when `githubUrl` is
                    // set and `repositoryPath` is not already a local git
                    // repo, clone it *before* branch naming so the branch
                    // uniquification and worktree provisioning downstream see
                    // a real checkout. Progress is streamed under the newly
                    // minted `workspaceId`; a failure fails the whole create
                    // pre-insert (no row persisted, no partial state).
                    let existing_repo = input
                        .repository_path
                        .as_deref()
                        .filter(|p| !p.is_empty())
                        .map(PathBuf::from)
                        .filter(|p| p.join(".git").exists());
                    if existing_repo.is_none() {
                        if let Some(url) = input
                            .github_url
                            .as_deref()
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                        {
                            let bus_ref = bus.clone().ok_or_else(|| {
                                Error::Internal(
                                    "event bus not wired; workspace.create clone requires streaming"
                                        .to_string(),
                                )
                            })?;
                            let target = match input.clone_path.as_deref() {
                                Some(p) if !p.trim().is_empty() => PathBuf::from(p),
                                _ => workspaces_root
                                    .join("clones")
                                    .join(clone_ops::derive_default_target(url)),
                            };
                            if target.as_os_str().is_empty()
                                || target
                                    .file_name()
                                    .map(|n| n.is_empty())
                                    .unwrap_or(true)
                            {
                                return Err(Error::InvalidParams(
                                    "clonePath must resolve to a non-empty target".to_string(),
                                ));
                            }
                            if clone_ops::target_exists(&target) {
                                return Err(Error::Internal(format!(
                                    "clone target already exists: {}",
                                    target.display()
                                )));
                            }
                            let request_id = uuid::Uuid::new_v4().to_string();
                            clone_ops::perform_clone(clone_ops::CloneJob {
                                request_id,
                                workspace_id: Some(id.clone()),
                                url: url.to_string(),
                                target_path: target.clone(),
                                bus: bus_ref,
                            })
                            .await
                            .map_err(|e| {
                                Error::Internal(format!("workspace.create clone failed: {e}"))
                            })?;
                            input.repository_path =
                                Some(target.to_string_lossy().to_string());
                            if let Some((owner, name)) = clone_ops::parse_owner_repo(url) {
                                if input.repository_owner.is_none() {
                                    input.repository_owner = Some(owner);
                                }
                                if input.repository_name.is_none() {
                                    input.repository_name = Some(name);
                                }
                            }
                        }
                    }
                    // Repository-name derivation (`known_repo_name` fallback
                    // parity): a caller-supplied `repositoryName` always wins;
                    // otherwise a local `repositoryPath` yields its basename so
                    // workspace payloads carry `repositoryName` for locally
                    // created workspaces (FE recent-repos surfaces). `owner` is
                    // left untouched — no local-remote inspection.
                    if !input
                        .repository_name
                        .as_deref()
                        .is_some_and(|n| !n.is_empty())
                    {
                        if let Some(name) = input
                            .repository_path
                            .as_deref()
                            .filter(|p| !p.is_empty())
                            .and_then(derive_repo_name_from_path)
                        {
                            input.repository_name = Some(name);
                        }
                    }
                    // Branch naming (TS parity): an explicit `branch` wins
                    // untouched; otherwise the branch is a friendly slug —
                    // extracted from `initialAgent.prompt` when possible
                    // (`generateLocalSlug`), else a random adjective-animal
                    // pair (`generateWorkspaceSlug`) — under the optional
                    // `workspace.branchPrefix` setting, uniquified against
                    // existing local/remote branches with a `-N` suffix.
                    let branch_auto_generated =
                        !input.branch.as_deref().is_some_and(|b| !b.is_empty());
                    let branch = match input.branch.clone().filter(|b| !b.is_empty()) {
                        Some(explicit) => explicit,
                        None => {
                            let slug = input
                                .initial_agent
                                .as_ref()
                                .and_then(|a| a.prompt.as_deref())
                                .and_then(intent_core::slug::extract_local_slug)
                                .unwrap_or_else(intent_core::slug::generate_workspace_slug);
                            // Branch prefix fallback: repo config > global setting
                            // (FE parity: workspace.service.ts L1215-1219).
                            let prefix = if let Some(repo_path) = input
                                .repository_path
                                .as_deref()
                                .filter(|p| !p.is_empty())
                                .map(PathBuf::from)
                            {
                                let repo_config = crate::repo_config::read_repo_config(&repo_path).await;
                                match repo_config.branch_prefix.filter(|p| !p.is_empty()) {
                                    Some(p) => p,
                                    None => settings::branch_prefix(&store).await,
                                }
                            } else {
                                settings::branch_prefix(&store).await
                            };
                            let desired = format!("{prefix}{slug}");
                            let git_repo = input
                                .repository_path
                                .as_deref()
                                .filter(|p| !p.is_empty())
                                .map(PathBuf::from)
                                .filter(|p| p.join(".git").exists());
                            match git_repo {
                                Some(repo) => {
                                    let want = desired;
                                    tokio::task::spawn_blocking(move || {
                                        intent_git::branches::ensure_unique_branch_name(
                                            &repo, &want,
                                        )
                                    })
                                    .await
                                    .map_err(|e| {
                                        Error::Internal(format!(
                                            "branch uniquification task failed: {e}"
                                        ))
                                    })??
                                }
                                None => desired,
                            }
                        }
                    };
                    // Normalise the caller-supplied title: trim surrounding
                    // whitespace so whitespace-only titles collapse to `""`,
                    // and default to `""` when the caller omits the field
                    // entirely. Matches the reference `workspace.service`
                    // shape (`title: request.title || ''`), with the added
                    // trim to keep persistence consistent with the rest of
                    // the daemon's title-emptiness guards. The FE renders
                    // `""` as "Untitled" until the initial agent's first-turn
                    // naming instruction calls `workspace.setTitle`. The
                    // `hasTitle` binding keeps its `title != id` guard as
                    // belt-and-braces for legacy rows the data heal has not
                    // yet touched.
                    let title = input
                        .title
                        .map(|t| t.trim().to_string())
                        .unwrap_or_default();
                    // Setup script fallback: request > repo config > none
                    // (FE parity: workspace.service.ts L1788-1791).
                    let setup_script = match input.setup_script.clone().filter(|s| !s.is_empty()) {
                        Some(explicit) => Some(setup_scripts::user_script(explicit)),
                        None => {
                            if let Some(repo_path) = input
                                .repository_path
                                .as_deref()
                                .filter(|p| !p.is_empty())
                                .map(PathBuf::from)
                            {
                                let repo_config = crate::repo_config::read_repo_config(&repo_path).await;
                                repo_config.setup_script
                                    .filter(|s| !s.is_empty())
                                    .map(setup_scripts::user_script)
                            } else {
                                None
                            }
                        }
                    };
                    let mut ws = Workspace {
                        id,
                        title,
                        branch,
                        // Canonicalise the wire `baseRef` on write so the
                        // persisted value is the plain branch name — a PR's
                        // `sourceBranch` (never remote-qualified) compares
                        // raw-equal without FE-side stripping (§7.6).
                        base_ref: input.base_ref.map(|r| canonicalise_base_ref(&r)),
                        base_commit_sha: input.base_commit_sha,
                        status: WorkspaceStatus::Active,
                        status_message: input.status_message,
                        // Derived, read-only; never persisted (§9.9).
                        activity: WorkspaceActivity::Idle,
                        attention: WorkspaceAttention::None,
                        created_at: now.clone(),
                        updated_at: now.clone(),
                        // Stamp `lastActivity` at create so it is always
                        // populated — the emit path (§9.1) still enriches
                        // with the max of notes / agents / updatedAt.
                        last_activity: Some(now),
                        tags: input.tags.unwrap_or_default(),
                        path: input.path,
                        repository_path: input.repository_path,
                        repository_owner: input.repository_owner,
                        repository_name: input.repository_name,
                        worktree_path: input.worktree_path,
                        scope: input.scope,
                        skip_worktree: input.skip_worktree.unwrap_or(false),
                        setup_script,
                        is_remote: input.is_remote.unwrap_or(false),
                        default_model: input.default_model,
                        pr_number: None,
                        pr_url: None,
                        pr_status: None,
                        active_pull_request: None,
                        pull_requests: None,
                        archived: false,
                        archived_at: None,
                        // Card aggregates are computed on the list/get emit path only.
                        task_stats: None,
                        agent_summary: None,
                        diff_summary: None,
                        token_usage: None,
                        cow_supported: None,
                    };
                    // Provision the git worktree (TS `createGitWorktree` parity):
                    // a local workspace created off a local git repo gets a
                    // linked worktree at `<root>/<workspaceId>/<repo-slug>` on
                    // its branch, so agents spawn inside a real checkout instead
                    // of falling back to a temp dir. Skipped for remote/
                    // `skipWorktree` workspaces, callers supplying their own
                    // `worktreePath`, and repository paths that are not a local
                    // git repo (registry-only rows keep the prior behavior).
                    let has_worktree = ws
                        .worktree_path
                        .as_deref()
                        .is_some_and(|p| !p.is_empty());
                    let repo_dir = ws
                        .repository_path
                        .as_deref()
                        .filter(|p| !p.is_empty())
                        .map(PathBuf::from);
                    let workspaces_root_pathbuf = workspaces_root.clone();
                    if let Some(repo_dir) = repo_dir {
                        if !ws.is_remote && !ws.skip_worktree && !has_worktree {
                            if repo_dir.join(".git").exists() {
                                let repo_name = known_repo_name(
                                    ws.repository_name.as_deref(),
                                    &repo_dir.to_string_lossy(),
                                );
                                let wt_path = workspaces_root_pathbuf
                                    .join(&ws.id.0)
                                    .join(worktree_folder_slug(&repo_name));
                                let name = ws.id.0.clone();
                                let branch = ws.branch.clone();
                                let base_ref = ws.base_ref.clone();
                                let remote =
                                    input.remote.unwrap_or_else(|| "origin".to_string());
                                let repo = repo_dir.clone();
                                let wt = wt_path.clone();
                                let sha = worktree_locks
                                    .with_lock(&repo_dir, move || async move {
                                        tokio::task::spawn_blocking(move || {
                                            intent_git::worktree::provision_worktree(
                                                &repo,
                                                &name,
                                                &wt,
                                                &branch,
                                                base_ref.as_deref(),
                                                &remote,
                                            )
                                        })
                                        .await
                                        .map_err(|e| {
                                            Error::Internal(format!(
                                                "worktree provisioning task failed: {e}"
                                            ))
                                        })?
                                    })
                                    .await?;
                                ws.worktree_path =
                                    Some(wt_path.to_string_lossy().to_string());
                                if ws.base_commit_sha.is_none() {
                                    ws.base_commit_sha = Some(sha);
                                }
                            } else {
                                tracing::warn!(
                                    repository_path = %repo_dir.display(),
                                    "workspace.create: repositoryPath is not a local git repo; skipping worktree provisioning"
                                );
                            }
                        }
                    }
                    store.insert_workspace(&ws).await?;
                    // Write the legacy `<root>/<id>/.workspace/workspace.json`
                    // file so renderer paths (FE `FileSystemWorkspaceRepository`)
                    // find their per-workspace metadata without ENOENT spam.
                    // Best-effort — a failure warns and continues.
                    if let Err(e) =
                        write_workspace_metadata_file(&workspaces_root_pathbuf, &ws)
                    {
                        tracing::warn!(
                            workspace = %ws.id.as_str(),
                            error = %e,
                            "workspace.create: failed to write .workspace/workspace.json"
                        );
                    }
                    // Record branch provenance for the delete-cleanup guard:
                    // only an auto-generated branch is ever deleted alongside
                    // the worktree (TS `removeGitWorktree` parity, where the
                    // guard is `branchName === workspaceId`). Best-effort — a
                    // failure leaves the conservative default (never delete).
                    if branch_auto_generated {
                        if let Err(e) = store
                            .set_workspace_branch_auto_generated(&ws.id, true)
                            .await
                        {
                            tracing::warn!(error = %e, "failed to record branch provenance");
                        }
                    }
                    // Register the repo in the persistent registry so it survives
                    // workspace deletion and appears in `repo.list` without a restart
                    // (TS `workspace.service` `addRepo` hook). Best-effort: a registry
                    // failure must not fail workspace creation.
                    let repo_path = ws
                        .repository_path
                        .as_deref()
                        .filter(|p| !p.is_empty())
                        .or(ws.path.as_deref())
                        .filter(|p| !p.is_empty());
                    if let Some(repo_path) = repo_path {
                        let name = known_repo_name(ws.repository_name.as_deref(), repo_path);
                        if let Err(e) = store
                            .upsert_known_repo(repo_path, &name, ws.repository_owner.as_deref())
                            .await
                        {
                            tracing::warn!(error = %e, "failed to register repo in registry");
                        }
                    }
                    // Inside the idempotency scope: a replayed create returns
                    // the stored result without re-running the op, so the
                    // event fires at most once per logical create (§6.5).
                    publish_event(&bus, workspace_created_event(&ws)).await;
                    // Seed the well-known `spec` note (reference parity with
                    // `notes.service.ts ensureSpecExists`). Idempotent under
                    // the replay guard: on retry the stored result comes back
                    // and this branch never re-runs.
                    ensure_spec_note(&store, &bus, &ws.id).await?;
                    // Daemon-owned initial-agent orchestration (§5.1): when the
                    // request carries an `initialAgent`, always create the agent
                    // row (reference parity: `workspace.service.ts` persists the
                    // session whenever `initialAgent` is present — the turn only
                    // starts when a prompt exists). The agent is parentless,
                    // non-background (delegate parity: `agent_create_op`). When
                    // the prompt is non-empty it is stored as
                    // `metadata.initialMessage` and delivered exactly once — all
                    // inside the idempotency scope, so a client retry replays
                    // the stored result instead of re-sending. An empty/missing
                    // prompt persists the row without a message; the FE first
                    // send starts the turn.
                    let mut initial_agent = None;
                    if let Some(agent) = input.initial_agent {
                        let prompt = agent
                            .prompt
                            .as_deref()
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                        // Persist the prompt (when present) as
                        // `metadata.initialMessage` (delegate parity: a wake-up
                        // can resume from it) and stamp the reference-parity
                        // `isInitialAgent`/`isFirstWorkspaceAgent` flags the
                        // FE surface (`agent-backend-handler.service.ts`,
                        // `instruction-service.ts` prompt-cache `':initial'`
                        // suffix, `agent-persistence.ts`) uses to classify the
                        // workspace's coordinator. Both flags are persisted on
                        // the raw `AgentSession.metadata` JSON — the strict
                        // `AgentLite.metadata` projection does not surface them
                        // yet (future work if the daemon-only wire path grows
                        // a consumer). The caller's metadata object is
                        // forwarded as-is; like agent.create, only the
                        // harvested gap fields persist today (P2-12a) —
                        // `behaviorPrompt` has no session column and the
                        // behavior derives from the persisted `specialist`.
                        let mut metadata = match agent.metadata {
                            Some(serde_json::Value::Object(m)) => m,
                            _ => serde_json::Map::new(),
                        };
                        metadata.insert("isInitialAgent".to_string(), serde_json::json!(true));
                        metadata.insert(
                            "isFirstWorkspaceAgent".to_string(),
                            serde_json::json!(true),
                        );
                        // Own the `metadata.initialMessage` invariant: when
                        // the daemon has a non-empty prompt, stamp it (delegate
                        // parity — a wake-up can resume from it); otherwise
                        // drop any caller-supplied `initialMessage` so
                        // `agent_create_op`'s metadata harvest cannot persist
                        // a stale prompt for a no-prompt workspace.
                        if let Some(ref p) = prompt {
                            metadata.insert(
                                "initialMessage".to_string(),
                                serde_json::json!(p.clone()),
                            );
                        } else {
                            metadata.remove("initialMessage");
                        }
                        let extra = intent_core::AgentCreateExtra {
                            provider: nonempty_owned(agent.provider),
                            agent_type: nonempty_owned(agent.agent_type),
                            metadata: Some(serde_json::Value::Object(metadata)),
                            context_references: agent
                                .context_references
                                .filter(|v| !v.is_null()),
                            image_blocks: agent.image_blocks.filter(|v| !v.is_null()),
                            // The initial agent is the workspace's
                            // foreground agent (top-level wins over any
                            // metadata.isBackground copy).
                            is_background: Some(false),
                            ..Default::default()
                        };
                        let requested = nonempty_owned(agent.agent_id)
                            .map(|id| AgentId::from(id.as_str()));
                        let created = services
                            .agent_create_op(
                                ws.id.clone(),
                                nonempty_owned(agent.name),
                                nonempty_owned(agent.model),
                                nonempty_owned(agent.specialist),
                                None,
                                None,
                                false,
                                requested,
                                extra,
                            )
                            .await?;
                        let child = AgentId::from(
                            created["agent"]["id"].as_str().unwrap_or_default(),
                        );
                        // Deliver the prompt and start the first turn only
                        // when one was supplied (delegate parity): the runtime
                        // `AgentManager` when attached, else the store-only
                        // persist. Best-effort like the delegate path — the
                        // agent holds `metadata.initialMessage` for resume.
                        if let Some(prompt) = prompt {
                            let send = match services.agent_manager() {
                                Some(manager) => {
                                    manager
                                        .send_message(child, ws.id.clone(), prompt, None, crate::agent_manager::TurnOptions::default())
                                        .await
                                }
                                None => {
                                    services.agent_send_message_op(child, prompt, None, None, None).await
                                }
                            };
                            if let Err(e) = send {
                                tracing::warn!(
                                    workspace = %ws.id.as_str(),
                                    error = %e,
                                    "workspace.create: failed to start initial agent turn"
                                );
                            }
                        }
                        initial_agent = created.get("agent").cloned();
                    }
                    Ok(WorkspaceCreateResult {
                        workspace: ws,
                        initial_agent,
                    })
                },
            )
            .await
        })
    }

    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let this = self.clone();
        // Normalise write-time-canonicalised fields on the delta snapshot so
        // `workspace:updated { changes }` (§6.5) matches the returned/persisted
        // `Workspace`: raw `baseRef: "origin/main"` collapses to canonical
        // `"main"`, and whitespace-only `statusMessage` folds to `""` (the
        // clear-signal wire value, preserved through `skip_serializing_if` so
        // subscribers can distinguish "clear" from "no change"). Lets
        // subscribers mirror the delta without a follow-up read.
        let mut normalised = update.clone();
        if let Some(raw) = normalised.base_ref.as_deref() {
            normalised.base_ref = Some(canonicalise_base_ref(raw));
        }
        if let Some(raw) = normalised.status_message.as_deref() {
            if raw.trim().is_empty() {
                normalised.status_message = Some(String::new());
            }
        }
        let changes = serde_json::to_value(&normalised).unwrap_or(serde_json::Value::Null);
        Box::pin(async move {
            let mut ws = if id.is_chief() {
                chief_workspace()
            } else {
                store.get_workspace(&id).await?
            };
            if let Some(v) = update.title {
                ws.title = v;
            }
            if let Some(v) = update.status_message {
                // Reference `setStatusMessage` contract (§5 / ws-misc-api.ts): empty
                // or whitespace-only strings clear the message. Normalize on write so
                // `ws.workspace.details()` and every downstream reader see `None`
                // instead of `Some("")` for a cleared value.
                ws.status_message = if v.trim().is_empty() { None } else { Some(v) };
            }
            if let Some(v) = update.branch {
                ws.branch = v;
            }
            if let Some(v) = update.base_ref {
                // Canonicalise the wire `baseRef` on write so the persisted
                // value stays the plain branch name (§7.6, matches create).
                ws.base_ref = Some(canonicalise_base_ref(&v));
            }
            if let Some(v) = update.base_commit_sha {
                ws.base_commit_sha = Some(v);
            }
            if let Some(v) = update.status {
                ws.status = v;
            }
            if let Some(v) = update.tags {
                ws.tags = v;
            }
            if let Some(v) = update.path {
                ws.path = Some(v);
            }
            if let Some(v) = update.repository_path {
                ws.repository_path = Some(v);
            }
            if let Some(v) = update.repository_owner {
                ws.repository_owner = Some(v);
            }
            if let Some(v) = update.repository_name {
                ws.repository_name = Some(v);
            }
            if let Some(v) = update.worktree_path {
                ws.worktree_path = Some(v);
            }
            if let Some(v) = update.scope {
                ws.scope = Some(v);
            }
            if let Some(v) = update.skip_worktree {
                ws.skip_worktree = v;
            }
            if let Some(v) = update.setup_script {
                ws.setup_script = Some(setup_scripts::user_script(v));
            }
            if let Some(v) = update.is_remote {
                ws.is_remote = v;
            }
            if let Some(v) = update.default_model {
                ws.default_model = Some(v);
            }
            // Clearable PR fields (§5.1): `Some(None)` deserialized from a
            // wire `null` clears the stored value; `Some(Some(v))` sets it;
            // bare `None` leaves it untouched (missing on the wire).
            if let Some(v) = update.pr_number {
                ws.pr_number = v;
            }
            if let Some(v) = update.pr_url {
                ws.pr_url = v;
            }
            if let Some(v) = update.pr_status {
                ws.pr_status = v;
            }
            if let Some(v) = update.active_pull_request {
                ws.active_pull_request = v;
            }
            if let Some(v) = update.pull_requests {
                ws.pull_requests = v;
            }
            if let Some(v) = update.last_activity {
                ws.last_activity = Some(v);
            }
            if let Some(v) = update.attention {
                ws.attention = v;
            }
            if let Some(v) = update.archived {
                ws.archived = v;
            }
            ws.updated_at = now_iso();
            // Chief is virtual: the seeded row is never mutated (TS
            // `saveWorkspaceUpdates` parity — return the applied delta layered
            // over the synthesized shape without persisting). Still emit
            // `workspace:updated` so subscribers see the delta.
            if ws.id.is_chief() {
                // Chief's canonical shape pins createdAt/updatedAt/lastActivity
                // (`CHIEF_WORKSPACE_TIMESTAMP` in `chief_workspace()`). Restore
                // the invariants so the update response matches the shape
                // `workspace.get` returns — a caller-supplied `lastActivity` or
                // the auto-touched `updatedAt` above must not leak through and
                // diverge Chief's timestamps from `chief_workspace()`.
                let pinned = chief_workspace();
                ws.created_at = pinned.created_at;
                ws.updated_at = pinned.updated_at;
                ws.last_activity = pinned.last_activity;
            } else {
                store.update_workspace(&ws).await?;
                // Derive `lastActivity` (§9.1) on the returned record so
                // `workspace.update` callers get the authoritative wire shape
                // without a follow-up `workspace.get`. Chief is skipped: its
                // timestamps are pinned above.
                this.derive_last_activity(&mut ws).await;
            }
            // Self-sufficient `workspace:updated` payload (§6.5) so every
            // client mirrors the delta without a follow-up read.
            publish_event(&bus, workspace_updated_event(&ws.id, changes)).await;
            Ok(ws)
        })
    }

    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        let worktree_locks = self.worktree_locks.clone();
        let bus = self.event_bus.clone();
        let workspaces_root = self
            .workspaces_root
            .clone()
            .unwrap_or_else(default_workspaces_root);
        // Live-state teardown handles: cloned so the boxed future owns them
        // across the store cascade below. `agent_manager()` upgrades the weak
        // reference; read-only/test wiring with no manager attached simply
        // skips the runtime stop and still sweeps the in-memory registries.
        let manager = self.agent_manager();
        let agent_queues = self.agent_queues.clone();
        let live_turns = self.live_turns.clone();
        let agent_subscriptions = self.agent_subscriptions.clone();
        Box::pin(async move {
            // Chief is virtual and never appears in `workspace.list`; delete is
            // a no-op success (TS `workspace.repository.delete` / virtual-guard
            // parity — the seeded row is not torn down and no cascade fires).
            if id.is_chief() {
                return Ok(());
            }
            // Terminate live agent sessions BEFORE the store cascade drops
            // their rows. A same-slug recreate hitting `agent.list` after the
            // delete would otherwise surface ghost sessions whose workers are
            // still draining, their live-turn slots + pending message queues
            // still populated, and their completion watches still firing. The
            // order mirrors `agent_delete_op`: capture the workspace-scoped
            // sessions, drop each session's runtime state, then emit
            // `agent:deleted` per session so subscribers see the tear-down
            // before the terminal `workspace:deleted`.
            // Fail fast on a transient `list_agent_sessions` error: skipping
            // the sweep here but still deleting the workspace row leaves ghost
            // workers, live-turn slots, queued messages, and completion
            // watches with no owning workspace — a client can retry the
            // delete, but silent partial success cannot recover.
            let sessions = store.list_agent_sessions(&id).await?;
            for session in &sessions {
                if let Some(manager) = manager.as_ref() {
                    // Aborts the worker, drops the handle (kill_child_tree on
                    // the ACP child), clears busy + agent_ws, and
                    // deregisters the process — the `agent.stop` semantics.
                    manager.stop(&session.id).await;
                }
                // Live-turn slot + pending message queue live in `Services`'
                // maps (not touched by `manager.stop`); drop them so a
                // same-slug recreate observes no ghost state. Recover through
                // a poisoned lock via `into_inner` — the delete path is the
                // last chance to unlink this state, so best-effort teardown
                // outweighs propagating a mutex-poison panic.
                live_turns
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&session.id);
                agent_queues
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&session.id);
            }
            // Drop the workspace's completion-watch + delegation-group entry
            // wholesale: every parent/child in the map is workspace-scoped, so
            // the entry cannot outlive the workspace it keys off. Poison
            // recovery mirrors the per-session sweep above.
            agent_subscriptions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            // Emit `agent:deleted` per swept session before the worktree /
            // store cleanup so subscribers see the terminal event for each
            // agent ahead of `workspace:deleted`. Best-effort: emits when the
            // bus is wired; a `None` bus (read-only wiring) is a quiet no-op.
            for session in &sessions {
                publish_event(
                    &bus,
                    NewEvent {
                        workspace_id: id.clone(),
                        timestamp: now_iso(),
                        event_type: AGENT_DELETED.to_string(),
                        actor: system_actor(),
                        session_id: Some(session.id.0.clone()),
                        correlation_id: None,
                        parent_event_id: None,
                        metadata: None,
                        data: serde_json::json!({ "agentId": session.id.0 }),
                    },
                )
                .await;
            }
            // Port the TS `deleteWorkspace` worktree cleanup: under the per-repo
            // worktree lock, remove the linked worktree (+ the now-empty
            // `<root>/<workspaceId>` parent) and delete the workspace branch —
            // but only when it is the branch the daemon auto-generated at
            // create time (guard parity with `removeGitWorktree`'s
            // `branchName === workspaceId`). Cleanup is best-effort throughout:
            // a git failure never blocks the row deletion (TS parity).
            if let Ok(ws) = store.get_workspace(&id).await {
                let repo_dir = ws
                    .repository_path
                    .as_deref()
                    .filter(|p| !p.is_empty())
                    .map(PathBuf::from);
                let wt = ws
                    .worktree_path
                    .as_deref()
                    .filter(|p| !p.is_empty())
                    .map(PathBuf::from);
                if let (Some(repo_dir), Some(wt)) = (repo_dir, wt) {
                    if !ws.skip_worktree && !ws.is_remote {
                        let branch_auto_generated = store
                            .workspace_branch_auto_generated(&id)
                            .await
                            .unwrap_or(false);
                        let branch = ws.branch.clone();
                        let repo = repo_dir.clone();
                        worktree_locks
                            .with_lock(&repo_dir, move || async move {
                                let task = tokio::task::spawn_blocking(move || {
                                    cleanup_workspace_worktree(
                                        &repo,
                                        &wt,
                                        &branch,
                                        branch_auto_generated,
                                    );
                                })
                                .await;
                                if let Err(e) = task {
                                    tracing::warn!(error = %e, "worktree cleanup task failed");
                                }
                            })
                            .await;
                    }
                }
            }
            // Final sweep of the daemon-owned workspace directory
            // (`<workspaces_root>/<id>/`, matching FE
            // `WorkspaceConfig.WORKSPACES_BASE`): the worktree cleanup's
            // best-effort `remove_dir` (empty-only) leaves any residual
            // content behind, and legacy pre-daemon workspaces have a
            // directory but no worktree path at all. Recursive best-effort
            // removal keeps the FE `FileSystemWorkspaceRepository.findAll`
            // scan from re-surfacing the deleted id (ENOENT WARN spam) and
            // makes the delete idempotent for orphaned directories.
            let dir = workspaces_root.join(id.as_str());
            let cleanup =
                tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&dir) {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(e),
                })
                .await;
            match cleanup {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(
                    workspace = %id.as_str(),
                    error = %e,
                    "workspace.delete: failed to remove workspace directory"
                ),
                Err(join_err) => tracing::warn!(
                    workspace = %id.as_str(),
                    error = %join_err,
                    "workspace.delete: workspace-dir cleanup task failed"
                ),
            }
            // Idempotent DB delete: swallow `NotFound` so retries and
            // orphan-dir-only cleanups don't surface as `"Failed to delete
            // space"` on the FE. Any other error still propagates. Publish
            // `workspace:deleted` only when we actually removed a row —
            // subscribers dedup by id but a spurious event confuses UIs
            // that treat it as a state transition.
            match store.delete_workspace(&id).await {
                Ok(()) => {
                    publish_event(&bus, workspace_deleted_event(&id)).await;
                }
                Err(Error::NotFound(_)) => {
                    tracing::debug!(
                        workspace = %id.as_str(),
                        "workspace.delete: row already gone; treating as idempotent success"
                    );
                }
                Err(e) => return Err(e),
            }
            Ok(())
        })
    }

    fn archive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let this = self.clone();
        Box::pin(async move {
            // Chief cannot be archived: it is a fixed virtual workspace, so
            // return the synthesized shape unchanged rather than mutating the
            // seeded row (TS virtual-workspace guard parity).
            if id.is_chief() {
                return Ok(chief_workspace());
            }
            let mut ws = store.get_workspace(&id).await?;
            let now = now_iso();
            ws.status = WorkspaceStatus::Archived;
            ws.archived = true;
            ws.archived_at = Some(now.clone());
            ws.updated_at = now;
            store.update_workspace(&ws).await?;
            // Derive `lastActivity` (§9.1) so archive callers get the
            // authoritative wire shape without a follow-up `workspace.get`.
            this.derive_last_activity(&mut ws).await;
            // §6.5 has no `workspace:archived`; mirror the reference emitter and
            // publish `workspace:updated` with the applied `{ archived }` delta
            // so subscribers flip state without a re-read.
            publish_event(
                &bus,
                workspace_updated_event(&ws.id, serde_json::json!({ "archived": true })),
            )
            .await;
            Ok(ws)
        })
    }

    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let this = self.clone();
        Box::pin(async move {
            if id.is_chief() {
                return Ok(chief_workspace());
            }
            let mut ws = store.get_workspace(&id).await?;
            ws.status = WorkspaceStatus::Active;
            ws.archived = false;
            ws.archived_at = None;
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            this.derive_last_activity(&mut ws).await;
            publish_event(
                &bus,
                workspace_updated_event(&ws.id, serde_json::json!({ "archived": false })),
            )
            .await;
            Ok(ws)
        })
    }

    fn duplicate_workspace(
        &self,
        id: WorkspaceId,
        new_title: Option<String>,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let worktree_locks = self.worktree_locks.clone();
        let this = self.clone();
        let workspaces_root = self
            .workspaces_root
            .clone()
            .unwrap_or_else(default_workspaces_root);
        Box::pin(async move {
            // Chief is virtual and never carries user content; duplication is
            // not meaningful (TS parity: coverflow never exposes a duplicate
            // affordance on the seeded row).
            if id.is_chief() {
                return Err(Error::InvalidParams(
                    "cannot duplicate chief workspace".to_string(),
                ));
            }
            let source = store.get_workspace(&id).await?;
            // Fresh id: reuse the random adjective-animal slug generator and
            // uniquify against live rows, tombstones, and stray directories.
            // Retries stay bounded so a wildly saturated slug space fails fast
            // rather than looping forever; if the budget is exhausted without
            // finding an available slug we surface `Internal` rather than
            // silently returning a colliding id.
            let mut new_id = WorkspaceId::from(intent_core::slug::generate_workspace_slug());
            let mut found = false;
            for _ in 0..32 {
                if workspace_id_available(&store, &workspaces_root, &new_id).await {
                    found = true;
                    break;
                }
                new_id = WorkspaceId::from(intent_core::slug::generate_workspace_slug());
            }
            if !found {
                return Err(Error::Internal(
                    "workspace.duplicate: exhausted slug retries without finding an available id"
                        .to_string(),
                ));
            }
            let now = now_iso();
            let title = new_title
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .unwrap_or_else(|| {
                    if source.title.trim().is_empty() {
                        "Untitled (Copy)".to_string()
                    } else {
                        format!("{} (Copy)", source.title)
                    }
                });
            // Copy the source's configuration verbatim but reset runtime state:
            // fresh id, branch pinned to the new id (TS `duplicateWorkspace`
            // parity), no PR/aggregates/token-usage carried over. Worktree
            // provisioning below mirrors `workspace.create` — set on `ws`
            // *before* the `insert_workspace` call so the row persists with
            // its `worktreePath`/`baseCommitSha` in one write.
            let mut ws = Workspace {
                id: new_id.clone(),
                title,
                branch: new_id.0.clone(),
                base_ref: source.base_ref.clone(),
                base_commit_sha: None,
                status: WorkspaceStatus::Active,
                status_message: None,
                activity: WorkspaceActivity::Idle,
                attention: WorkspaceAttention::None,
                created_at: now.clone(),
                updated_at: now.clone(),
                // Stamp the wire `lastActivity` at creation time (parity with
                // `workspace.create`) so the returned `Workspace` is never
                // missing it; the post-insert `derive_last_activity` below
                // then folds in any copied notes / activity carried over from
                // the source workspace.
                last_activity: Some(now.clone()),
                tags: source.tags.clone(),
                path: source.path.clone(),
                repository_path: source.repository_path.clone(),
                repository_owner: source.repository_owner.clone(),
                repository_name: source.repository_name.clone(),
                worktree_path: None,
                scope: source.scope.clone(),
                skip_worktree: source.skip_worktree,
                setup_script: source.setup_script.clone(),
                is_remote: source.is_remote,
                default_model: source.default_model.clone(),
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
            };
            // Provision the git worktree for the duplicate (TS
            // `duplicateWorkspace` parity, mirroring the `workspace.create`
            // flow): a duplicated local workspace off a local git repo gets a
            // linked worktree at `<root>/<workspaceId>/<repo-slug>` on a
            // branch named for its id. Skipped for remote/`skipWorktree`
            // sources and for non-repo `repositoryPath`s. Failures are logged
            // and swallowed — the FE reference continues on worktree failure
            // ("Continue without worktree - user can create it manually").
            let repo_dir = ws
                .repository_path
                .as_deref()
                .filter(|p| !p.is_empty())
                .map(PathBuf::from);
            if let Some(repo_dir) = repo_dir {
                if !ws.is_remote && !ws.skip_worktree && repo_dir.join(".git").exists() {
                    let repo_name =
                        known_repo_name(ws.repository_name.as_deref(), &repo_dir.to_string_lossy());
                    let wt_path = workspaces_root
                        .join(&ws.id.0)
                        .join(worktree_folder_slug(&repo_name));
                    // Uniquify the branch name against the source repo's
                    // local/remote-tracking branches so a `newId` collision
                    // with an existing branch (rare, but possible when the
                    // repo already carries prior workspace branches) does not
                    // fail the worktree provisioning.
                    let branch_repo = repo_dir.clone();
                    let desired = ws.branch.clone();
                    let branch_result = tokio::task::spawn_blocking(move || {
                        intent_git::branches::ensure_unique_branch_name(&branch_repo, &desired)
                    })
                    .await;
                    let branch = match branch_result {
                        Ok(Ok(b)) => b,
                        Ok(Err(e)) => {
                            tracing::warn!(
                                workspace = %ws.id.as_str(),
                                error = %e,
                                "workspace.duplicate: branch uniquification failed; falling back to workspace-id branch name"
                            );
                            ws.branch.clone()
                        }
                        Err(join_err) => {
                            tracing::warn!(
                                workspace = %ws.id.as_str(),
                                error = %join_err,
                                "workspace.duplicate: branch uniquification task failed; falling back to workspace-id branch name"
                            );
                            ws.branch.clone()
                        }
                    };
                    ws.branch = branch.clone();
                    let name = ws.id.0.clone();
                    let base_ref = ws.base_ref.clone();
                    let repo = repo_dir.clone();
                    let wt = wt_path.clone();
                    let provision_branch = branch.clone();
                    let sha_result = worktree_locks
                        .with_lock(&repo_dir, move || async move {
                            tokio::task::spawn_blocking(move || {
                                intent_git::worktree::provision_worktree(
                                    &repo,
                                    &name,
                                    &wt,
                                    &provision_branch,
                                    base_ref.as_deref(),
                                    "origin",
                                )
                            })
                            .await
                        })
                        .await;
                    // `provision_worktree` creates (or reuses) the branch
                    // before `git worktree add`; on partial failure the branch
                    // may exist without a worktree tracking it, and
                    // `workspace.delete` only cleans the branch when
                    // `worktree_path` is set. Best-effort delete it here to
                    // avoid orphaning a `<workspaceId>` branch in the source
                    // repo. Runs for both the inner-error and JoinError paths
                    // because the blocking task may have created the branch
                    // before panicking.
                    let ws_id = ws.id.clone();
                    let cleanup_orphan_branch = |reason: &'static str| {
                        // Two owned handles to the same repo path: `lock_key`
                        // is borrowed by `with_lock`, while `repo_for_delete`
                        // is moved into the blocking closure. Splitting them
                        // avoids a borrow/move conflict on a single binding.
                        let lock_key = repo_dir.clone();
                        let repo_for_delete = repo_dir.clone();
                        let cleanup_branch = branch.clone();
                        let cleanup_locks = worktree_locks.clone();
                        let cleanup_ws_id = ws_id.clone();
                        let branch_for_log = branch.clone();
                        async move {
                            let cleanup = cleanup_locks
                                .with_lock(&lock_key, move || async move {
                                    tokio::task::spawn_blocking(move || {
                                        intent_git::branches::delete_local_branch(
                                            &repo_for_delete,
                                            &cleanup_branch,
                                        )
                                    })
                                    .await
                                })
                                .await;
                            match cleanup {
                                Ok(Ok(())) => {}
                                Ok(Err(cleanup_err)) => tracing::debug!(
                                    workspace = %cleanup_ws_id.as_str(),
                                    branch = %branch_for_log,
                                    reason,
                                    error = %cleanup_err,
                                    "workspace.duplicate: best-effort branch cleanup did not delete branch (may not have been created)"
                                ),
                                Err(join_err) => tracing::warn!(
                                    workspace = %cleanup_ws_id.as_str(),
                                    branch = %branch_for_log,
                                    reason,
                                    error = %join_err,
                                    "workspace.duplicate: best-effort branch cleanup task failed"
                                ),
                            }
                        }
                    };
                    match sha_result {
                        Ok(Ok(sha)) => {
                            ws.worktree_path = Some(wt_path.to_string_lossy().to_string());
                            ws.base_commit_sha = Some(sha);
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(
                                workspace = %ws.id.as_str(),
                                error = %e,
                                "workspace.duplicate: worktree provisioning failed; continuing without worktree"
                            );
                            cleanup_orphan_branch("provision_worktree returned Err").await;
                        }
                        Err(join_err) => {
                            tracing::warn!(
                                workspace = %ws.id.as_str(),
                                error = %join_err,
                                "workspace.duplicate: worktree provisioning task failed"
                            );
                            cleanup_orphan_branch("provision_worktree task JoinError").await;
                        }
                    }
                }
            }
            store.insert_workspace(&ws).await?;
            // Record branch provenance so `workspace.delete` cleans up the
            // duplicated branch alongside its worktree (same guard as
            // `workspace.create`: only auto-generated branches are deleted).
            if ws.worktree_path.is_some() {
                if let Err(e) = store
                    .set_workspace_branch_auto_generated(&ws.id, true)
                    .await
                {
                    tracing::warn!(error = %e, "failed to record branch provenance");
                }
            }
            if let Err(e) = write_workspace_metadata_file(&workspaces_root, &ws) {
                tracing::warn!(
                    workspace = %ws.id.as_str(),
                    error = %e,
                    "workspace.duplicate: failed to write .workspace/workspace.json"
                );
            }
            publish_event(&bus, workspace_created_event(&ws)).await;
            // Seed the default spec note for the new workspace (mirrors the
            // `workspace.create` flow so `note.list` returns the well-known
            // `spec` id immediately).
            ensure_spec_note(&store, &bus, &ws.id).await?;
            // Copy over every non-`spec` note from the source with a freshly
            // minted id; per-note failures are logged and skipped so a single
            // bad row does not abort the duplication.
            match store.list_notes(&id).await {
                Ok(source_notes) => {
                    for src in source_notes {
                        if src.id.as_str() == "spec" {
                            continue;
                        }
                        let clone = Note {
                            id: NoteId::new(),
                            workspace_id: ws.id.clone(),
                            title: src.title,
                            content: src.content,
                            content_type: src.content_type,
                            tags: src.tags,
                            is_pinned: src.is_pinned,
                            is_archived: src.is_archived,
                            is_default: false,
                            parent_id: None,
                            visibility: src.visibility,
                            metadata: NoteMetadata::default(),
                            created_at: now.clone(),
                            rev: 0,
                            updated_at: now.clone(),
                        };
                        if let Err(e) = store.insert_note(&clone).await {
                            tracing::warn!(
                                workspace = %ws.id.as_str(),
                                error = %e,
                                "workspace.duplicate: failed to copy note"
                            );
                        }
                    }
                }
                Err(e) => tracing::warn!(
                    workspace = %id.as_str(),
                    error = %e,
                    "workspace.duplicate: failed to list source notes"
                ),
            }
            // Fold in copied notes/sessions so the returned `lastActivity`
            // matches what `workspace.list`/`workspace.get` would compute for
            // the new row (§9.1, mutation-path parity).
            this.derive_last_activity(&mut ws).await;
            Ok(ws)
        })
    }

    fn cleanup_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        let workspaces_root = self
            .workspaces_root
            .clone()
            .unwrap_or_else(default_workspaces_root);
        Box::pin(async move {
            // Chief is virtual: no on-disk cache and no worktree — TS parity's
            // `isVirtualWorkspace` guard.
            if id.is_chief() {
                return Ok(());
            }
            // Confirm the row exists before touching disk so a bogus id
            // surfaces a `NotFound` at the trait layer instead of silently
            // sweeping a random directory.
            let ws = store.get_workspace(&id).await?;
            // Best-effort recursive delete of the daemon-owned cache directory
            // (`<workspaces_root>/<id>/cache`). Missing / non-existent is a
            // success; other errors are logged and swallowed.
            let cache_dir = workspaces_root.join(id.as_str()).join("cache");
            let cache_task =
                tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&cache_dir) {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(e),
                })
                .await;
            match cache_task {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!(
                    workspace = %id.as_str(),
                    error = %e,
                    "workspace.cleanup: failed to remove cache directory"
                ),
                Err(join_err) => tracing::warn!(
                    workspace = %id.as_str(),
                    error = %join_err,
                    "workspace.cleanup: cache cleanup task failed"
                ),
            }
            // Run `git gc` against the worktree when there is one — reclaims
            // loose objects and packs (TS `cleanupWorkspace` parity).
            // Failures do not fail the RPC.
            if let Some(wt) = ws
                .worktree_path
                .as_deref()
                .filter(|p| !p.is_empty())
                .map(PathBuf::from)
            {
                if wt.exists() {
                    let gc = tokio::task::spawn_blocking(move || {
                        std::process::Command::new("git")
                            .arg("gc")
                            .arg("--auto")
                            .current_dir(&wt)
                            .output()
                    })
                    .await;
                    match gc {
                        Ok(Ok(out)) if !out.status.success() => {
                            tracing::warn!(
                                workspace = %id.as_str(),
                                stderr = %String::from_utf8_lossy(&out.stderr),
                                "workspace.cleanup: git gc reported non-zero exit"
                            );
                        }
                        Ok(Err(e)) => tracing::warn!(
                            workspace = %id.as_str(),
                            error = %e,
                            "workspace.cleanup: git gc failed"
                        ),
                        Err(join_err) => tracing::warn!(
                            workspace = %id.as_str(),
                            error = %join_err,
                            "workspace.cleanup: git gc task failed"
                        ),
                        _ => {}
                    }
                }
            }
            Ok(())
        })
    }

    fn find_repositories(&self, directory: String) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            let requested = PathBuf::from(&directory);
            // Normalize a relative `directory` to an absolute base against the
            // daemon's cwd before scanning so the response matches the
            // protocol contract (absolute paths). `canonicalize` is preferred
            // because it resolves symlinks and `..`, but fall back to a plain
            // `cwd.join(requested)` when the path does not exist yet so the
            // scan can still surface an empty result rather than an error.
            let root = if requested.is_absolute() {
                requested
            } else {
                match std::fs::canonicalize(&requested) {
                    Ok(p) => p,
                    Err(_) => match std::env::current_dir() {
                        Ok(cwd) => cwd.join(&requested),
                        Err(_) => requested,
                    },
                }
            };
            let scan = tokio::task::spawn_blocking(move || {
                let mut out = Vec::new();
                scan_for_repositories(&root, 0, &mut out);
                out
            })
            .await
            .map_err(|e| Error::Internal(format!("find_repositories task failed: {e}")))?;
            Ok(scan)
        })
    }

    fn initialize_repository(&self, path: String) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let repo_path = PathBuf::from(&path);
            tokio::task::spawn_blocking(move || initialize_repository_blocking(&repo_path))
                .await
                .map_err(|e| Error::Internal(format!("initialize_repository task failed: {e}")))?
        })
    }

    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            // Chief has no attention state to dismiss (synthesized as `None`).
            if id.is_chief() {
                return Ok(chief_workspace());
            }
            let mut ws = store.get_workspace(&id).await?;
            let changed = ws.attention != WorkspaceAttention::None;
            ws.attention = WorkspaceAttention::None;
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            // Self-sufficient `workspace:attention-changed` so every client clears
            // the blue dot together (PROTOCOL §6.5); emit only on an actual change.
            if changed {
                publish_event(&bus, attention_changed_event(&ws.id, ws.attention)).await;
            }
            Ok(ws)
        })
    }

    fn mark_seen(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            if id.is_chief() {
                return Ok(chief_workspace());
            }
            let mut ws = store.get_workspace(&id).await?;
            // "Seen" clears the unread flag; review-required attention persists.
            if ws.attention == WorkspaceAttention::Unread {
                ws.attention = WorkspaceAttention::None;
                ws.updated_at = now_iso();
                store.update_workspace(&ws).await?;
                publish_event(&bus, attention_changed_event(&ws.id, ws.attention)).await;
            }
            Ok(ws)
        })
    }

    fn get_token_usage(&self, id: WorkspaceId) -> BoxFuture<'_, Result<TokenUsage>> {
        let store = self.store.clone();
        Box::pin(async move {
            // The scan job is daemon-internal; this is the wire read. Surface a
            // default (empty, `lastScanAt: null`) snapshot before the first scan;
            // `NotFound` propagates so the router maps it to `-32602` (§5.23).
            let ws = store.get_workspace(&id).await?;
            Ok(ws.token_usage.unwrap_or_default())
        })
    }

    fn get_setup_script(&self, id: WorkspaceId) -> BoxFuture<'_, Result<SetupScript>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Surface a default (empty `script`, `updatedAt: 0`) record before the
            // first save; `NotFound` propagates so the router maps it to `-32602`.
            let ws = store.get_workspace(&id).await?;
            Ok(ws.setup_script.unwrap_or_else(|| SetupScript {
                script: String::new(),
                project_type: None,
                updated_at: 0,
                generated_by: None,
            }))
        })
    }

    fn save_setup_script(
        &self,
        id: WorkspaceId,
        script: String,
    ) -> BoxFuture<'_, Result<SetupScript>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            let record = setup_scripts::user_script(script);
            ws.setup_script = Some(record.clone());
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(record)
        })
    }

    fn detect_project_type(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Option<ProjectType>>> {
        let store = self.store.clone();
        Box::pin(async move {
            let ws = store.get_workspace(&id).await?;
            Ok(git_ops::worktree_path(&ws).and_then(|p| setup_scripts::detect(&p)))
        })
    }

    fn generate_setup_script(&self, id: WorkspaceId) -> BoxFuture<'_, Result<SetupScript>> {
        let store = self.store.clone();
        Box::pin(async move {
            // AI-assisted draft: the provider/agent path is not wired into this
            // service, so generation falls back to the deterministic per-project
            // template generator. Returned (not persisted) with `generatedBy:
            // "agent"`; the client persists via `saveSetupScript` (§5.25).
            let ws = store.get_workspace(&id).await?;
            let project_type = git_ops::worktree_path(&ws).and_then(|p| setup_scripts::detect(&p));
            Ok(setup_scripts::generate(project_type))
        })
    }

    fn get_repo_config(&self, id: WorkspaceId) -> BoxFuture<'_, Result<intent_core::RepoConfig>> {
        let store = self.store.clone();
        Box::pin(async move {
            let ws = store.get_workspace(&id).await?;
            let Some(repo_path) = git_ops::worktree_path(&ws) else {
                // No repo path → return empty config (tolerant, FE parity)
                return Ok(intent_core::RepoConfig::default());
            };
            Ok(repo_config::read_repo_config(&repo_path).await)
        })
    }

    fn save_repo_config(
        &self,
        id: WorkspaceId,
        config: intent_core::RepoConfig,
    ) -> BoxFuture<'_, Result<intent_core::RepoConfig>> {
        let store = self.store.clone();
        Box::pin(async move {
            let ws = store.get_workspace(&id).await?;
            let Some(repo_path) = git_ops::worktree_path(&ws) else {
                return Err(Error::Internal(
                    "Cannot save repo config: workspace has no repository path".to_string(),
                ));
            };
            repo_config::write_repo_config(&repo_path, config.clone()).await?;
            Ok(config)
        })
    }

    fn has_repo_config(&self, id: WorkspaceId) -> BoxFuture<'_, Result<bool>> {
        let store = self.store.clone();
        Box::pin(async move {
            let ws = store.get_workspace(&id).await?;
            let Some(repo_path) = git_ops::worktree_path(&ws) else {
                return Ok(false);
            };
            Ok(repo_config::has_repo_config(&repo_path))
        })
    }

    fn ensure_repo_intent_dir(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        Box::pin(async move {
            let ws = store.get_workspace(&id).await?;
            let Some(repo_path) = git_ops::worktree_path(&ws) else {
                return Err(Error::Internal(
                    "Cannot ensure .intent directory: workspace has no repository path".to_string(),
                ));
            };
            repo_config::ensure_intent_dir(&repo_path).await
        })
    }

    fn get_workspace_context(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Vec<ContextItem>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // `NotFound` on missing workspace propagates so the router maps to
            // `-32602`; an empty list is the pre-first-save default (§5.1).
            store.get_workspace(&id).await?;
            store.list_workspace_context_items(&id).await
        })
    }

    fn update_workspace_context(
        &self,
        id: WorkspaceId,
        items: Vec<ContextItem>,
    ) -> BoxFuture<'_, Result<Vec<ContextItem>>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            store.get_workspace(&id).await?;
            let persisted = store.replace_workspace_context_items(&id, &items).await?;
            publish_event(&bus, workspace_context_changed_event(&id, &persisted)).await;
            Ok(persisted)
        })
    }

    fn get_workspace_ui_context(
        &self,
        id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Option<serde_json::Value>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // `NotFound` on missing workspace propagates so the router maps to
            // `-32602`; `None` is the pre-first-save default (§5.1).
            store.get_workspace(&id).await?;
            store.get_workspace_ui_context(&id).await
        })
    }

    fn update_workspace_ui_context(
        &self,
        id: WorkspaceId,
        ui_context: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            store.get_workspace(&id).await?;
            store.update_workspace_ui_context(&id, &ui_context).await
        })
    }

    fn link_task_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_key: String,
        task_text: String,
        agent_id: String,
    ) -> BoxFuture<'_, Result<TaskAgentLink>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            store.get_workspace(&workspace_id).await?;
            let link = TaskAgentLink {
                workspace_id: workspace_id.clone(),
                note_id: note_id.clone(),
                task_key: task_key.clone(),
                task_text,
                agent_id,
                created_at: now_epoch_ms() as i64,
            };
            let stored = store.upsert_task_agent_link(&link).await?;
            publish_event(&bus, task_agent_linked_event(&stored)).await;
            Ok(stored)
        })
    }

    fn unlink_task_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_key: String,
    ) -> BoxFuture<'_, Result<bool>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            store.get_workspace(&workspace_id).await?;
            let removed = store
                .delete_task_agent_link(&workspace_id, &note_id, &task_key)
                .await?;
            if removed {
                publish_event(
                    &bus,
                    task_agent_unlinked_event(&workspace_id, &note_id, &task_key),
                )
                .await;
            }
            Ok(removed)
        })
    }

    fn list_task_agent_links(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Vec<TaskAgentLink>>> {
        let store = self.store.clone();
        Box::pin(async move {
            store.get_workspace(&workspace_id).await?;
            store.list_task_agent_links(&workspace_id).await
        })
    }

    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let id = workspace_id.clone();
        Box::pin(async move {
            // Self-heal parity with `notes.service.ts getNotes`: if the
            // well-known `spec` note is missing for this workspace, reseed it
            // before listing so pre-#110 workspaces (and any that lost their
            // spec) come back with the default. `ensure_spec_note` is
            // idempotent — no-op when the spec already exists, no spurious
            // events, no extra writes. Chief is virtual (no workspace row, no
            // FK target), so skip the reseed; `store.list_notes` returns an
            // empty vec for it naturally.
            //
            // The reseed is best-effort so `note.list` never regresses from
            // Ok(_) to Err on transient conditions the caller cannot act on:
            // an FK violation for a bare/nonexistent workspace id (previous
            // behaviour was Ok([])) and the check-then-insert race between
            // concurrent list callers (whichever loses the insert would trip
            // the unique constraint). Log-warn and fall through to
            // `store.list_notes`, which returns the freshly-seeded row when
            // the winning insert already committed, or the pre-existing
            // empty/other shape when the reseed genuinely could not run.
            if !id.is_chief() {
                if let Err(e) = ensure_spec_note(&store, &bus, &id).await {
                    tracing::warn!(
                        workspace_id = %id.0,
                        error = %e,
                        "note.list spec reseed failed; continuing with best-effort list"
                    );
                }
            }
            store.list_notes(&id).await
        })
    }

    fn get_note(&self, workspace_id: WorkspaceId, note_id: NoteId) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        Box::pin(async move { fetch_note(&store, &workspace_id, &note_id).await })
    }

    fn create_note(
        &self,
        workspace_id: WorkspaceId,
        input: NoteCreate,
        idempotency_key: Option<String>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let ws_scope = workspace_id.0.clone();
            let op_store = store.clone();
            with_idempotency(
                &store,
                &ws_scope,
                idempotency_key,
                "note.create",
                move || async move {
                    let store = op_store;
                    let now = now_iso();
                    let note = Note {
                        id: NoteId::new(),
                        workspace_id,
                        title: input.title,
                        content: input.content.unwrap_or_default(),
                        content_type: ContentType::Markdown,
                        tags: input.tags.unwrap_or_default(),
                        is_pinned: false,
                        is_archived: false,
                        is_default: false,
                        parent_id: input.parent_id.map(NoteId::from),
                        visibility: NoteVisibility::Workspace,
                        metadata: NoteMetadata::default(),
                        created_at: now.clone(),
                        rev: 0,
                        updated_at: now,
                    };
                    store.insert_note(&note).await?;
                    let author =
                        resolve_note_version_author(&store, caller_agent_id.as_ref()).await;
                    capture_note_version(&store, &note, &author).await?;
                    services.schedule_line_attribution_recompute(
                        note.workspace_id.clone(),
                        note.id.clone(),
                    );
                    let mut note = note;
                    let outcome = services
                        .auto_convert_task_blocks_after_write(
                            &note.workspace_id,
                            &note.id,
                            &note.content,
                            caller_agent_id.as_ref(),
                        )
                        .await;
                    if let Some(refetched) = outcome.refetched_note {
                        note = refetched;
                    }
                    publish_event(
                        &bus,
                        note_change_event(
                            &note.workspace_id,
                            &note.id,
                            &note.title,
                            NOTE_CREATED,
                            "create",
                        ),
                    )
                    .await;
                    Ok(note)
                },
            )
            .await
        })
    }

    fn update_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteUpdateInput,
    ) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let expected_version = input.expected_version;
            let mut note = fetch_note(&store, &workspace_id, &note_id).await?;
            // content present → raw full set; otherwise title/tags metadata.
            let content_changed = input.content.is_some();
            let mut reanchor_plan: Option<ReanchorPlan> = None;
            if let Some(content) = input.content {
                // Route the full-content write through the CRDT merge engine
                // so concurrent writers converge (§5.2 / A5 parity with
                // `CRDTNotesService.applyContentUpdate`).
                let old_content = note.content.clone();
                let merged = services.crdt_notes.apply_full_content(
                    &workspace_id,
                    &note_id,
                    &old_content,
                    &content,
                );
                // Comment anchor recovery mirrors reference `updateNote`: run
                // the merged markdown through `recoverAllPartialAnchors` before
                // persisting so surviving-partial anchors are repaired and
                // unrecoverable ones are stripped + flipped to orphaned.
                let mut plan =
                    reanchor_note_comments(&store, &workspace_id, &note_id, merged).await?;
                note.content = std::mem::take(&mut plan.content);
                reanchor_plan = Some(plan);
            } else {
                if let Some(title) = input.title {
                    note.title = title;
                }
                if let Some(tags) = input.tags {
                    note.tags = tags;
                }
            }
            note.updated_at = now_iso();
            store.update_note_versioned(&note, expected_version).await?;
            if let Some(plan) = reanchor_plan {
                plan.apply_orphaned(&store, &workspace_id).await?;
            }
            if content_changed {
                // FE-only `note.update`: no caller-agent context on this arm
                // (transport router path), so the version author is the user.
                capture_note_version(&store, &note, &user_version_author()).await?;
                services.schedule_line_attribution_recompute(
                    note.workspace_id.clone(),
                    note.id.clone(),
                );
                let outcome = services
                    .auto_convert_task_blocks_after_write(
                        &note.workspace_id,
                        &note.id,
                        &note.content,
                        None,
                    )
                    .await;
                if let Some(refetched) = outcome.refetched_note {
                    note = refetched;
                }
            }
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            Ok(note)
        })
    }

    fn add_to_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteAddInput,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let (new_content, position) = note_ops::apply_add(
                &old_content,
                &input.content,
                input.heading.as_deref(),
                input.position.as_deref(),
            )?;
            let mut plan =
                reanchor_note_comments(&store, &workspace_id, &note_id, new_content).await?;
            let new_content = std::mem::take(&mut plan.content);
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            plan.apply_orphaned(&store, &workspace_id).await?;
            let author = resolve_note_version_author(&store, caller_agent_id.as_ref()).await;
            capture_note_version(&store, &note, &author).await?;
            services
                .schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
            services.invalidate_crdt_note(&note.workspace_id, &note.id);
            let outcome = services
                .auto_convert_task_blocks_after_write(
                    &note.workspace_id,
                    &note.id,
                    &new_content,
                    caller_agent_id.as_ref(),
                )
                .await;
            let final_content = outcome
                .refetched_note
                .map(|n| n.content)
                .unwrap_or(new_content);
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            let total_length = final_content.chars().count();
            Ok(NoteAddResult {
                ok: true,
                note_id: note.id,
                added_length: input.content.chars().count(),
                total_length,
                position,
                old_content,
                new_content: final_content,
                converted_count: outcome.converted_count,
                created_task_note_ids: outcome.created_note_ids,
            })
        })
    }

    fn edit_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteEditResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            if input.old.is_empty() {
                return Err(Error::Internal(
                    "old is required and cannot be empty".to_string(),
                ));
            }
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let (new_content, match_position, was_empty) =
                note_ops::apply_edit(&old_content, &input.old, &input.new)?;
            let mut plan =
                reanchor_note_comments(&store, &workspace_id, &note_id, new_content).await?;
            let new_content = std::mem::take(&mut plan.content);
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            plan.apply_orphaned(&store, &workspace_id).await?;
            let author = resolve_note_version_author(&store, caller_agent_id.as_ref()).await;
            capture_note_version(&store, &note, &author).await?;
            services
                .schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
            services.invalidate_crdt_note(&note.workspace_id, &note.id);
            let outcome = services
                .auto_convert_task_blocks_after_write(
                    &note.workspace_id,
                    &note.id,
                    &new_content,
                    caller_agent_id.as_ref(),
                )
                .await;
            let final_content = outcome
                .refetched_note
                .map(|n| n.content)
                .unwrap_or(new_content);
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            Ok(NoteEditResult {
                ok: true,
                note_id: note.id,
                old_text_length: if was_empty {
                    0
                } else {
                    input.old.chars().count()
                },
                new_text_length: input.new.chars().count(),
                match_position,
                old_content,
                new_content: final_content,
                converted_count: outcome.converted_count,
                created_task_note_ids: outcome.created_note_ids,
            })
        })
    }

    fn edit_note_lines(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let new_content =
                note_ops::apply_edit_lines(&old_content, input.start, input.end, &input.content)?;
            let total_lines_before = old_content.split('\n').count();
            let mut plan =
                reanchor_note_comments(&store, &workspace_id, &note_id, new_content).await?;
            let new_content = std::mem::take(&mut plan.content);
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            plan.apply_orphaned(&store, &workspace_id).await?;
            let author = resolve_note_version_author(&store, caller_agent_id.as_ref()).await;
            capture_note_version(&store, &note, &author).await?;
            services
                .schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
            services.invalidate_crdt_note(&note.workspace_id, &note.id);
            let outcome = services
                .auto_convert_task_blocks_after_write(
                    &note.workspace_id,
                    &note.id,
                    &new_content,
                    caller_agent_id.as_ref(),
                )
                .await;
            let final_content = outcome
                .refetched_note
                .map(|n| n.content)
                .unwrap_or(new_content);
            let total_lines_after = final_content.split('\n').count();
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            Ok(NoteEditLinesResult {
                ok: true,
                note_id: note.id,
                start_line: input.start,
                end_line: input.end,
                total_lines_before,
                total_lines_after,
                old_content,
                new_content: final_content,
                converted_count: outcome.converted_count,
                created_task_note_ids: outcome.created_note_ids,
            })
        })
    }

    fn set_note_content(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        confirm_replacement: bool,
        expected_version: Option<i64>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let previous_title = note.title.clone();
            if !old_content.is_empty() {
                let old_len = old_content.chars().count() as f64;
                let new_len = content.chars().count() as f64;
                let reduction = (old_len - new_len) / old_len * 100.0;
                if reduction > 50.0 && !confirm_replacement {
                    return Err(Error::Internal(format!(
                        "⚠️ CONTENT REDUCTION DETECTED: Your new content ({} chars) is {}% shorter than the existing content ({} chars).\n\nThis will REPLACE the entire note. If you intended to:\n- ADD content: Use note.add instead\n- EDIT a section: Use note.edit instead\n- PROCEED with replacement: Call note.setContent again with confirmReplacement=true",
                        content.chars().count(),
                        reduction.round() as i64,
                        old_content.chars().count()
                    )));
                }
            }
            // Route the full-content write through the CRDT merge engine
            // (`CRDTNotesService.applyContentUpdate`): the yrs `Doc` is seeded
            // from `old_content` on first touch and subsequent writes diff
            // against the doc's current text, so concurrent full-content
            // writes converge instead of last-write-wins. The merged text is
            // what we then clean + persist through the normal mutation flow,
            // so the line-attribution recompute still fires downstream.
            let merged = services.crdt_notes.apply_full_content(
                &workspace_id,
                &note_id,
                &old_content,
                &content,
            );
            let clean = note_ops::clean_set_content(&merged)?;
            let mut plan = reanchor_note_comments(&store, &workspace_id, &note_id, clean).await?;
            let clean = std::mem::take(&mut plan.content);
            note.content = clean.clone();
            let now = now_iso();
            note.updated_at = now.clone();
            store.update_note_versioned(&note, expected_version).await?;
            plan.apply_orphaned(&store, &workspace_id).await?;
            let author = resolve_note_version_author(&store, caller_agent_id.as_ref()).await;
            capture_note_version(&store, &note, &author).await?;
            services
                .schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
            let outcome = services
                .auto_convert_task_blocks_after_write(
                    &note.workspace_id,
                    &note.id,
                    &clean,
                    caller_agent_id.as_ref(),
                )
                .await;
            let (final_content, final_updated_at) = match outcome.refetched_note {
                Some(n) => (n.content, n.updated_at),
                None => (clean, now),
            };
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            Ok(NoteSetContentResult {
                ok: true,
                title: note.title,
                note_id: note.id,
                previous_title: Some(previous_title),
                updated_at: final_updated_at,
                old_content: Some(old_content),
                new_content: final_content,
                converted_count: outcome.converted_count,
                created_task_note_ids: outcome.created_note_ids,
            })
        })
    }

    fn update_note_metadata(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
        expected_version: Option<i64>,
        // Metadata-only writes do not push a note-version snapshot (parity
        // with `notes.service.ts`), so the caller-agent hint is unused here.
        // Accepted for trait uniformity and future-proofing.
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            if title.is_none() && tags.is_none() {
                return Err(Error::Internal(
                    "At least one of title or tags must be provided".to_string(),
                ));
            }
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let is_spec = note_id.as_str() == "spec";
            let mut changed = false;
            // Spec title cannot be modified (matches the TS peer).
            if let Some(t) = title {
                if !is_spec {
                    note.title = t;
                    changed = true;
                }
            }
            if let Some(t) = tags {
                note.tags = t;
                changed = true;
            }
            if !changed {
                return Ok(NoteUpdateMetadataResult {
                    ok: true,
                    note_id,
                    title: None,
                    tags: None,
                    updated_at: None,
                    skipped: Some(true),
                    reason: Some("spec title cannot be modified".to_string()),
                });
            }
            let now = now_iso();
            note.updated_at = now.clone();
            store.update_note_versioned(&note, expected_version).await?;
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            Ok(NoteUpdateMetadataResult {
                ok: true,
                note_id: note.id,
                title: Some(note.title),
                tags: Some(note.tags),
                updated_at: Some(now),
                skipped: None,
                reason: None,
            })
        })
    }

    fn delete_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            // Scope-check first so a foreign/absent note yields the peer message.
            let note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            store
                .delete_note_versioned(&workspace_id, &note_id, expected_version)
                .await?;
            services.crdt_notes.remove(&workspace_id, &note_id);
            publish_event(
                &bus,
                note_change_event(&workspace_id, &note_id, &note.title, NOTE_DELETED, "delete"),
            )
            .await;
            Ok(NoteDeleteResult {
                ok: true,
                note_id,
                deleted: true,
            })
        })
    }

    fn list_note_tasks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteTaskRow>>> {
        let store = self.store.clone();
        Box::pin(async move {
            let note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            Ok(note_ops::parse_tasks(&note.content))
        })
    }

    fn read_asset(
        &self,
        workspace_id: WorkspaceId,
        asset: String,
    ) -> BoxFuture<'_, Result<ReadAssetResult>> {
        let assets_root = self.assets_root.clone();
        Box::pin(async move {
            let asset_id = note_ops::parse_asset_id(&asset)?;
            let root = assets_root
                .ok_or_else(|| Error::Internal("asset storage is not configured".to_string()))?;
            let path = root.join(&workspace_id.0).join(&asset_id);
            let bytes = std::fs::read(&path)
                .map_err(|e| Error::Internal(format!("Failed to read asset: {e}")))?;
            let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let size_kb = ((data.len() as f64) / 1024.0).round() as i64;
            let mime_type = note_ops::mime_from_extension(&asset_id);
            Ok(ReadAssetResult {
                asset_id,
                mime_type,
                data,
                size_kb,
            })
        })
    }

    fn save_asset(
        &self,
        workspace_id: WorkspaceId,
        data: String,
        mime_type: String,
        original_name: Option<String>,
    ) -> BoxFuture<'_, Result<SaveAssetResult>> {
        let assets_root = self.assets_root.clone();
        Box::pin(async move {
            let root = assets_root
                .ok_or_else(|| Error::Internal("asset storage is not configured".to_string()))?;
            let base64_data = note_ops::strip_data_url_prefix(&data);
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(base64_data.as_bytes())
                .map_err(|e| Error::Internal(format!("Failed to save asset: {e}")))?;
            let asset_id = note_ops::new_asset_id(base64_data, &mime_type);

            let dir = root.join(&workspace_id.0);
            std::fs::create_dir_all(&dir)
                .map_err(|e| Error::Internal(format!("Failed to save asset: {e}")))?;
            let path = dir.join(&asset_id);
            std::fs::write(&path, &bytes)
                .map_err(|e| Error::Internal(format!("Failed to save asset: {e}")))?;

            // Metadata sidecar, TS-peer parity (`<assetId>.meta.json`).
            let metadata = serde_json::json!({
                "id": asset_id,
                "originalName": original_name.unwrap_or_else(|| asset_id.clone()),
                "mimeType": mime_type,
                "size": bytes.len(),
                "createdAt": now_iso(),
            });
            let meta_path = dir.join(format!("{asset_id}.meta.json"));
            std::fs::write(
                &meta_path,
                serde_json::to_string_pretty(&metadata).unwrap_or_default(),
            )
            .map_err(|e| Error::Internal(format!("Failed to save asset metadata: {e}")))?;

            Ok(SaveAssetResult {
                url: format!("workspace-asset://{}/{}", workspace_id.0, asset_id),
                path: path.to_string_lossy().into_owned(),
                asset_id,
            })
        })
    }

    fn list_note_versions(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteVersionSummary>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Scope check: 404 if the note is missing or in another workspace.
            fetch_note(&store, &workspace_id, &note_id).await?;
            store.list_note_versions(&workspace_id, &note_id).await
        })
    }

    fn get_note_version(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        v: i64,
    ) -> BoxFuture<'_, Result<NoteVersion>> {
        let store = self.store.clone();
        Box::pin(async move {
            fetch_note(&store, &workspace_id, &note_id).await?;
            store.get_note_version(&workspace_id, &note_id, v).await
        })
    }

    fn restore_note_version(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        v: i64,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteRestoreVersionResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let mut note = fetch_note(&store, &workspace_id, &note_id).await?;
            let version = store.get_note_version(&workspace_id, &note_id, v).await?;
            note.title = version.title;
            note.content = version.content;
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            let author = resolve_note_version_author(&store, caller_agent_id.as_ref()).await;
            let new_v = capture_note_version(&store, &note, &author).await?;
            services
                .schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
            services.invalidate_crdt_note(&note.workspace_id, &note.id);
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            // Re-read so the returned note carries the post-update `rev`.
            let note = fetch_note(&store, &workspace_id, &note_id).await?;

            Ok(NoteRestoreVersionResult {
                ok: true,
                note_id: note.id.clone(),
                restored_from: v,
                v: new_v,
                note,
            })
        })
    }

    fn line_attribution_load(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Option<LineAttributionData>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Scope check: 404 if the note is missing or in another workspace.
            fetch_note(&store, &workspace_id, &note_id).await?;
            store
                .get_note_line_attribution(&workspace_id, &note_id)
                .await
        })
    }

    fn line_attribution_compute_now(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<LineAttributionComputeResult>> {
        let services = self.clone();
        Box::pin(async move {
            services
                .compute_and_persist_line_attribution(&workspace_id, &note_id)
                .await?;
            Ok(LineAttributionComputeResult { ok: true })
        })
    }

    fn task_update_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_text: String,
        status: String,
    ) -> BoxFuture<'_, Result<TaskUpdateStatusResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            if task_text.is_empty() {
                return Err(Error::Internal(
                    "Task text is required to identify the task".to_string(),
                ));
            }
            let checkbox = note_ops::checkbox_for(&status).ok_or_else(|| {
                Error::Internal("Status must be 'done', 'todo', or 'in-progress'".to_string())
            })?;
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let normalized = task_text.trim().to_string();
            let updated = note_ops::apply_task_status(&note.content, &normalized, checkbox)?;
            note.content = updated;
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            services.invalidate_crdt_note(&note.workspace_id, &note.id);
            services
                .schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            Ok(TaskUpdateStatusResult {
                ok: true,
                note_id: note.id,
                task_text: normalized,
                status,
            })
        })
    }

    fn task_update_note_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
        expected_version: Option<i64>,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<TaskUpdateNoteStatusResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let new_status = parse_task_status_strict(&status)?;
            let mut note = fetch_note(&store, &workspace_id, &note_id).await?;
            let mut task = match note.metadata.task.clone() {
                Some(t) => t,
                None => {
                    return Err(Error::Internal(
                        "Note is not a task. Use markAsTask() first.".to_string(),
                    ))
                }
            };
            let previous_status = task.status;
            let now = now_iso();
            apply_status_transition(&mut task, new_status, &now);
            note.metadata.task = Some(task);
            note.updated_at = now.clone();
            store.update_note_versioned(&note, expected_version).await?;
            // Mirror `notes.service.ts`: emit only when the status actually changed.
            if previous_status != new_status {
                // LC-1: agent-attributed changes carry provenance — resolve the
                // caller's display name best-effort for the agent actor.
                let agent = match caller_agent_id {
                    Some(agent_id) => Some((
                        agent_id.0.clone(),
                        store
                            .get_agent_session(&agent_id)
                            .await
                            .ok()
                            .map(|s| s.name),
                    )),
                    None => None,
                };
                publish_event(
                    &bus,
                    task_status_changed_event(
                        &note.workspace_id,
                        &note.id,
                        &note.title,
                        previous_status,
                        new_status,
                        &now,
                        agent,
                    ),
                )
                .await;
                // Then recompute + broadcast the ready-task set, mirroring the
                // `emitReadyTasksChanged` call that follows `task:status-changed`.
                let all = store.list_notes(&note.workspace_id).await?;
                let ready_task_ids = compute_ready_task_ids(&all);
                publish_event(
                    &bus,
                    ready_tasks_changed_event(
                        &note.workspace_id,
                        ready_task_ids,
                        &note.id,
                        previous_status,
                        new_status,
                        &now_iso(),
                    ),
                )
                .await;
            }
            Ok(TaskUpdateNoteStatusResult {
                ok: true,
                note_id: note.id.clone(),
                status: new_status,
                note,
            })
        })
    }

    fn task_update(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        line: i64,
        text: Option<String>,
        status: Option<String>,
        expected: Option<String>,
    ) -> BoxFuture<'_, Result<TaskUpdateResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            if text.is_none() && status.is_none() {
                return Err(Error::Internal(
                    "Either text or status (or both) must be provided".to_string(),
                ));
            }
            if line < 1 {
                return Err(Error::Internal(
                    "Line number must be a positive integer".to_string(),
                ));
            }
            if let Some(s) = status.as_deref() {
                if note_ops::checkbox_for(s).is_none() {
                    return Err(Error::Internal(
                        "Status must be 'todo', 'in-progress', or 'done'".to_string(),
                    ));
                }
            }
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let update = note_ops::apply_task_line_update(
                &note.content,
                line,
                text.as_deref(),
                status.as_deref(),
                expected.as_deref(),
            )?;
            note.content = update.content;
            note.updated_at = now_iso();
            store.update_note(&note).await?;
            services.invalidate_crdt_note(&note.workspace_id, &note.id);
            services
                .schedule_line_attribution_recompute(note.workspace_id.clone(), note.id.clone());
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            Ok(TaskUpdateResult {
                ok: true,
                note_id: note.id,
                line_number: line,
                previous_text: update.previous_text,
                new_text: update.new_text,
                status: update.status_word,
            })
        })
    }

    fn task_list(
        &self,
        workspace_id: WorkspaceId,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<TaskListResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let filter = match status.as_deref() {
                Some(s) => Some(parse_task_status_strict(s)?),
                None => None,
            };
            let notes = store.list_notes(&workspace_id).await?;
            // `stats` is the workspace-wide rollup over the full spec-linked
            // set (mirrors the FE `computeTaskStats`); the optional `status`
            // filter narrows `tasks` only.
            let stats = compute_task_stats(&notes);
            let mut tasks = workspace_task_list(&notes);
            if let Some(f) = filter {
                tasks.retain(|t| t.status == f);
            }
            Ok(TaskListResult { tasks, stats })
        })
    }

    fn task_get(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<WorkspaceTask>> {
        let store = self.store.clone();
        Box::pin(async move {
            let note = match store.get_note(&workspace_id, &task_note_id).await {
                Ok(n) => n,
                Err(Error::NotFound(_)) => {
                    return Err(Error::NotFound(format!("task note {task_note_id}")))
                }
                Err(e) => return Err(e),
            };
            note_to_workspace_task(&note)
        })
    }

    fn get_my_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let note = match store.get_note(&workspace_id, &task_note_id).await {
                Ok(n) => n,
                Err(Error::NotFound(_)) => {
                    return Err(Error::Internal("Task note not found".to_string()))
                }
                Err(e) => return Err(e),
            };
            let task = match note.metadata.task.clone() {
                Some(t) => t,
                None => return Err(Error::Internal("Note is not a task".to_string())),
            };
            let all = store.list_notes(&workspace_id).await?;
            let subtasks = all
                .iter()
                .filter(|n| n.parent_id.as_ref() == Some(&note.id) && n.metadata.task.is_some())
                .map(|n| TaskSubtask {
                    id: n.id.clone(),
                    title: n.title.clone(),
                    status: n
                        .metadata
                        .task
                        .as_ref()
                        .map(|t| status_word(t.status))
                        .unwrap_or("unknown")
                        .to_string(),
                })
                .collect();
            Ok(TaskGetMyTaskResult {
                note_id: note.id.clone(),
                title: note.title.clone(),
                content: note.content.clone(),
                status: task.status,
                parent_id: note.parent_id.clone(),
                assigned_agents: task.assigned_agent_ids.clone(),
                subtasks,
                task_metadata: task,
                rev: note.rev,
            })
        })
    }

    fn mark_as_task(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        status: String,
        acceptance_criteria: Vec<String>,
        effort: Option<String>,
    ) -> BoxFuture<'_, Result<TaskMarkAsTaskResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let new_status =
                serde_json::from_value::<TaskStatus>(serde_json::Value::String(status.clone()))
                    .map_err(|_| Error::Internal(format!("Invalid status: {status}")))?;
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let now = now_iso();
            match note.metadata.task.clone() {
                // Already a task with a changing status → preserve other fields.
                Some(mut existing) if existing.status != new_status => {
                    apply_status_transition(&mut existing, new_status, &now);
                    note.metadata.task = Some(existing);
                }
                // Fresh task (or same status) → set the markAsTask metadata.
                _ => {
                    let mut task = TaskMetadata {
                        status: new_status,
                        acceptance_criteria,
                        estimated_effort: effort,
                        ..Default::default()
                    };
                    apply_status_transition(&mut task, new_status, &now);
                    note.metadata.task = Some(task);
                }
            }
            note.updated_at = now;
            store.update_note(&note).await?;
            Ok(TaskMarkAsTaskResult {
                ok: true,
                note_id: note.id,
                status: new_status,
            })
        })
    }

    fn convert_task_blocks(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<TaskConvertBlocksResult>> {
        // Thin trait wrapper; `convert_task_blocks_op` owns the logic so the
        // auto-convert hook after every mutation can reuse it while carrying
        // the outer write's caller-agent context.
        let services = self.clone();
        Box::pin(async move {
            services
                .convert_task_blocks_op(workspace_id, note_id, caller_agent_id.as_ref())
                .await
        })
    }

    fn create_prerequisite(
        &self,
        workspace_id: WorkspaceId,
        dependent_note_id: NoteId,
        title: String,
        content: Option<String>,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<TaskCreatePrerequisiteResult>> {
        let services = self.clone();
        Box::pin(async move {
            let store = &services.store;
            // Verify the dependent note exists in this workspace.
            match store.get_note(&workspace_id, &dependent_note_id).await {
                Ok(_) => {}
                Err(Error::NotFound(_)) => {
                    return Err(Error::Internal(format!(
                        "Dependent note not found: {dependent_note_id}"
                    )))
                }
                Err(e) => return Err(e),
            }
            let status_word = status.as_deref().unwrap_or("not_started");
            let task_status = parse_task_status_strict(status_word)?;
            let child = services
                .create_child_task_note(
                    &workspace_id,
                    &dependent_note_id,
                    &title,
                    content.unwrap_or_default(),
                    task_status,
                    None,
                )
                .await?;
            Ok(TaskCreatePrerequisiteResult {
                ok: true,
                prerequisite_note_id: child.id,
                dependent_note_id,
                title: child.title,
            })
        })
    }

    fn assign_agent(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
    ) -> BoxFuture<'_, Result<TaskAssignAgentResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            if !is_valid_agent_id(&agent_id) {
                return Err(Error::Internal(format!(
                    "Invalid agentId format: \"{agent_id}\". Agent IDs must be in format \"agent-{{uuid}}\" (e.g., \"agent-b0a8044a-5eac-4b52-8456-15d3b784decb\"). To create a new agent and assign it to this task, use create_agent with taskNoteId=\"{note_id}\" instead."
                )));
            }
            let mut note = match store.get_note(&workspace_id, &note_id).await {
                Ok(n) => n,
                Err(Error::NotFound(_)) => {
                    return Err(Error::Internal(format!("Note not found: {note_id}")))
                }
                Err(e) => return Err(e),
            };
            let mut task = match note.metadata.task.clone() {
                Some(t) => t,
                None => return Err(Error::Internal(format!("Note {note_id} is not a task"))),
            };
            let agent = AgentId::from(agent_id.as_str());
            let already_assigned = task.assigned_agent_ids.contains(&agent);
            let should_update_status = task.status == TaskStatus::NotStarted;
            if already_assigned && !should_update_status {
                return Ok(TaskAssignAgentResult {
                    ok: true,
                    note_id,
                    agent_id: agent,
                });
            }
            let now = now_iso();
            let previous_status = task.status;
            if !already_assigned {
                task.assigned_agent_ids.push(agent.clone());
            }
            if should_update_status {
                apply_status_transition(&mut task, TaskStatus::InProgress, &now);
            }
            note.metadata.task = Some(task);
            note.updated_at = now.clone();
            store.update_note(&note).await?;
            // TS parity (`assignAgentToTask`): the assignment write routes
            // through `updateNote` (→ `note:updated`) and the not_started →
            // in_progress transition through `updateTaskStatus`
            // (→ `task:status-changed` + ready-tasks recompute), so
            // subscribers see the delegation live.
            publish_event(
                &bus,
                note_change_event(
                    &note.workspace_id,
                    &note.id,
                    &note.title,
                    NOTE_UPDATED,
                    "update",
                ),
            )
            .await;
            if should_update_status {
                publish_event(
                    &bus,
                    task_status_changed_event(
                        &note.workspace_id,
                        &note.id,
                        &note.title,
                        previous_status,
                        TaskStatus::InProgress,
                        &now,
                        None,
                    ),
                )
                .await;
                let all = store.list_notes(&note.workspace_id).await?;
                let ready_task_ids = compute_ready_task_ids(&all);
                publish_event(
                    &bus,
                    ready_tasks_changed_event(
                        &note.workspace_id,
                        ready_task_ids,
                        &note.id,
                        previous_status,
                        TaskStatus::InProgress,
                        &now,
                    ),
                )
                .await;
            }
            Ok(TaskAssignAgentResult {
                ok: true,
                note_id,
                agent_id: agent,
            })
        })
    }

    fn remove_agent_from_all_tasks(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<TaskRemoveAgentFromAllTasksResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let notes = store.list_notes(&workspace_id).await?;
            let now = now_iso();
            let mut updated_count: u32 = 0;
            for mut note in notes {
                let Some(mut task) = note.metadata.task.clone() else {
                    continue;
                };
                if !task.assigned_agent_ids.contains(&agent_id) {
                    continue;
                }
                task.assigned_agent_ids.retain(|id| id != &agent_id);
                note.metadata.task = Some(task);
                note.updated_at = now.clone();
                store.update_note(&note).await?;
                updated_count += 1;
            }
            Ok(TaskRemoveAgentFromAllTasksResult {
                ok: true,
                updated_count,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn comment_add(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        search_context: String,
        comment_target: String,
        comment: String,
        kind: Option<String>,
        author: Option<String>,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<CommentAddResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let services = self.clone();
        Box::pin(async move {
            let ws_scope = workspace_id.0.clone();
            let op_store = store.clone();
            // Emission lives inside the idempotency scope so a replayed add
            // (same idempotencyKey) returns the cached result without a second
            // `comment:added` (design note TB-0 §5).
            with_idempotency(
                &store,
                &ws_scope,
                idempotency_key,
                "comment.add",
                move || async move {
                    let store = op_store;
                    if comment.trim().is_empty() {
                        return Err(Error::Internal(
                            "Comment text is required and must be non-empty".to_string(),
                        ));
                    }
                    if search_context.trim().is_empty() {
                        return Err(Error::Internal(
                            "searchContext is required and must be non-empty".to_string(),
                        ));
                    }
                    if comment_target.trim().is_empty() {
                        return Err(Error::Internal(
                            "commentTarget is required and must be non-empty".to_string(),
                        ));
                    }
                    let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
                    let (from, to, line) = note_ops::find_and_anchor_text(
                        &note.content,
                        &search_context,
                        &comment_target,
                    )?;
                    let anchored_text = note.content[from..to].to_string();
                    // Capture the surrounding context BEFORE inserting the
                    // anchor markers so the stored context reflects the
                    // original text (parity with `extractAnchoredText` in
                    // `markdown-anchor-recovery.ts`). This is Audit D M1: the
                    // saved context lets a later note edit re-anchor a
                    // partial-marker survivor without needing note versions.
                    let ctx_before = note_ops::context_before(&note.content, from);
                    let ctx_after = note_ops::context_after(&note.content, to);
                    let comment_id = uuid::Uuid::new_v4().to_string();
                    note.content = format!(
                        "{}<!--anchor:{id}:start-->{anchored}<!--anchor:{id}:end-->{}",
                        &note.content[..from],
                        &note.content[to..],
                        id = comment_id,
                        anchored = anchored_text,
                    );
                    note.updated_at = now_iso();
                    store.update_note(&note).await?;
                    services.invalidate_crdt_note(&note.workspace_id, &note.id);
                    services.schedule_line_attribution_recompute(
                        note.workspace_id.clone(),
                        note.id.clone(),
                    );
                    let now = now_iso();
                    let new_comment = Comment {
                        id: comment_id.clone(),
                        thread_id: comment_id.clone(),
                        note_id: Some(note_id.clone()),
                        kind: parse_comment_type(kind.as_deref()),
                        content: comment,
                        author: author.unwrap_or_else(|| "Agent".to_string()),
                        author_type: AuthorType::Agent,
                        status: CommentStatus::Open,
                        parent_id: None,
                        anchor: CommentAnchor {
                            kind: CommentAnchorType::Range,
                            start_id: Some(comment_id.clone()),
                            end_id: Some(comment_id.clone()),
                            point_id: None,
                        },
                        anchor_text: Some(anchored_text.clone()),
                        anchor_before: if ctx_before.is_empty() {
                            None
                        } else {
                            Some(ctx_before)
                        },
                        anchor_after: if ctx_after.is_empty() {
                            None
                        } else {
                            Some(ctx_after)
                        },
                        suggestion_original: None,
                        suggestion_proposed: None,
                        agent_id: None,
                        is_orphaned: None,
                        created_at: now.clone(),
                        updated_at: now,
                    };
                    store.insert_comment(&workspace_id, &new_comment).await?;
                    publish_event(
                        &bus,
                        comment_added_event(&workspace_id, &note_id, &comment_id),
                    )
                    .await;
                    Ok(CommentAddResult {
                        success: true,
                        message: format!("Comment successfully anchored to \"{anchored_text}\""),
                        comment_id,
                        anchored: true,
                        location: CommentLocation {
                            line,
                            anchored_text,
                        },
                    })
                },
            )
            .await
        })
    }

    fn comment_list(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        since: Option<String>,
        author_type: Option<String>,
        status: Option<String>,
        include_comments: bool,
    ) -> BoxFuture<'_, Result<CommentListResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let since_dt = match &since {
                Some(s) => match parse_iso(s) {
                    Some(dt) => Some(dt),
                    None => {
                        return Err(Error::Internal(format!(
                            "Invalid 'since' timestamp: {s}. Must be ISO 8601 format."
                        )))
                    }
                },
                None => None,
            };
            if let Some(at) = author_type.as_deref() {
                if at != "user" && at != "agent" {
                    return Err(Error::Internal(format!(
                        "Invalid 'authorType': {at}. Must be 'user' or 'agent'."
                    )));
                }
            }
            if let Some(st) = status.as_deref() {
                if !["open", "resolved", "pending"].contains(&st) {
                    return Err(Error::Internal(format!(
                        "Invalid 'status': {st}. Must be 'open', 'resolved', or 'pending'."
                    )));
                }
            }
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let comments = store.list_comments(&note_id).await?;

            // Group by thread id (roots carry thread_id == id).
            let mut order: Vec<String> = Vec::new();
            let mut groups: std::collections::HashMap<String, Vec<Comment>> =
                std::collections::HashMap::new();
            for c in comments {
                let tid = c.thread_id.clone();
                if !groups.contains_key(&tid) {
                    order.push(tid.clone());
                }
                groups.entry(tid).or_default().push(c);
            }

            let mut threads: Vec<CommentThreadSummary> = Vec::new();
            for tid in order {
                let mut group = groups.remove(&tid).unwrap_or_default();
                group.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                let root = group
                    .iter()
                    .find(|c| c.parent_id.is_none())
                    .cloned()
                    .unwrap_or_else(|| group[0].clone());
                let latest = group
                    .iter()
                    .max_by(|a, b| a.updated_at.cmp(&b.updated_at))
                    .cloned()
                    .unwrap_or_else(|| group[0].clone());
                let thread_status = if group.iter().any(|c| c.status == CommentStatus::Open) {
                    "open"
                } else if group.iter().all(|c| c.status == CommentStatus::Resolved) {
                    "resolved"
                } else {
                    "pending"
                };
                let last_activity = group
                    .iter()
                    .map(|c| c.updated_at.clone())
                    .max()
                    .unwrap_or_else(|| root.updated_at.clone());
                let comment_wires = if include_comments {
                    Some(group.iter().map(CommentWire::from_comment).collect())
                } else {
                    None
                };
                threads.push(CommentThreadSummary {
                    thread_id: tid,
                    note_id: note_id.clone(),
                    targeted_text: root.anchor_text.clone(),
                    anchor_id: derive_mark_id(&root.anchor),
                    status: thread_status.to_string(),
                    created_at: root.created_at.clone(),
                    last_activity,
                    latest_comment_author: latest.author.clone(),
                    latest_comment_author_type: latest.author_type,
                    latest_comment_at: latest.updated_at.clone(),
                    comment_count: group.len(),
                    comments: comment_wires,
                });
            }

            if let Some(dt) = since_dt {
                threads.retain(|t| match parse_iso(&t.last_activity) {
                    Some(la) => la > dt,
                    None => false,
                });
            }
            if let Some(at) = author_type.as_deref() {
                threads.retain(|t| status_word_author(t.latest_comment_author_type) == at);
            }
            if let Some(st) = status.as_deref() {
                threads.retain(|t| t.status == st);
            }
            threads.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));

            let total_comments = threads.iter().map(|t| t.comment_count).sum();
            Ok(CommentListResult {
                total_threads: threads.len(),
                total_comments,
                threads,
            })
        })
    }

    fn comment_get_thread(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
    ) -> BoxFuture<'_, Result<CommentGetThreadResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            if thread_id.is_none() && comment_id.is_none() {
                return Err(Error::Internal(
                    "Either threadId or commentId must be provided".to_string(),
                ));
            }
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let all = store.list_comments(&note_id).await?;
            let target = match thread_id {
                Some(t) => t,
                None => {
                    let cid = comment_id.unwrap();
                    match all.iter().find(|c| c.id == cid) {
                        Some(c) => c.thread_id.clone(),
                        None => return Err(Error::Internal(format!("Comment not found: {cid}"))),
                    }
                }
            };
            let mut group: Vec<Comment> =
                all.into_iter().filter(|c| c.thread_id == target).collect();
            if group.is_empty() {
                return Err(Error::Internal(format!("Thread not found: {target}")));
            }
            group.sort_by(|a, b| a.created_at.cmp(&b.created_at));
            let root = group
                .iter()
                .find(|c| c.parent_id.is_none())
                .cloned()
                .unwrap_or_else(|| group[0].clone());
            let replies: Vec<CommentWire> = group
                .iter()
                .filter(|c| c.parent_id.is_some())
                .map(CommentWire::from_comment)
                .collect();
            let resolved = group.iter().all(|c| {
                matches!(
                    c.status,
                    CommentStatus::Resolved | CommentStatus::Accepted | CommentStatus::Rejected
                )
            });
            Ok(CommentGetThreadResult {
                thread_id: target,
                note_id,
                root_comment: CommentWire::from_comment(&root),
                replies,
                total_comments: group.len(),
                status: if resolved { "resolved" } else { "open" }.to_string(),
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn comment_respond(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
        comment: String,
        kind: Option<String>,
        author: Option<String>,
        suggestion_original: Option<String>,
        suggestion_proposed: Option<String>,
    ) -> BoxFuture<'_, Result<CommentRespondResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            if thread_id.is_none() && comment_id.is_none() {
                return Err(Error::Internal(
                    "Either threadId or commentId must be provided".to_string(),
                ));
            }
            if comment.trim().is_empty() {
                return Err(Error::Internal(
                    "Comment text is required and must be non-empty".to_string(),
                ));
            }
            let kind_parsed = parse_comment_type(kind.as_deref());
            if kind_parsed == CommentType::Suggestion
                && (suggestion_original.is_none() || suggestion_proposed.is_none())
            {
                return Err(Error::Internal(
                    "For type='suggestion', both suggestionOriginal and suggestionProposed are required"
                        .to_string(),
                ));
            }
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            // Workspace-scoped so a caller-supplied `commentId` cannot match a
            // comment belonging to a same-`note_id` note in a different
            // workspace (e.g. the well-known `spec` id), which would let the
            // reply's `parent_id` chain into that other workspace.
            let all = store
                .list_comments_in_workspace(&workspace_id, &note_id)
                .await?;
            let (target, parent_from_id) = match thread_id {
                Some(t) => (t, None),
                None => {
                    let cid = comment_id.unwrap();
                    match all.iter().find(|c| c.id == cid).cloned() {
                        Some(c) => (c.thread_id.clone(), Some(c)),
                        None => return Err(Error::Internal(format!("Comment not found: {cid}"))),
                    }
                }
            };
            let mut group: Vec<Comment> = all
                .iter()
                .filter(|c| c.thread_id == target)
                .cloned()
                .collect();
            if group.is_empty() {
                return Err(Error::Internal(format!("Thread not found: {target}")));
            }
            // Newest first; the parent defaults to the most recent comment.
            group.sort_by(|a, b| b.created_at.cmp(&a.created_at));
            let parent = parent_from_id.unwrap_or_else(|| group[0].clone());
            let now = now_iso();
            let reply = Comment {
                id: uuid::Uuid::new_v4().to_string(),
                thread_id: target.clone(),
                note_id: Some(note_id.clone()),
                kind: kind_parsed,
                content: comment,
                author: author.unwrap_or_else(|| "Agent".to_string()),
                author_type: AuthorType::Agent,
                status: CommentStatus::Open,
                parent_id: Some(parent.id.clone()),
                anchor: parent.anchor.clone(),
                anchor_text: parent.anchor_text.clone(),
                anchor_before: None,
                anchor_after: None,
                suggestion_original: if kind_parsed == CommentType::Suggestion {
                    suggestion_original
                } else {
                    None
                },
                suggestion_proposed: if kind_parsed == CommentType::Suggestion {
                    suggestion_proposed
                } else {
                    None
                },
                agent_id: None,
                is_orphaned: None,
                created_at: now.clone(),
                updated_at: now,
            };
            store.insert_comment(&workspace_id, &reply).await?;
            // Reference `comment.respond` dispatches the same domain event as
            // `comment.add`; publish `comment:added` so the comment channel
            // pushes the new reply without a re-read (§6.5).
            publish_event(
                &bus,
                comment_added_event(&workspace_id, &note_id, &reply.id),
            )
            .await;
            Ok(CommentRespondResult {
                success: true,
                message: "Reply added successfully".to_string(),
                comment: CommentWire::from_comment(&reply),
                thread: CommentRespondThread {
                    thread_id: target,
                    total_comments: group.len() + 1,
                },
            })
        })
    }

    fn comment_delete(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        comment_id: String,
    ) -> BoxFuture<'_, Result<CommentDeleteResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            match store.delete_comment(&workspace_id, &comment_id).await {
                Ok(()) => Ok(CommentDeleteResult {
                    success: true,
                    message: format!("Comment {comment_id} deleted from note {note_id}"),
                }),
                Err(Error::NotFound(_)) => {
                    Err(Error::Internal("Failed to delete comment".to_string()))
                }
                Err(e) => Err(e),
            }
        })
    }

    fn comment_resolve_thread(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
        resolved: bool,
    ) -> BoxFuture<'_, Result<CommentResolveThreadResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            if thread_id.is_none() && comment_id.is_none() {
                return Err(Error::Internal(
                    "Either threadId or commentId must be provided".to_string(),
                ));
            }
            fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let all = store.list_comments(&note_id).await?;
            let target = match thread_id {
                Some(t) => t,
                None => {
                    let cid = comment_id.unwrap();
                    match all.iter().find(|c| c.id == cid) {
                        Some(c) => c.thread_id.clone(),
                        None => return Err(Error::Internal(format!("Comment not found: {cid}"))),
                    }
                }
            };
            let count = all.iter().filter(|c| c.thread_id == target).count();
            if count == 0 {
                return Err(Error::Internal(format!("Thread not found: {target}")));
            }
            let new_status = if resolved {
                CommentStatus::Resolved
            } else {
                CommentStatus::Open
            };
            store
                .set_thread_status(&workspace_id, &target, new_status, &now_iso())
                .await?;
            publish_event(
                &bus,
                comment_resolved_event(&workspace_id, &note_id, &target, resolved),
            )
            .await;
            Ok(CommentResolveThreadResult {
                success: true,
                thread_id: target,
                note_id,
                resolved,
                status: if resolved { "resolved" } else { "open" }.to_string(),
                comment_count: count,
            })
        })
    }

    fn event_recent_files(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<Vec<FileActivity>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // TS `recentFiles(limit)` peer: `limit || 10` (0 is falsy → 10).
            let limit = limit.filter(|&l| l != 0).unwrap_or(10);
            let events = store.recent_files(&workspace_id, limit).await?;
            Ok(events
                .iter()
                .map(event_ops::file_activity_combined)
                .collect())
        })
    }

    fn event_agent_activity(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<String>,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            if let Some(agent_id) = agent_id {
                // `getAgentFiles(agentId, 100)`: file:changed by that agent.
                let events = store
                    .query_events(&EventQuery {
                        workspace_id: Some(workspace_id),
                        event_types: vec![intent_core::events::FILE_CHANGED.to_string()],
                        actor_type: Some(ActorType::Agent),
                        actor_id: Some(agent_id),
                        limit: Some(100),
                        ..Default::default()
                    })
                    .await?;
                let files: Vec<FileActivity> =
                    events.iter().map(event_ops::file_activity_named).collect();
                serde_json::to_value(files)
                    .map_err(|e| Error::Internal(format!("serialize agent files failed: {e}")))
            } else {
                // `getAgentActivity(minutesAgo || 30)`: agent events in-window.
                let minutes = minutes_ago.filter(|&m| m != 0).unwrap_or(30);
                let events = store
                    .query_events(&EventQuery {
                        workspace_id: Some(workspace_id),
                        actor_type: Some(ActorType::Agent),
                        since: Some(iso_minutes_ago(minutes)),
                        ..Default::default()
                    })
                    .await?;
                let activity = event_ops::aggregate_agent_activity(&events);
                serde_json::to_value(activity)
                    .map_err(|e| Error::Internal(format!("serialize agent activity failed: {e}")))
            }
        })
    }

    fn event_workspace_summary(
        &self,
        workspace_id: WorkspaceId,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<WorkspaceEventSummary>> {
        let store = self.store.clone();
        Box::pin(async move {
            // TS `workspaceSummary(minutesAgo || 60)`.
            let minutes = minutes_ago.filter(|&m| m != 0).unwrap_or(60);
            let events = store
                .query_events(&EventQuery {
                    workspace_id: Some(workspace_id),
                    since: Some(iso_minutes_ago(minutes)),
                    ..Default::default()
                })
                .await?;
            Ok(event_ops::build_workspace_summary(&events, minutes))
        })
    }

    fn event_directory_changes(
        &self,
        workspace_id: WorkspaceId,
        dir: String,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<Vec<FileActivity>>> {
        let store = self.store.clone();
        Box::pin(async move {
            if dir.is_empty() {
                return Err(Error::Internal("Directory path is required".to_string()));
            }
            // TS `directoryChanges(dir, limit || 20)`.
            let limit = limit.filter(|&l| l != 0).unwrap_or(20);
            let events = store.directory_changes(&workspace_id, &dir, limit).await?;
            Ok(events
                .iter()
                .map(event_ops::file_activity_combined)
                .collect())
        })
    }

    fn event_query(
        &self,
        workspace_id: WorkspaceId,
        params: EventQueryParams,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Opt-in pagination (TA-2 / §5.5): the `{ items, nextToken }` envelope
            // is returned only when the caller engages pagination (`paginate` or a
            // `page_token`); otherwise the legacy bare array is preserved verbatim.
            let paginate = params.paginate.unwrap_or(false) || params.page_token.is_some();
            // Mirror `buildQueryFilters`: each option is applied only when
            // truthy (empty strings / 0 are skipped); `limit || 50`.
            let legacy_limit = params.limit.filter(|&l| l != 0).unwrap_or(50);
            let mut q = EventQuery {
                workspace_id: Some(workspace_id),
                limit: Some(legacy_limit),
                ..Default::default()
            };
            if let Some(t) = params.event_type.filter(|s| !s.is_empty()) {
                q.event_types = vec![t];
            }
            if let Some(at) = params.actor_type.filter(|s| !s.is_empty()) {
                // An unrecognized actorType matches nothing (TS equals filter).
                match serde_json::from_value::<ActorType>(serde_json::Value::String(at)) {
                    Ok(parsed) => q.actor_type = Some(parsed),
                    Err(_) => {
                        return Ok(if paginate {
                            serde_json::json!({ "items": [], "nextToken": serde_json::Value::Null })
                        } else {
                            serde_json::json!([])
                        })
                    }
                }
            }
            if let Some(aid) = params.actor_id.filter(|s| !s.is_empty()) {
                q.actor_id = Some(aid);
            }
            if let Some(p) = params.path.filter(|s| !s.is_empty()) {
                q.path_prefix = Some(p);
            }
            if let Some(m) = params.minutes_ago.filter(|&m| m != 0) {
                q.since = Some(iso_minutes_ago(m));
            }
            if !paginate {
                // Legacy bare array (store yields newest→oldest).
                let events = store.query_events(&q).await?;
                return serde_json::to_value(events)
                    .map_err(|e| Error::Internal(format!("serialize events failed: {e}")));
            }
            // Paginated: clamp the page size, page backward via OFFSET, and fetch
            // one extra row to decide whether an older page remains. The store
            // already orders newest→oldest, so the page is in contract order.
            let limit = pagination::clamp_limit(params.limit);
            let offset = pagination::parse_offset(params.page_token.as_deref());
            q.limit = Some((limit + 1) as i64);
            q.offset = Some(offset as i64);
            let mut events = store.query_events(&q).await?;
            let has_more = events.len() > limit;
            if has_more {
                events.truncate(limit);
            }
            let next_token = if has_more {
                serde_json::Value::String(pagination::offset_token(offset + limit))
            } else {
                serde_json::Value::Null
            };
            let items = serde_json::to_value(events)
                .map_err(|e| Error::Internal(format!("serialize events failed: {e}")))?;
            Ok(serde_json::json!({
                "items": items,
                "nextToken": next_token,
            }))
        })
    }

    fn event_subscribe(
        &self,
        _workspace_id: WorkspaceId,
        event_types: Vec<String>,
        _exclude_self: Option<bool>,
        _batch_window: Option<i64>,
    ) -> BoxFuture<'_, Result<EventSubscribeResult>> {
        let subs = self.event_subscriptions.clone();
        Box::pin(async move {
            if event_types.is_empty() {
                return Err(Error::Internal(
                    "eventTypes is required. Specify category wildcards like \"agent:*\", \"file:*\" or specific types like \"agent:idle\".".to_string(),
                ));
            }
            // Bare `*` expands to the category wildcards (`resolveSubscriptionEventTypes`).
            let resolved = events::resolve_event_types(&event_types);
            let subscription_id = uuid::Uuid::new_v4().to_string();
            subs.lock()
                .expect("event subscription registry poisoned")
                .insert(subscription_id.clone());
            Ok(EventSubscribeResult {
                subscription_id,
                event_types: resolved,
            })
        })
    }

    fn event_unsubscribe(
        &self,
        _workspace_id: WorkspaceId,
        subscription_id: String,
    ) -> BoxFuture<'_, Result<EventUnsubscribeResult>> {
        let subs = self.event_subscriptions.clone();
        Box::pin(async move {
            if subscription_id.is_empty() {
                return Err(Error::Internal("subscriptionId is required".to_string()));
            }
            let removed = subs
                .lock()
                .expect("event subscription registry poisoned")
                .remove(&subscription_id);
            if !removed {
                return Err(Error::Internal("Subscription not found".to_string()));
            }
            Ok(EventUnsubscribeResult {
                ok: true,
                subscription_id,
            })
        })
    }

    // ========================================================================
    // git.* surface (PROTOCOL §5.6). Worktree resolution + wire policy live in
    // `git_ops`; the git operations themselves are in `intent-git` (core-only).
    // ========================================================================

    fn git_status(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<intent_core::GitStatus>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Unknown workspace / remote / non-repo all return the empty status
            // (the TS `getStatus` fallbacks), never an error.
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => return Ok(intent_git::status::empty_status()),
                Err(e) => return Err(e),
            };
            if ws.is_remote {
                return Ok(intent_git::status::empty_status());
            }
            let Some(path) = git_ops::worktree_path(&ws) else {
                return Ok(intent_git::status::empty_status());
            };
            if !path.join(".git").exists() {
                return Ok(intent_git::status::empty_status());
            }
            intent_git::status::status(&path)
        })
    }

    fn git_get_config(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<String>> {
        let store = self.store.clone();
        Box::pin(async move {
            // `NotFound` if the workspace is absent (router maps to `-32602`).
            // Remote workspaces return an empty string. For local workspaces,
            // attempts to read `.git/config` (resolving linked-worktree pointers),
            // then walks parent directories to find a containing repo (nested-repo
            // parity with FE). Returns empty string if no config found.
            let ws = store.get_workspace(&workspace_id).await?;
            if ws.is_remote {
                return Ok(String::new());
            }
            let Some(path) = git_ops::worktree_path(&ws) else {
                return Ok(String::new());
            };
            // Read `.git/config` from the worktree's git directory. For linked
            // worktrees, `.git` is a file containing `gitdir: <path>`. Resolve the
            // pointer, then look for `commondir` (which points to the main repo's
            // `.git` if present). Fallback to parent-walk for nested repo parity.
            let git_path = path.join(".git");
            let config_path = if git_path.is_file() {
                // Linked worktree: read the gitdir pointer file.
                match tokio::fs::read_to_string(&git_path).await {
                    Ok(content) => {
                        // Strip `gitdir:` prefix (tolerant of varied whitespace), quotes, and trim.
                        let trimmed = content.trim();
                        let gitdir = trimmed
                            .strip_prefix("gitdir:")
                            .map(|s| s.trim())
                            .map(|s| {
                                // If quoted, strip the quotes.
                                if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
                                    &s[1..s.len() - 1]
                                } else {
                                    s
                                }
                            })
                            .unwrap_or(trimmed);
                        // Resolve relative gitdir paths against the worktree directory.
                        let gitdir_path = if std::path::Path::new(gitdir).is_relative() {
                            path.join(gitdir)
                        } else {
                            std::path::PathBuf::from(gitdir)
                        };
                        // Check for commondir (points to main repo .git for linked worktrees).
                        let commondir_path = gitdir_path.join("commondir");
                        if let Ok(commondir_content) =
                            tokio::fs::read_to_string(&commondir_path).await
                        {
                            let commondir = commondir_content.trim();
                            gitdir_path.join(commondir).join("config")
                        } else {
                            gitdir_path.join("config")
                        }
                    }
                    Err(_) => git_path.join("config"),
                }
            } else {
                git_path.join("config")
            };
            match tokio::fs::read_to_string(&config_path).await {
                Ok(content) => Ok(content),
                Err(_) => {
                    // If direct read fails, try parent directories (mirroring the FE
                    // `readGitConfig` fallback logic for nested repos). Limit the walk
                    // to prevent data exposure from climbing too far.
                    const MAX_PARENT_LEVELS: usize = 5;
                    let mut current = path.as_path();
                    for _ in 0..MAX_PARENT_LEVELS {
                        if let Some(parent) = current.parent() {
                            let parent_git_config = parent.join(".git").join("config");
                            if let Ok(content) = tokio::fs::read_to_string(&parent_git_config).await
                            {
                                return Ok(content);
                            }
                            current = parent;
                        } else {
                            break;
                        }
                    }
                    // No config found in the parent chain (or hit depth limit).
                    Ok(String::new())
                }
            }
        })
    }

    fn git_stage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Reject `.`/`*`/`--all` and parse first (TS `ws.git.stage` order).
            let path_list = git_ops::parse_stage_paths(&paths)?;
            // All stage failures surface as `-32603` (TS wraps the whole path in
            // INTERNAL_ERROR), so a missing workspace/worktree is `Internal` too.
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to stage files: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to stage files: workspace has no worktree".to_string())
            })?;
            intent_git::stage::stage(&worktree, &path_list)?;
            Ok(path_list)
        })
    }

    fn git_unstage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Same `.`/`*`/`--all` rejection + CSV/array parse as `git.stage`.
            let path_list = git_ops::parse_stage_paths(&paths)?;
            // Mirror `git.stage`: every failure surfaces as `-32603`, so a
            // missing workspace/worktree is `Internal` too.
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to unstage files: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to unstage files: workspace has no worktree".to_string())
            })?;
            // `reset_default` is a no-op on already-unstaged paths → idempotent.
            intent_git::stage::unstage(&worktree, &path_list)?;
            Ok(path_list)
        })
    }

    fn git_discard(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Discard-specific parser: `.`/`*`/`--all` rejected in BOTH the
            // top-level string and every array element (closes the array-form
            // bypass that would otherwise let `["*"]` / `["--all"]` devolve
            // into a silent no-op). Error messages are discard-oriented.
            let path_list = git_ops::parse_discard_paths(&paths)?;
            // Workspace / worktree resolution mirrors `git.stage`: a missing
            // workspace or unset worktree is `Internal` (→ `-32603`). The
            // subsequent `intent_git::stage::discard` call may still return
            // `Error::InvalidParams` (→ `-32602`) for unsafe pathspecs
            // (e.g. `..` traversal, absolute paths outside the worktree) —
            // those propagate unchanged.
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to discard changes: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to discard changes: workspace has no worktree".to_string())
            })?;
            // Tracked paths → checkout from index; untracked → delete from
            // disk. Idempotent on clean/missing paths (matches reference).
            intent_git::stage::discard(&worktree, &path_list)?;
            Ok(path_list)
        })
    }

    fn git_stage_hunk(
        &self,
        workspace_id: WorkspaceId,
        file_path: String,
        hunk_patch: String,
    ) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            // Wire policy mirrors `git.stage`: every failure surfaces as
            // `-32603`. A missing workspace/worktree stays `Internal`.
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to stage hunk: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to stage hunk: workspace has no worktree".to_string())
            })?;
            // Read the post-mutation status snapshot INSIDE the lock so a
            // concurrent write can't slip a different state into the event
            // that this call is supposed to describe. The event itself is
            // published outside the lock (payload is already a Value).
            let status_json = locks
                .with_lock(&worktree, || async {
                    intent_git::stage::stage_hunk(&worktree, &file_path, &hunk_patch)?;
                    let status = intent_git::status::status(&worktree)
                        .unwrap_or_else(|_| intent_git::status::empty_status());
                    Ok::<_, Error>(serde_json::to_value(&status).unwrap_or(serde_json::Value::Null))
                })
                .await?;
            // Notify the FE bridge so the changes view refreshes without a
            // follow-up `git.status` read (parity with `git.stage`).
            publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            Ok(())
        })
    }

    fn git_unstage_hunk(
        &self,
        workspace_id: WorkspaceId,
        file_path: String,
        hunk_patch: String,
    ) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to unstage hunk: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to unstage hunk: workspace has no worktree".to_string())
            })?;
            // Status snapshot inside the lock — see the matching note on
            // `git_stage_hunk` above.
            let status_json = locks
                .with_lock(&worktree, || async {
                    intent_git::stage::unstage_hunk(&worktree, &file_path, &hunk_patch)?;
                    let status = intent_git::status::status(&worktree)
                        .unwrap_or_else(|_| intent_git::status::empty_status());
                    Ok::<_, Error>(serde_json::to_value(&status).unwrap_or(serde_json::Value::Null))
                })
                .await?;
            publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            Ok(())
        })
    }

    fn git_push(
        &self,
        workspace_id: WorkspaceId,
        force: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to push: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to push: workspace has no worktree".to_string())
            })?;
            // Resolve the currently-checked-out branch (FE parity — `git push`
            // with no args pushes HEAD's upstream, which for our worktrees is
            // the current branch on `origin`). Resolve inside the lock so a
            // concurrent checkout/rename cannot change HEAD between the read
            // and the push. The status snapshot for the follow-up
            // `changes:git-status` event is taken inside the lock too, so a
            // concurrent write cannot slip a different state into the event
            // this call is supposed to describe (same pattern as
            // `git_stage_hunk`/`git_unstage_hunk`).
            let (outcome, status_json) = locks
                .with_lock(&worktree, || async {
                    let branch =
                        intent_git::status::current_branch_at(&worktree).ok_or_else(|| {
                            Error::Internal(
                                "Failed to push: no branch checked out (detached HEAD?)"
                                    .to_string(),
                            )
                        })?;
                    let outcome = intent_git::push::push(&worktree, "origin", &branch, force)?;
                    let status = intent_git::status::status(&worktree)
                        .unwrap_or_else(|_| intent_git::status::empty_status());
                    let status_json =
                        serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
                    Ok::<_, Error>((outcome, status_json))
                })
                .await?;
            publish_event(
                &bus,
                git_push_event(&ws.id, &outcome.branch, &outcome.pushed_sha, force),
            )
            .await;
            publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            Ok(serde_json::json!({
                "branch": outcome.branch,
                "pushedSha": outcome.pushed_sha,
            }))
        })
    }

    fn git_fetch(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to fetch: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to fetch: workspace has no worktree".to_string())
            })?;
            // Resolve HEAD's branch inside the lock so a concurrent
            // checkout/rename cannot change it between the read and the fetch.
            // The status snapshot is taken inside the lock too, so the
            // follow-up `changes:git-status` event reflects the tree state
            // this fetch produced rather than a state a concurrent writer
            // slipped in between the unlock and the read (same pattern as
            // `git_stage_hunk`/`git_unstage_hunk`).
            let status_json = locks
                .with_lock(&worktree, || async {
                    let branch =
                        intent_git::status::current_branch_at(&worktree).ok_or_else(|| {
                            Error::Internal(
                                "Failed to fetch: no branch checked out (detached HEAD?)"
                                    .to_string(),
                            )
                        })?;
                    intent_git::fetch::fetch(&worktree, "origin", &branch)?;
                    let status = intent_git::status::status(&worktree)
                        .unwrap_or_else(|_| intent_git::status::empty_status());
                    Ok::<_, Error>(serde_json::to_value(&status).unwrap_or(serde_json::Value::Null))
                })
                .await?;
            // Refresh the FE change view without a follow-up `git.status` read.
            publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            Ok(())
        })
    }

    fn git_create_branch(
        &self,
        workspace_id: WorkspaceId,
        branch_name: String,
        checkout: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            if branch_name.trim().is_empty() {
                return Err(Error::InvalidParams("branchName is required".to_string()));
            }
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to create branch: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to create branch: workspace has no worktree".to_string())
            })?;
            let name = branch_name.clone();
            locks
                .with_lock(&worktree, || async {
                    intent_git::branches::create_branch(&worktree, &name, checkout)
                })
                .await?;
            publish_event(&bus, git_branch_event(&ws.id, "create", &branch_name, None)).await;
            if checkout {
                let status = intent_git::status::status(&worktree)
                    .unwrap_or_else(|_| intent_git::status::empty_status());
                let status_json = serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
                publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            }
            Ok(serde_json::json!({ "branch": branch_name }))
        })
    }

    fn git_checkout_branch(
        &self,
        workspace_id: WorkspaceId,
        branch_name: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            if branch_name.trim().is_empty() {
                return Err(Error::InvalidParams("branchName is required".to_string()));
            }
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to checkout branch: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to checkout branch: workspace has no worktree".to_string())
            })?;
            let name = branch_name.clone();
            locks
                .with_lock(&worktree, || async {
                    intent_git::branches::checkout_branch(&worktree, &name)
                })
                .await?;
            publish_event(
                &bus,
                git_branch_event(&ws.id, "checkout", &branch_name, None),
            )
            .await;
            let status = intent_git::status::status(&worktree)
                .unwrap_or_else(|_| intent_git::status::empty_status());
            let status_json = serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
            publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            Ok(serde_json::json!({ "branch": branch_name }))
        })
    }

    fn git_rename_branch(
        &self,
        workspace_id: WorkspaceId,
        old_branch_name: String,
        new_branch_name: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            if old_branch_name.trim().is_empty() {
                return Err(Error::InvalidParams(
                    "oldBranchName cannot be empty".to_string(),
                ));
            }
            let trimmed_new = new_branch_name.trim().to_string();
            if trimmed_new.is_empty() {
                return Err(Error::InvalidParams(
                    "newBranchName cannot be empty".to_string(),
                ));
            }
            // Same-name rename is a no-op (TS parity — reference returns `ok`
            // without touching git).
            if old_branch_name == trimmed_new {
                return Ok(serde_json::json!({
                    "oldBranch": old_branch_name,
                    "newBranch": trimmed_new,
                }));
            }
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to rename branch: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to rename branch: workspace has no worktree".to_string())
            })?;
            let old = old_branch_name.clone();
            let new = trimmed_new.clone();
            locks
                .with_lock(&worktree, || async {
                    intent_git::branches::rename_branch(&worktree, &old, &new)
                })
                .await?;
            publish_event(
                &bus,
                git_branch_event(&ws.id, "rename", &trimmed_new, Some(&old_branch_name)),
            )
            .await;
            // Renaming the currently-checked-out branch changes the
            // `git.status` `branch` field, so refresh the FE change view.
            let status = intent_git::status::status(&worktree)
                .unwrap_or_else(|_| intent_git::status::empty_status());
            let status_json = serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
            publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            Ok(serde_json::json!({
                "oldBranch": old_branch_name,
                "newBranch": trimmed_new,
            }))
        })
    }

    fn git_remove_lock_file(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let locks = self.worktree_locks.clone();
        Box::pin(async move {
            // Unknown workspace → surface as `-32603`; a bare `NotFound` here
            // would be misleading given the FE's `Result` shape.
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to remove lock file: {e}")))?;
            // Remote workspaces skip the local fs operation (TS parity — the
            // reference short-circuits `{ removed: false }` here too).
            if ws.is_remote {
                return Ok(serde_json::json!({ "removed": false }));
            }
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to remove lock file: workspace has no worktree".to_string())
            })?;
            // Hold the per-worktree lock so a stale-lock cleanup cannot race
            // with a healthy in-flight write and delete its live `index.lock`.
            let removed = locks
                .with_lock(&worktree, || async {
                    intent_git::worktree::remove_index_lock(&worktree)
                })
                .await?;
            Ok(serde_json::json!({ "removed": removed }))
        })
    }

    fn git_get_branches(
        &self,
        repo_path: String,
        include_remote: bool,
    ) -> BoxFuture<'_, Result<intent_core::GitBranches>> {
        Box::pin(async move {
            // Path-based read used by the workspace-create flow *before* the
            // repo is registered: any existing local git repo is accepted
            // (matching the ungated TS handler); nonexistent / non-git paths
            // are `-32602` (PROTOCOL §5.6).
            git_ops::validate_repo_path(&repo_path)?;
            intent_git::branches::get_branches(std::path::Path::new(&repo_path), include_remote)
        })
    }

    fn git_branch_status(
        &self,
        repo_path: String,
        branch_name: String,
    ) -> BoxFuture<'_, Result<intent_core::GitBranchStatus>> {
        Box::pin(async move {
            // Same repo-path validation as `git_get_branches` (PROTOCOL §5.6).
            git_ops::validate_repo_path(&repo_path)?;
            intent_git::branches::branch_status(std::path::Path::new(&repo_path), &branch_name)
        })
    }

    fn git_pull(
        &self,
        repo_path: String,
        branch_name: String,
    ) -> BoxFuture<'_, Result<intent_core::GitPullResult>> {
        let store = self.store.clone();
        let event_bus = self.event_bus.clone();
        Box::pin(async move {
            // Path-based like `git_get_branches`: the workspace-create auto-pull
            // runs *before* the repo is registered, so any existing local git
            // repo is accepted; nonexistent / non-git paths are `-32602`
            // (PROTOCOL §5.6).
            git_ops::validate_repo_path(&repo_path)?;
            if branch_name.trim().is_empty() {
                return Err(Error::InvalidParams("branchName is required".to_string()));
            }
            // libgit2's pull is blocking and has no cancellation, so drive it
            // through `spawn_blocking` + a wall-clock timeout to keep a slow /
            // hung remote from wedging the async runtime (the vector behind the
            // FE `git.pull` and `host.status` timeouts). On timeout the caller
            // gets the structured `{ ok: false, error }` failure shape — never
            // a JSON-RPC error — matching the TS PULL_BRANCH contract.
            let pull_path = std::path::PathBuf::from(&repo_path);
            let pull_branch = branch_name.clone();
            let outcome = git_pull_bounded(
                move || intent_git::pull::pull_branch(&pull_path, &pull_branch),
                GIT_PULL_TIMEOUT,
            )
            .await?;
            // Best-effort workspace resolution: `git.pull` is path-scoped (the
            // workspace-create auto-pull runs before the workspace row exists),
            // so we look up any workspace pointing at this worktree and skip
            // event emission when none is found. Failing to find a workspace
            // is a normal path — no error, no event.
            if outcome.ok {
                if let Some(ws) = find_workspace_by_worktree_path(&store, &repo_path).await {
                    // `git:pull` mirrors the reserved `GitOperationEvent` FE
                    // shape; `changes:git-status` feeds the FE bridge's
                    // `git:status-changed` relay so the UI refreshes without a
                    // follow-up `git.status` read.
                    publish_event(&event_bus, git_pull_event(&ws.id, &branch_name)).await;
                    let status = intent_git::status::status(std::path::Path::new(&repo_path))
                        .unwrap_or_else(|_| intent_git::status::empty_status());
                    let status_json =
                        serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
                    publish_event(&event_bus, changes_git_status_event(&ws.id, status_json)).await;
                }
            }
            Ok(outcome)
        })
    }

    fn git_clone(
        &self,
        url: String,
        parent_dir: String,
        target_name: Option<String>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let bus = self.event_bus.clone();
        Box::pin(async move {
            if url.trim().is_empty() {
                return Err(Error::InvalidParams("url is required".to_string()));
            }
            let target = clone_ops::resolve_target_path(&parent_dir, &url, target_name.as_deref())?;
            if clone_ops::target_exists(&target) {
                return Err(Error::Internal(format!(
                    "target already exists: {}",
                    target.display()
                )));
            }
            let request_id = request_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let bus = bus.ok_or_else(|| {
                Error::Internal("event bus not wired; git.clone requires streaming".to_string())
            })?;
            clone_ops::spawn_clone(clone_ops::CloneJob {
                request_id: request_id.clone(),
                workspace_id: None,
                url,
                target_path: target.clone(),
                bus,
            });
            Ok(serde_json::json!({
                "requestId": request_id,
                "targetPath": target.to_string_lossy(),
            }))
        })
    }

    fn repo_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Return the known repos immediately — don't block on the sync.
            let repos = store.list_known_repos().await?;
            // One-time background sync: register repos from existing workspaces
            // so pre-existing workspaces (created before this feature) get
            // picked up. Spawned so it never blocks/fails the response, and
            // guarded to run at most once per process (TS `repoRegistrySynced`).
            if REPO_REGISTRY_SYNCED
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                let store = store.clone();
                tokio::spawn(async move {
                    if let Err(e) = sync_repos_from_workspaces(&store).await {
                        tracing::warn!(error = %e, "failed to sync workspace repos to registry");
                    }
                });
            }
            Ok(serde_json::json!({ "repos": repos }))
        })
    }

    fn repo_remove(&self, path: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let removed = store.remove_known_repo(&path).await?;
            Ok(serde_json::json!({ "removed": removed }))
        })
    }

    fn git_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<intent_core::GitCommitResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let ws_scope = workspace_id.0.clone();
            let op_store = store.clone();
            let event_bus = bus.clone();
            let event_message = message.clone();
            with_idempotency(
                &store,
                &ws_scope,
                idempotency_key,
                "git.commit",
                move || async move {
                    let store = op_store;
                    // TS `ws.git.commit` gates on auto-commit (no userRequested bypass).
                    git_ops::assert_agent_commit_allowed(&store, false).await?;
                    // All commit failures surface as `-32603` (the TS handler wraps the
                    // whole path in INTERNAL_ERROR), so a missing workspace is `Internal`.
                    let ws = store
                        .get_workspace(&workspace_id)
                        .await
                        .map_err(|e| Error::Internal(format!("Failed to commit: {e}")))?;
                    let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                        Error::Internal("Failed to commit: workspace has no worktree".to_string())
                    })?;
                    let outcome = intent_git::commit::commit(&worktree, &message)?;
                    // Emissions live inside the idempotency scope so a replayed
                    // commit (same idempotencyKey) returns the cached result
                    // without re-firing events (parity with `workspace:created`
                    // §6.5). `git:commit` mirrors the reserved
                    // `GitOperationEvent` FE shape; `changes:git-status` feeds
                    // the FE bridge's `git:status-changed` relay so the UI
                    // refreshes without a follow-up `git.status` read.
                    publish_event(
                        &event_bus,
                        git_commit_event(&ws.id, &outcome.hash, &event_message, &outcome.files),
                    )
                    .await;
                    let status = intent_git::status::status(&worktree)
                        .unwrap_or_else(|_| intent_git::status::empty_status());
                    let status_json =
                        serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
                    publish_event(&event_bus, changes_git_status_event(&ws.id, status_json)).await;
                    Ok(intent_core::GitCommitResult {
                        hash: outcome.hash,
                        files: outcome.files,
                    })
                },
            )
            .await
        })
    }

    fn git_agent_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        agent_id: Option<AgentId>,
        linked_note_id: Option<NoteId>,
        files: Option<Vec<String>>,
        user_requested: bool,
    ) -> BoxFuture<'_, Result<intent_core::GitAgentCommitResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            // userRequested bypasses the auto-commit gate (TS parity).
            git_ops::assert_agent_commit_allowed(&store, user_requested).await?;
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to commit: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to commit: workspace has no worktree".to_string())
            })?;
            // PARITY NOTE: TS filters to the agent's own unstaged changes via the
            // file-tracking attribution pipeline (deferred — not yet ported). Until
            // it lands, an explicit `files` list is committed as-is; without one, a
            // `userRequested` checkpoint commits only the already-staged paths
            // (plain `git commit` semantics — a user checkpoint must not sweep up
            // other agents' worktree changes), while an agent-initiated commit
            // falls back to every changed path in the worktree.
            let (to_commit, needs_stage) = match files {
                Some(f) if !f.is_empty() => (f, true),
                _ if user_requested => (intent_git::commit::staged_paths(&worktree)?, false),
                _ => (intent_git::commit::all_changed_paths(&worktree)?, true),
            };
            if to_commit.is_empty() {
                return Err(Error::Internal(
                    "No uncommitted changes found for this agent".to_string(),
                ));
            }
            if needs_stage {
                intent_git::stage::stage(&worktree, &to_commit)?;
            }
            let outcome = intent_git::commit::commit_with_trailers(
                &worktree,
                &message,
                agent_id.as_ref().map(AgentId::as_str),
                linked_note_id.as_ref().map(NoteId::as_str),
            )?;
            let file_count = to_commit.len() as i64;
            // `git:commit` mirrors the reserved `GitOperationEvent` FE shape;
            // `changes:git-status` feeds the FE bridge's `git:status-changed`
            // relay so the UI refreshes without a follow-up `git.status` read.
            publish_event(
                &bus,
                git_commit_event(&ws.id, &outcome.hash, &message, &to_commit),
            )
            .await;
            let status = intent_git::status::status(&worktree)
                .unwrap_or_else(|_| intent_git::status::empty_status());
            let status_json = serde_json::to_value(&status).unwrap_or(serde_json::Value::Null);
            publish_event(&bus, changes_git_status_event(&ws.id, status_json)).await;
            Ok(intent_core::GitAgentCommitResult {
                hash: outcome.hash,
                files: to_commit,
                file_count,
            })
        })
    }

    fn git_check_merge_conflicts(
        &self,
        workspace_id: WorkspaceId,
        target_branch: Option<String>,
    ) -> BoxFuture<'_, Result<intent_core::GitMergeConflicts>> {
        let store = self.store.clone();
        Box::pin(async move {
            // TS throws 'Could not find workspace git info' when the workspace or
            // its worktree can't be resolved.
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|_| Error::Internal("Could not find workspace git info".to_string()))?;
            let worktree = git_ops::worktree_path(&ws)
                .ok_or_else(|| Error::Internal("Could not find workspace git info".to_string()))?;

            let current_branch = intent_git::conflicts::current_branch(&worktree)?;
            if current_branch.is_empty() {
                return Err(Error::Internal(
                    "Failed to get current branch: No branch checked out (detached HEAD?)"
                        .to_string(),
                ));
            }

            let target = match target_branch {
                Some(t) => t,
                None => intent_git::conflicts::detect_default_branch(&worktree)?.ok_or_else(
                    || {
                        Error::Internal(
                            "Could not determine target branch. Please specify a targetBranch parameter."
                                .to_string(),
                        )
                    },
                )?,
            };

            // Same branch can't conflict with itself (TS short-circuit).
            if current_branch == target {
                return Ok(intent_core::GitMergeConflicts {
                    has_conflicts: false,
                    conflicted_files: Vec::new(),
                    cannot_determine: None,
                    target_branch: target,
                    current_branch,
                });
            }

            let mc =
                intent_git::conflicts::detect_merge_conflicts(&worktree, &current_branch, &target)?;
            Ok(intent_core::GitMergeConflicts {
                has_conflicts: mc.has_conflicts,
                conflicted_files: mc.conflicted_files,
                cannot_determine: if mc.cannot_determine {
                    Some(true)
                } else {
                    None
                },
                target_branch: target,
                current_branch,
            })
        })
    }

    fn git_changes(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Same empty fallbacks as `git_status` (unknown/remote/non-repo →
            // empty list), but projecting only the working-tree file list.
            let empty = serde_json::json!([]);
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => return Ok(empty),
                Err(e) => return Err(e),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(path) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !path.join(".git").exists() {
                return Ok(empty);
            }
            let status = intent_git::status::status(&path)?;
            Ok(serde_json::to_value(&status.files).unwrap_or(empty))
        })
    }

    fn git_diffs(
        &self,
        workspace_id: WorkspaceId,
        path: Option<String>,
        staged: bool,
        commit_hash: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let empty = serde_json::json!([]);
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => return Ok(empty),
                Err(e) => return Err(e),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !worktree.join(".git").exists() {
                return Ok(empty);
            }
            git_ops::build_diffs(&worktree, path.as_deref(), staged, commit_hash.as_deref())
        })
    }

    fn git_commit_details(
        &self,
        workspace_id: WorkspaceId,
        commit_hash: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let empty = git_ops::empty_commit_details(&commit_hash);
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => return Ok(empty),
                Err(e) => return Err(e),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !worktree.join(".git").exists() {
                return Ok(empty);
            }
            match git_ops::build_commit_details(&worktree, &commit_hash) {
                Ok(value) => Ok(value),
                Err(Error::NotFound(_)) => Ok(empty),
                Err(e) => Err(e),
            }
        })
    }

    fn git_commits(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // §5.5 paginated read: clamp the page size to [1,200] (default 50)
            // and walk backward through the (newest-first) first-parent history
            // via an opaque skip token; the envelope is `{ items, nextToken }`.
            let limit = pagination::clamp_limit(limit);
            let skip = pagination::parse_offset(page_token.as_deref());
            let empty = serde_json::json!({ "items": [], "nextToken": serde_json::Value::Null });
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(_) => return Ok(empty),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !worktree.join(".git").exists() {
                return Ok(empty);
            }
            // Fetch one past the page window to decide whether older commits remain.
            let commits = intent_git::history::history(&worktree, skip + limit + 1)?;
            let has_more = commits.len() > skip + limit;
            let items: Vec<serde_json::Value> = commits
                .iter()
                .skip(skip)
                .take(limit)
                .map(git_ops::commit_to_commit_info)
                .collect();
            let next_token = if has_more {
                serde_json::Value::String(pagination::offset_token(skip + limit))
            } else {
                serde_json::Value::Null
            };
            Ok(serde_json::json!({ "items": items, "nextToken": next_token }))
        })
    }

    fn git_show_file(
        &self,
        workspace_id: WorkspaceId,
        file_path: String,
        git_ref: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Same empty fallbacks as the other `git.*` reads (unknown/remote/
            // non-repo → empty content); a missing path at the ref is also
            // empty (legacy `git:show-file` parity), while a bad ref errors.
            let empty = serde_json::json!({ "content": "" });
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => return Ok(empty),
                Err(e) => return Err(e),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !worktree.join(".git").exists() {
                return Ok(empty);
            }
            let content = intent_git::show::show_file(&worktree, &git_ref, &file_path)?;
            Ok(serde_json::json!({ "content": content }))
        })
    }

    fn git_numstat(
        &self,
        workspace_id: WorkspaceId,
        staged: Option<bool>,
        base_ref: Option<String>,
        base_sha: Option<String>,
        target_ref: Option<String>,
        paths: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let empty = serde_json::json!([]);
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => return Ok(empty),
                Err(e) => return Err(e),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !worktree.join(".git").exists() {
                return Ok(empty);
            }
            let target = target_ref
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("HEAD");
            git_ops::build_numstat(
                &worktree,
                staged,
                base_ref.as_deref(),
                base_sha.as_deref(),
                target,
                paths.as_deref(),
            )
        })
    }

    fn git_branch_diff(
        &self,
        workspace_id: WorkspaceId,
        base_ref: Option<String>,
        base_sha: Option<String>,
        target_ref: Option<String>,
        paths: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Router already rejects the missing-both-bases case as -32602
            // (see the `git.branchDiff` arm); this is a defense-in-depth
            // check for direct trait callers.
            if base_ref.as_deref().unwrap_or("").is_empty()
                && base_sha.as_deref().unwrap_or("").is_empty()
            {
                return Err(Error::InvalidParams(
                    "git.branchDiff requires baseRef or baseCommitSha".to_string(),
                ));
            }
            let empty = serde_json::json!([]);
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(Error::NotFound(_)) => return Ok(empty),
                Err(e) => return Err(e),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !worktree.join(".git").exists() {
                return Ok(empty);
            }
            let target = target_ref
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("HEAD");
            git_ops::build_branch_diff(
                &worktree,
                base_ref.as_deref(),
                base_sha.as_deref(),
                target,
                paths.as_deref(),
            )
        })
    }

    fn git_get_remote_url(
        &self,
        repo_path: String,
        remote_name: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // Path-based validation like `git_get_branches`/`git_branch_status`:
            // nonexistent / non-git repo path → -32602 (PROTOCOL §5.6).
            git_ops::validate_repo_path(&repo_path)?;
            let name = remote_name
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("origin");
            let url = intent_git::remote::remote_url(std::path::Path::new(&repo_path), name)?;
            Ok(serde_json::json!({ "url": url }))
        })
    }

    // ========================================================================
    // agent.* surface (PROTOCOL §5.5). Store/in-memory-backed; the live-runtime
    // coupling (spawning a turn from sendMessage, flipping `queued` mid-stream)
    // lands with the end-to-end orchestration flow. Helpers live in `agent_ops`.
    // ========================================================================

    fn agent_delegate(
        &self,
        workspace_id: WorkspaceId,
        input: AgentDelegateInput,
        parent_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_delegate_op(workspace_id, input, parent_agent_id)
                .await
        })
    }

    fn agent_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        Box::pin(async move { self.agent_list_op(workspace_id).await })
    }

    fn agent_get(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<AgentLite>> {
        Box::pin(async move { self.agent_get_op(agent_id, workspace_id).await })
    }

    fn agent_get_conversation(
        &self,
        agent_id: AgentId,
        limit: Option<i64>,
        workspace_id: Option<WorkspaceId>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_get_conversation_op(agent_id, limit, workspace_id, page_token)
                .await
        })
    }

    fn agent_get_session(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<AgentSession>> {
        Box::pin(async move { self.agent_get_session_op(agent_id).await })
    }

    fn agent_update(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
        changes: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_update_op(agent_id, changes).await })
    }

    fn agent_append_message(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
        role: String,
        content: serde_json::Value,
        metadata: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_append_message_op(agent_id, role, content, metadata)
                .await
        })
    }

    fn agent_replace_messages(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
        messages: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_replace_messages_op(agent_id, messages).await })
    }

    fn agent_live_turn(&self, agent_id: AgentId) -> Option<serde_json::Value> {
        let live = self.live_turn(&agent_id)?;
        // An empty (just-begun) turn has nothing to reconstruct yet; surfacing it
        // would leave an empty in-flight message envelope in the snapshot.
        if live.blocks.is_empty() {
            return None;
        }
        Some(serde_json::json!({
            "messageId": live.message_id,
            "contentBlocks": live.blocks,
        }))
    }

    fn agent_is_busy(&self, agent_id: AgentId) -> bool {
        // Production: defer to the attached [`AgentManager`] — its in-flight
        // [`AgentManager::is_busy`] set is the authoritative "active worker"
        // signal that gates the chat snapshot's live-turn merge. The test seam
        // below covers unit/UDS tests that simulate a mid-turn without spawning
        // a real manager.
        if let Some(manager) = self.agent_manager() {
            if manager.is_busy(&agent_id) {
                return true;
            }
        }
        match self.test_busy.lock() {
            Ok(set) => set.contains(&agent_id),
            Err(_) => false,
        }
    }

    fn agent_activity_flags(&self, agent_id: AgentId) -> BoxFuture<'_, serde_json::Value> {
        Box::pin(async move {
            // Load the session so the flags honor the same terminal-status
            // short-circuit and parent-watch lookup as the `AgentLite`
            // projection (see [`agent_activity_flags_for`]). On a read error the
            // snapshot degrades to all-`false` rather than failing the
            // subscription (matching `chat_snapshot`'s degrade-to-empty pattern).
            match self.store.get_agent_session(&agent_id).await {
                Ok(session) => {
                    let (
                        is_responding,
                        is_waiting_on_tool,
                        is_waiting_for_other_agents,
                        waiting_for_agent_ids,
                    ) = self.agent_activity_flags_for(&session);
                    serde_json::json!({
                        "isResponding": is_responding,
                        "isWaitingOnTool": is_waiting_on_tool,
                        "isWaitingForOtherAgents": is_waiting_for_other_agents,
                        "waitingForAgentIds": waiting_for_agent_ids,
                    })
                }
                Err(_) => serde_json::json!({
                    "isResponding": false,
                    "isWaitingOnTool": false,
                    "isWaitingForOtherAgents": false,
                    "waitingForAgentIds": [],
                }),
            }
        })
    }

    fn agent_create(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        specialist_id: Option<String>,
        parent_agent_id: Option<AgentId>,
        idempotency_key: Option<String>,
        requested_agent_id: Option<AgentId>,
        extra: intent_core::AgentCreateExtra,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let ws_scope = workspace_id.0.clone();
            with_idempotency(
                &self.store,
                &ws_scope,
                idempotency_key,
                "agent.create",
                move || async move {
                    self.agent_create_op(
                        workspace_id,
                        name,
                        model,
                        specialist_id,
                        parent_agent_id,
                        None,
                        false,
                        requested_agent_id,
                        extra,
                    )
                    .await
                },
            )
            .await
        })
    }

    fn agent_send_to_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        message: String,
        priority: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_send_to_task_op(workspace_id, task_note_id, message, priority)
                .await
        })
    }

    fn agent_send_message(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        content: String,
        message_id: Option<String>,
        image_blocks: Option<serde_json::Value>,
        file_blocks: Option<serde_json::Value>,
        priority: Option<String>,
        note_ids: Option<serde_json::Value>,
        stdin_context: Option<String>,
        context_references: Option<serde_json::Value>,
        message_metadata: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // When the runtime manager is attached, drive a real spawn/turn loop;
            // otherwise fall back to the store-only persist (read-only wiring).
            // `priority: "interrupt"` preempts the in-flight turn keep-alive
            // (the child is never killed) and streams the message immediately.
            match self.agent_manager() {
                Some(manager) => {
                    // Construct TurnOptions only when the manager is attached
                    // to avoid cloning large payloads on the fallback path.
                    let options = crate::agent_manager::TurnOptions {
                        stdin_context,
                        note_ids,
                        context_references,
                        image_blocks,
                        file_blocks,
                        message_metadata,
                    };
                    if crate::agent_ops::is_interrupt_priority(priority.as_deref()) {
                        manager
                            .interrupt_send_message(
                                agent_id,
                                workspace_id,
                                content,
                                message_id,
                                options,
                            )
                            .await
                    } else {
                        manager
                            .send_message(agent_id, workspace_id, content, message_id, options)
                            .await
                    }
                }
                None => {
                    // Read-only fallback (no `agent_manager` wired): the
                    // router has already extracted `messageMetadata`, but
                    // the store-only op doesn't accept metadata since none
                    // of its internal callers carry it; the payload is dropped
                    // on this fallback path (metadata IS preserved on the
                    // production `AgentManager::send_message` path above).
                    // STAB-7: image_blocks and file_blocks ARE forwarded now.
                    let _ = message_metadata;
                    self.agent_send_message_op(
                        agent_id,
                        content,
                        message_id,
                        image_blocks,
                        file_blocks,
                    )
                    .await
                }
            }
        })
    }

    fn agent_force_message(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        message_id: String,
        content: String,
        image_blocks: Option<serde_json::Value>,
        file_blocks: Option<serde_json::Value>,
        note_ids: Option<serde_json::Value>,
        stdin_context: Option<String>,
        context_references: Option<serde_json::Value>,
        message_metadata: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let options = crate::agent_manager::TurnOptions {
                stdin_context,
                note_ids,
                context_references,
                image_blocks,
                file_blocks,
                message_metadata: message_metadata.clone(),
            };
            match self.agent_manager() {
                Some(manager) => {
                    manager
                        .force_message(agent_id, workspace_id, message_id, content, options)
                        .await
                }
                None => {
                    // See `agent_send_message` above: metadata is dropped
                    // on the read-only fallback and preserved on the
                    // production `AgentManager::force_message` path.
                    let _ = message_metadata;
                    self.agent_force_message_op(agent_id, message_id, content)
                        .await
                }
            }
        })
    }

    fn agent_queue_message(
        &self,
        agent_id: AgentId,
        content: String,
        image_blocks: Option<serde_json::Value>,
        file_blocks: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_queue_message_op(agent_id, content, image_blocks, file_blocks)
                .await
        })
    }

    fn agent_edit_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
        editing: Option<bool>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_edit_queued_message_op(agent_id, message_id, content, editing)
                .await
        })
    }

    fn agent_remove_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_remove_queued_message_op(agent_id, message_id)
                .await
        })
    }

    fn agent_get_queue(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_get_queue_op(agent_id, workspace_id).await })
    }

    fn agent_stop(&self, agent_id: AgentId) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // Interrupt the in-flight turn while KEEPING the child alive (TS
            // `agent.stop` keep-alive: `provider.interrupt()`), emitting the
            // terminal `agent:stream:end`; falls back to a hard kill only when no
            // live session can be interrupted. `{ success: true }` either way (§5.5).
            if let Some(manager) = self.agent_manager() {
                manager.interrupt(&agent_id).await;
            }
            Ok(serde_json::json!({ "success": true }))
        })
    }

    fn agent_retry(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            if let Some(manager) = self.agent_manager() {
                manager.agent_retry(agent_id, workspace_id).await
            } else {
                Ok(serde_json::json!({ "ok": false }))
            }
        })
    }

    fn agent_set_model(
        &self,
        _workspace_id: WorkspaceId,
        agent_id: AgentId,
        model_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_set_model_op(agent_id, model_id).await })
    }

    fn agent_get_models(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_get_models_op().await })
    }

    fn models_list(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.models_list_op().await })
    }

    fn agent_enhance_prompt(
        &self,
        prompt: String,
        mode: String,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_enhance_prompt_op(prompt, mode, model, workspace_id, timeout_ms)
                .await
        })
    }

    fn agent_complete_once(
        &self,
        prompt: String,
        system_prompt: Option<String>,
        model: Option<String>,
        workspace_id: Option<WorkspaceId>,
        timeout_ms: Option<u64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_complete_once_op(prompt, system_prompt, model, workspace_id, timeout_ms)
                .await
        })
    }

    fn agent_respond_permission(
        &self,
        request_id: String,
        outcome: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // Parse the §8 wire `outcome` shape before touching the registry so a
            // malformed body is rejected as invalid params, not silently dropped.
            let parsed = PermissionOutcome::from_wire(&outcome).ok_or_else(|| {
                Error::InvalidParams(
                    "agent.respondPermission: malformed `outcome` (expected \
                     { outcome: \"selected\", optionId } or { outcome: \"cancelled\" })"
                        .to_string(),
                )
            })?;
            // Without a runtime manager there is no registry to answer against, so
            // every request id is unresolved.
            let resolved = match self.agent_manager() {
                Some(manager) => manager.respond_permission(&request_id, parsed),
                None => false,
            };
            Ok(serde_json::json!({ "resolved": resolved }))
        })
    }

    fn agent_pending_permissions(
        &self,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // No manager ⇒ no outstanding prompts. Otherwise snapshot the registry
            // and, when an `agentId` filter is given, keep only that session's
            // prompts (`session_id` == intentd `agentId`, PROTOCOL §8).
            let mut requests = match self.agent_manager() {
                Some(manager) => manager.pending_permissions(),
                None => Vec::new(),
            };
            if let Some(agent_id) = agent_id {
                let filter = agent_id.as_str();
                requests.retain(|r| r.session_id == filter);
            }
            Ok(serde_json::json!({ "requests": requests }))
        })
    }

    fn agent_rename(
        &self,
        agent_id: AgentId,
        name: String,
        skip_if_explicitly_set: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_rename_op(agent_id, name, skip_if_explicitly_set)
                .await
        })
    }

    fn agent_delete(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_delete_op(agent_id, workspace_id).await })
    }

    fn agent_wake_or_create(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        context_message: String,
        input: intent_core::AgentWakeOrCreateInput,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_wake_or_create_op(workspace_id, task_note_id, context_message, input)
                .await
        })
    }

    fn agent_get_session_stats(
        &self,
        session_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_get_session_stats_op(session_id, workspace_id)
                .await
        })
    }

    fn agent_summary(
        &self,
        _workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_summary_op(agent_id).await })
    }

    fn agent_report_to_parent(
        &self,
        workspace_id: WorkspaceId,
        report: serde_json::Value,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_report_to_parent_op(workspace_id, report, caller_agent_id)
                .await
        })
    }

    fn agent_get_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_get_subscriptions_op(workspace_id, agent_id)
                .await
        })
    }

    fn agent_watch_completion(
        &self,
        workspace_id: WorkspaceId,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_watch_completion_op(workspace_id, parent_agent_id, child_agent_id)
                .await
        })
    }

    fn agent_watch_completion_for_sender(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_watch_completion_for_sender_op(
                workspace_id,
                caller_agent_id,
                target_agent_id,
            )
            .await
        })
    }

    fn agent_diagnostics(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        task_note_id: Option<NoteId>,
        stale_responding_after_ms: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_diagnostics_op(
                workspace_id,
                agent_id,
                task_note_id,
                stale_responding_after_ms,
            )
            .await
        })
    }

    fn agent_cancel_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_cancel_subscriptions_op(workspace_id, agent_id)
                .await
        })
    }

    fn agent_subscribe(
        &self,
        _workspace_id: WorkspaceId,
        event_types: Vec<String>,
        _exclude_self: Option<bool>,
        _batch_window: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let subs = self.event_subscriptions.clone();
        Box::pin(async move {
            let resolved = events::resolve_event_types(&event_types);
            let subscription_id = uuid::Uuid::new_v4().to_string();
            subs.lock()
                .expect("event subscription registry poisoned")
                .insert(subscription_id.clone());
            Ok(serde_json::json!({
                "subscriptionId": subscription_id,
                "eventTypes": resolved,
            }))
        })
    }

    fn agent_unsubscribe(
        &self,
        _workspace_id: WorkspaceId,
        subscription_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let subs = self.event_subscriptions.clone();
        Box::pin(async move {
            let removed = subs
                .lock()
                .expect("event subscription registry poisoned")
                .remove(&subscription_id);
            if !removed {
                return Err(Error::Internal("Subscription not found".to_string()));
            }
            Ok(serde_json::json!({ "success": true, "subscriptionId": subscription_id }))
        })
    }

    // ========================================================================
    // sandbox.* surface (PROTOCOL §5.34). Manual escape-hatch RPCs for
    // triggering merge-back or discarding a sandbox when auto-merge fails.
    // ========================================================================

    fn sandbox_merge(
        &self,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let event_bus = self.event_bus.clone();
        Box::pin(async move {
            use crate::sandbox_ops::{merge_sandbox, MergeOutcome};
            use intent_store::SandboxStatus;

            // Attempt merge
            let outcome = merge_sandbox(&store, &workspace_id, &sandbox_id).await?;

            match outcome {
                MergeOutcome::Merged {
                    commit_range,
                    canonical_head,
                } => {
                    // Mark merged
                    let _ = store
                        .update_sandbox_status(
                            &workspace_id,
                            &sandbox_id,
                            SandboxStatus::Merged,
                            &now_iso(),
                        )
                        .await;

                    // Discard sandbox
                    let _ = crate::sandbox_ops::discard_sandbox(&store, &workspace_id, &sandbox_id)
                        .await;

                    // Emit event
                    let event = NewEvent {
                        workspace_id: workspace_id.clone(),
                        timestamp: now_iso(),
                        event_type: "sandbox:merged".to_string(),
                        actor: intent_core::EventActor {
                            actor_type: ActorType::System,
                            id: Some("intentd".to_string()),
                            name: Some("intentd".to_string()),
                            ..Default::default()
                        },
                        session_id: Some(sandbox_id.0.clone()),
                        correlation_id: None,
                        parent_event_id: None,
                        metadata: None,
                        data: serde_json::json!({
                            "workspaceId": workspace_id.0,
                            "agentId": sandbox_id.0,
                            "commitRange": commit_range,
                            "canonicalHead": canonical_head,
                        }),
                    };
                    crate::publish_event(&event_bus, event).await;

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

    fn sandbox_discard(
        &self,
        workspace_id: WorkspaceId,
        sandbox_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            crate::sandbox_ops::discard_sandbox(&store, &workspace_id, &sandbox_id).await?;
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    // ========================================================================
    // pr.* read surface (PROTOCOL §5.7). Maps onto the host-agnostic
    // `SourceControl` trait (§7.5); every method requires an active PR on the
    // workspace (else `-32603`). Pure mapping/aggregation lives in `pr_ops`.
    // ========================================================================

    fn pr_status(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let pr = sc
                .get_pr(&repo_ref, number)
                .await
                .map_err(pr_ops::map_sc_err)?;
            let state = pr_ops::derive_status_state(&pr);
            let mergeable_state = pr
                .mergeable_state
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            let summary = pr_ops::build_status_summary(state, pr.mergeable, &mergeable_state);
            Ok(serde_json::json!({
                "prNumber": number,
                "title": pr.title,
                "url": pr.url,
                "state": state,
                "mergeable": pr.mergeable,
                "mergeableState": mergeable_state,
                "hasConflicts": mergeable_state == "dirty",
                "isDraft": state == "draft",
                "isMerged": state == "merged",
                "isClosed": state == "closed",
                "summary": summary,
            }))
        })
    }

    fn pr_list_comments(
        &self,
        workspace_id: WorkspaceId,
        count: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let comments = sc
                .list_comments(&repo_ref, number)
                .await
                .map_err(pr_ops::map_sc_err)?;
            let limit = pr_ops::clamp_count(count);
            let comments: Vec<_> = comments.into_iter().take(limit).collect();
            Ok(serde_json::json!({ "count": comments.len(), "comments": comments }))
        })
    }

    fn pr_list_review_comments(
        &self,
        workspace_id: WorkspaceId,
        path: Option<String>,
        status: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let status = pr_ops::validate_review_comment_status(status)?;
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            match sc
                .get_review_threads(
                    &repo_ref,
                    number,
                    intent_sourcecontrol::PageParams::first(100),
                )
                .await
            {
                Ok(page) => {
                    let mut threads = page.items;
                    let total = threads.len() as i64;
                    if status == "resolved" {
                        threads.retain(|t| t.is_resolved);
                    } else if status == "unresolved" {
                        threads.retain(|t| !t.is_resolved);
                    }
                    if let Some(p) = &path {
                        pr_ops::retain_path(&mut threads, p);
                    }
                    let json_threads = pr_ops::thread_list_json(&threads);
                    Ok(serde_json::json!({
                        "threads": json_threads,
                        "threadCount": threads.len(),
                        "usingFallback": false,
                        "pagination": { "totalCount": total, "pagesFetched": 1, "hasMore": false },
                        "filter": { "path": path, "status": status },
                        "note": serde_json::Value::Null,
                    }))
                }
                Err(_) => {
                    let comments = sc
                        .list_review_comments(
                            &repo_ref,
                            number,
                            intent_sourcecontrol::PageParams::first(100),
                        )
                        .await
                        .map_err(pr_ops::map_sc_err)?
                        .items;
                    let total_fetched = comments.len() as i64;
                    let mut threads = pr_ops::fallback_threads(comments);
                    if let Some(p) = &path {
                        pr_ops::retain_path(&mut threads, p);
                    }
                    let json_threads = pr_ops::thread_list_json(&threads);
                    let note = if status != "all" {
                        serde_json::Value::String(
                            "Resolved status is unavailable with REST fallback; returning all \
                             threads regardless of the status filter."
                                .to_string(),
                        )
                    } else {
                        serde_json::Value::Null
                    };
                    Ok(serde_json::json!({
                        "threads": json_threads,
                        "threadCount": threads.len(),
                        "usingFallback": true,
                        "pagination": { "totalFetched": total_fetched, "pagesFetched": 1, "hasMore": false },
                        "filter": { "path": path, "status": status },
                        "note": note,
                    }))
                }
            }
        })
    }

    fn pr_get_reviews(
        &self,
        workspace_id: WorkspaceId,
        pr_number: Option<u64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = match pr_number {
                Some(n) => n,
                None => pr_ops::active_pr_number(&ws)?,
            };
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let reviews = sc
                .list_reviews(&repo_ref, number)
                .await
                .map_err(pr_ops::map_sc_err)?;
            let agg = pr_ops::aggregate_reviews(&reviews);
            Ok(serde_json::json!({
                "reviewDecision": agg.review_decision,
                "approvalCount": agg.approval_count,
                "changesRequestedCount": agg.changes_requested_count,
                "approvedBy": agg.approved_by,
                "reviews": reviews,
            }))
        })
    }

    fn pr_list_check_runs(
        &self,
        workspace_id: WorkspaceId,
        git_ref: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let git_ref = match git_ref {
                Some(r) => r,
                None => {
                    let pr = sc
                        .get_pr(&repo_ref, number)
                        .await
                        .map_err(pr_ops::map_sc_err)?;
                    pr.head_sha
                        .filter(|s| !s.is_empty())
                        .or_else(|| Some(pr.source_branch).filter(|s| !s.is_empty()))
                        .ok_or_else(|| {
                            Error::Internal("Could not determine PR head commit".to_string())
                        })?
                }
            };
            let runs = sc
                .check_runs(&repo_ref, &git_ref)
                .await
                .map_err(pr_ops::map_sc_err)?;
            let s = pr_ops::summarize_check_runs(&runs);
            Ok(serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "pending": s.pending,
                "runs": runs,
            }))
        })
    }

    // ------------------------------------------------------------------------
    // pr.* write/action surface (PROTOCOL §5.7). Same active-PR enforcement as
    // the read methods; validation/poll glue lives in `pr_ops`.
    // ------------------------------------------------------------------------

    fn pr_merge(
        &self,
        workspace_id: WorkspaceId,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws_scope = workspace_id.0.clone();
            let op_store = store.clone();
            with_idempotency(
                &store,
                &ws_scope,
                idempotency_key,
                "pr.merge",
                move || async move {
                    let store = op_store;
                    let method = pr_ops::validate_merge_method(merge_method)?;
                    let ws = load_ws_for_pr(&store, &workspace_id).await?;
                    let (owner, repo) = pr_ops::repo_of(&ws)?;
                    let number = pr_ops::active_pr_number(&ws)?;
                    let sc = pr_ops::resolve_source_control(injected).await?;
                    let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
                    let pr = sc
                        .get_pr(&repo_ref, number)
                        .await
                        .map_err(pr_ops::map_sc_err)?;
                    let state = pr_ops::derive_status_state(&pr);
                    if state == "draft" {
                        return Err(Error::Internal(format!(
                    "PR #{number} is a draft and cannot be merged. GitHub blocks merging draft \
                     PRs. Mark the PR as \"Ready for review\" first using the GitHub UI or API."
                )));
                    }
                    if state != "open" {
                        return Err(Error::Internal(format!(
                            "PR #{number} is {state} and cannot be merged."
                        )));
                    }
                    if pr.mergeable == Some(false) {
                        return Err(Error::Internal(format!(
                    "PR #{number} is not mergeable. This could be due to merge conflicts, failing \
                     required checks, or missing required reviews. Please resolve the issues \
                     before attempting to merge."
                )));
                    }
                    let outcome = sc
                        .merge_pr(
                            &repo_ref,
                            number,
                            method,
                            intent_sourcecontrol::MergeOptions {
                                commit_title,
                                commit_message,
                            },
                        )
                        .await
                        .map_err(pr_ops::map_sc_err)?;
                    if !outcome.merged {
                        return Err(Error::Internal(format!(
                            "Failed to merge PR #{number}: {}",
                            outcome.message
                        )));
                    }
                    Ok(serde_json::json!({
                        "merged": true,
                        "sha": outcome.sha,
                        "mergeMethod": pr_ops::merge_method_word(method),
                        "message": outcome.message,
                        "prNumber": number,
                    }))
                },
            )
            .await
        })
    }

    fn pr_update_branch(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            match sc.update_branch(&repo_ref, number).await {
                // URL revisit (§7.6): the forge `update_branch` returns no URL,
                // so mirror the TS `result.url ?? null` by surfacing the PR URL
                // now persisted on the workspace (`null` when not yet linked).
                Ok(()) => Ok(serde_json::json!({
                    "method": "merge",
                    "alreadyUpToDate": false,
                    "message": "PR branch updated from the base branch.",
                    "url": ws.pr_url.clone(),
                })),
                Err(e) => {
                    let msg = e.to_string();
                    let lower = msg.to_lowercase();
                    if lower.contains("already up-to-date") || lower.contains("already up to date")
                    {
                        Ok(serde_json::json!({
                            "method": "merge",
                            "alreadyUpToDate": true,
                            "message": "PR branch is already up-to-date with the base branch.",
                            "url": serde_json::Value::Null,
                        }))
                    } else if lower.contains("merge conflict") {
                        Err(Error::Internal(format!(
                            "Cannot update PR branch: merge conflicts detected. The conflicts must \
                             be resolved manually.\n{msg}"
                        )))
                    } else {
                        Err(Error::Internal(format!(
                            "Failed to update PR branch: {msg}"
                        )))
                    }
                }
            }
        })
    }

    fn pr_post_comment(
        &self,
        workspace_id: WorkspaceId,
        body: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let comment = sc
                .add_comment(&repo_ref, number, &body, None)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({
                "id": comment.id,
                "htmlUrl": comment.url,
            }))
        })
    }

    fn pr_reply_to_review_comment(
        &self,
        workspace_id: WorkspaceId,
        comment_id: u64,
        body: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let reply = sc
                .reply_to_review_comment(&repo_ref, number, comment_id, &body)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({
                "id": reply.id,
                "htmlUrl": reply.url,
            }))
        })
    }

    fn pr_resolve_thread(
        &self,
        workspace_id: WorkspaceId,
        thread_id: String,
        action: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let action = pr_ops::validate_resolve_action(action)?;
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            // Active-PR enforcement (TS `requirePrContext`) even though the
            // forge call keys off the thread id alone.
            let _ = pr_ops::repo_of(&ws)?;
            let _ = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let success = if action == "unresolve" {
                sc.unresolve_thread(&thread_id).await
            } else {
                sc.resolve_thread(&thread_id).await
            }
            .map_err(pr_ops::map_sc_err)?;
            if !success {
                return Err(Error::Internal(format!(
                    "Failed to {action} thread. The operation may have failed silently."
                )));
            }
            Ok(serde_json::json!({
                "ok": true,
                "threadId": thread_id,
                "action": action,
            }))
        })
    }

    fn pr_create_review(
        &self,
        workspace_id: WorkspaceId,
        verdict: String,
        body: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let verdict = pr_ops::validate_review_verdict(&verdict)?;
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let review = sc
                .submit_review(&repo_ref, number, verdict, body)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({ "review": review }))
        })
    }

    fn pr_wait_for_changes(
        &self,
        workspace_id: WorkspaceId,
        timeout_seconds: Option<i64>,
        poll_interval_seconds: Option<i64>,
        watch: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let watch = pr_ops::validate_watch_mode(watch)?;
            let timeout = pr_ops::clamp_timeout(timeout_seconds);
            let poll_interval = pr_ops::clamp_poll_interval(poll_interval_seconds);
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner.clone(), repo.clone());

            let timeout_ms = timeout * 1000;
            let poll_ms = poll_interval * 1000;
            let safety_ms = pr_ops::SAFETY_PADDING_SECONDS * 1000;
            let effective_ms = timeout_ms.min(poll_ms.max(timeout_ms.saturating_sub(safety_ms)));

            let start = tokio::time::Instant::now();
            let initial = capture_pr_snapshot(&*sc, &repo_ref, number)
                .await
                .ok_or_else(|| {
                    Error::Internal(format!("Could not fetch PR #{number} for {owner}/{repo}."))
                })?;
            let mut baseline = initial.clone();
            let mut last = initial;
            let mut iterations: u64 = 0;

            loop {
                let elapsed_ms = start.elapsed().as_millis() as u64;
                if elapsed_ms >= effective_ms {
                    break;
                }
                iterations += 1;
                let remaining = effective_ms - elapsed_ms;
                tokio::time::sleep(std::time::Duration::from_millis(poll_ms.min(remaining))).await;
                let current = match capture_pr_snapshot(&*sc, &repo_ref, number).await {
                    Some(s) => s,
                    None => continue,
                };
                last = current.clone();
                if baseline.check_runs_fetch_failed && !current.check_runs_fetch_failed {
                    baseline.check_runs = current.check_runs.clone();
                    baseline.check_runs_fetch_failed = false;
                }
                let changes = pr_ops::detect_changes(&baseline, &current, &watch);
                if !changes.is_empty() {
                    let elapsed_s = ((start.elapsed().as_millis() as f64) / 1000.0).round() as u64;
                    let summary = pr_ops::format_change_summary(&changes, &current, elapsed_s);
                    return Ok(serde_json::json!({
                        "changed": true,
                        "changes": changes,
                        "elapsedSeconds": elapsed_s,
                        "iterations": iterations,
                        "snapshot": pr_ops::snapshot_json(&current),
                        "summary": summary,
                    }));
                }
            }

            let elapsed_s = ((start.elapsed().as_millis() as f64) / 1000.0).round() as u64;
            Ok(serde_json::json!({
                "changed": false,
                "elapsedSeconds": elapsed_s,
                "iterations": iterations,
                "snapshot": pr_ops::snapshot_json(&last),
                "summary": format!(
                    "⏱️ Timeout reached after {elapsed_s} seconds without detecting changes.\n\
                     Watched mode: {watch}\nPolls performed: {iterations}"
                ),
            }))
        })
    }

    // ------------------------------------------------------------------------
    // `github.*` explicit-addressing surface (PROTOCOL §5.27). Every method
    // takes `(owner, repo[, number])` directly (no workspace/active-PR
    // resolution) and reuses the `intent-sourcecontrol` engine that backs
    // `pr.*`; the source-control handle and forge-error mapping are shared with
    // `pr_ops`, while the GitHub-shaped wire DTOs are rendered by `github_ops`.
    // ------------------------------------------------------------------------

    fn github_pulls_create(
        &self,
        owner: String,
        repo: String,
        title: String,
        body: String,
        head: String,
        base: String,
        draft: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            // `head` is forwarded VERBATIM (no `owner:branch` login prefix) —
            // the engine sends `input.source_branch` as the raw `head`,
            // preserving the FE's same-repo-branch bypass (§5.27).
            let pr = sc
                .create_pr(
                    &repo_ref,
                    intent_sourcecontrol::NewPullRequest {
                        title,
                        body: Some(body),
                        source_branch: head,
                        target_branch: base,
                        draft,
                    },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({ "pull": github_ops::pull_to_json(&pr) }))
        })
    }

    fn github_pulls_get(
        &self,
        owner: String,
        repo: String,
        number: u64,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let pr = sc
                .get_pr(&repo_ref, number)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({ "pull": github_ops::pull_to_json(&pr) }))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn github_pulls_list(
        &self,
        owner: String,
        repo: String,
        state: Option<String>,
        head: Option<String>,
        base: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let state = github_ops::parse_pr_state(state.as_deref())?;
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let page = sc
                .list_prs(
                    &repo_ref,
                    intent_sourcecontrol::PrQuery {
                        state,
                        base,
                        head,
                        author: None,
                        involvement: None,
                        limit: Some(limit),
                        cursor,
                    },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            let pulls: Vec<_> = page.items.iter().map(github_ops::pull_to_json).collect();
            Ok(serde_json::json!({
                "pulls": pulls,
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    fn github_pulls_search(
        &self,
        owner: String,
        repo: String,
        filter: Option<String>,
        state: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let involvement = github_ops::parse_pr_involvement(filter.as_deref())?;
            // Search defaults to open PRs (FE `searchGitHubPullRequests`); a
            // `filter:"all"` carries no involvement constraint and so degrades
            // to the plain `github.pulls.list` listing the engine performs.
            let state = match state {
                Some(s) => github_ops::parse_pr_state(Some(s.as_str()))?,
                None => Some(intent_sourcecontrol::PrState::Open),
            };
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let page = sc
                .list_prs(
                    &repo_ref,
                    intent_sourcecontrol::PrQuery {
                        state,
                        base: None,
                        head: None,
                        author: None,
                        involvement,
                        limit: Some(limit),
                        cursor,
                    },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            let pulls: Vec<_> = page.items.iter().map(github_ops::pull_to_json).collect();
            Ok(serde_json::json!({
                "pulls": pulls,
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    fn github_pulls_merge(
        &self,
        owner: String,
        repo: String,
        number: u64,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let method = pr_ops::validate_merge_method(merge_method)?;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let outcome = sc
                .merge_pr(
                    &repo_ref,
                    number,
                    method,
                    intent_sourcecontrol::MergeOptions {
                        commit_title,
                        commit_message,
                    },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({
                "merged": outcome.merged,
                "message": outcome.message,
                "sha": outcome.sha,
            }))
        })
    }

    // ========================================================================
    // github.* browse / auth / identity (PROTOCOL §5.27)
    //
    // These reuse the same `SourceControl` engine as `pr.*` (via
    // `pr_ops::resolve_source_control` / `map_sc_err`) but address GitHub
    // directly by params, with no workspace/active-PR scoping. The PAT is read
    // once by the engine at build time and is NEVER logged or returned here.
    // ========================================================================

    fn github_repos_list(
        &self,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let page = sc
                .list_repos(intent_sourcecontrol::PageParams { limit, cursor })
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({
                "repos": github_browse_ops::repos_to_wire(&page.items),
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    fn github_pulls_update_branch(
        &self,
        owner: String,
        repo: String,
        number: u64,
        expected_head_sha: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            // `expectedHeadSha` is accepted for FE shape parity; the engine
            // `update_branch` does not take a race guard, so it is unused.
            let _ = expected_head_sha;
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            sc.update_branch(&repo_ref, number)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({
                "message": "PR branch updated from the base branch.",
                "url": serde_json::Value::Null,
            }))
        })
    }

    fn github_repos_search(
        &self,
        query: String,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let page = sc
                .search_repos(&query, intent_sourcecontrol::PageParams { limit, cursor })
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({
                "repos": github_browse_ops::repos_to_wire(&page.items),
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn github_issues_list(
        &self,
        owner: String,
        repo: String,
        state: Option<String>,
        labels: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let state = github_ops::parse_issue_state(state.as_deref())?;
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let page = sc
                .list_issues(
                    &repo_ref,
                    intent_sourcecontrol::IssueQuery {
                        state: Some(state),
                        labels,
                        limit: Some(limit),
                        cursor,
                    },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            let items: Vec<_> = page
                .items
                .iter()
                .map(|i| github_ops::issue_to_json(i, &repo_ref.owner, &repo_ref.name))
                .collect();
            Ok(serde_json::json!({
                "issues": items,
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn github_issues_search(
        &self,
        owner: String,
        repo: String,
        filter: Option<String>,
        state: Option<String>,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            // Validate `filter` against the FE value set. The host-agnostic
            // engine has no `/search/issues` capability for issues, so `@me`
            // involvement cannot be expressed; the search degrades to the
            // engine's repo-issue listing filtered by state (v1 limitation —
            // full involvement search needs an engine `search_issues` method).
            let _ = github_ops::parse_pr_involvement(filter.as_deref())?;
            let state = match state {
                Some(s) => github_ops::parse_issue_state(Some(s.as_str()))?,
                None => "open".to_string(),
            };
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let page = sc
                .list_issues(
                    &repo_ref,
                    intent_sourcecontrol::IssueQuery {
                        state: Some(state),
                        labels: None,
                        limit: Some(limit),
                        cursor,
                    },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            let items: Vec<_> = page
                .items
                .iter()
                .map(|i| github_ops::issue_to_json(i, &repo_ref.owner, &repo_ref.name))
                .collect();
            Ok(serde_json::json!({
                "issues": items,
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    fn github_list_review_comments(
        &self,
        owner: String,
        repo: String,
        number: u64,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let page = sc
                .list_review_comments(
                    &repo_ref,
                    number,
                    intent_sourcecontrol::PageParams { limit, cursor },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            let items: Vec<_> = page
                .items
                .iter()
                .map(github_ops::review_comment_to_json)
                .collect();
            Ok(serde_json::json!({
                "comments": items,
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    fn github_reply_review_comment(
        &self,
        owner: String,
        repo: String,
        number: u64,
        comment_id: u64,
        body: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let rc = sc
                .reply_to_review_comment(&repo_ref, number, comment_id, &body)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({ "comment": github_ops::review_comment_to_json(&rc) }))
        })
    }

    fn github_get_review_threads(
        &self,
        owner: String,
        repo: String,
        number: u64,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            let page = sc
                .get_review_threads(
                    &repo_ref,
                    number,
                    intent_sourcecontrol::PageParams { limit, cursor },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            let items: Vec<_> = page
                .items
                .iter()
                .map(github_ops::review_thread_to_json)
                .collect();
            Ok(serde_json::json!({
                "threads": items,
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    fn github_resolve_thread(&self, thread_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let sc = pr_ops::resolve_source_control(injected).await?;
            let is_resolved = sc
                .resolve_thread(&thread_id)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({ "isResolved": is_resolved }))
        })
    }

    fn github_unresolve_thread(
        &self,
        thread_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let sc = pr_ops::resolve_source_control(injected).await?;
            let is_resolved = sc
                .unresolve_thread(&thread_id)
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({ "isResolved": is_resolved }))
        })
    }

    fn github_repos_get(
        &self,
        owner: String,
        repo: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let sc = pr_ops::resolve_source_control(injected).await?;
            match sc.get_repo(&owner, &repo).await {
                Ok(r) => Ok(serde_json::json!({ "repo": github_browse_ops::repo_to_wire(&r) })),
                // FE `getGitHubRepo` returns `GithubRepo | null`; a missing repo
                // surfaces as `{ repo: null }` rather than an error (§5.27).
                Err(intent_sourcecontrol::Error::NotFound(_)) => {
                    Ok(serde_json::json!({ "repo": serde_json::Value::Null }))
                }
                Err(e) => Err(pr_ops::map_sc_err(e)),
            }
        })
    }

    fn github_branches_list(
        &self,
        owner: String,
        repo: String,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let limit = github_ops::clamp_limit(limit);
            let cursor = github_ops::decode_next_token(next_token.as_deref());
            let sc = pr_ops::resolve_source_control(injected).await?;
            let page = sc
                .list_remote_branches(
                    &owner,
                    &repo,
                    intent_sourcecontrol::PageParams { limit, cursor },
                )
                .await
                .map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({
                "branches": github_browse_ops::branch_names(&page.items),
                "nextToken": github_ops::next_token_value(page.next_cursor.as_deref()),
            }))
        })
    }

    fn github_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            // A missing/invalid token is the graceful "not configured" state,
            // NOT an error: report `isConfigured: false` instead of throwing.
            let is_configured = match pr_ops::resolve_source_control(injected).await {
                Ok(sc) => sc
                    .check_auth()
                    .await
                    .map(|s| s.authenticated)
                    .unwrap_or(false),
                Err(_) => false,
            };
            Ok(github_browse_ops::auth_status_to_wire(is_configured))
        })
    }

    fn github_connect(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        // No-op in the PAT-from-env model: nothing to connect (no OAuth/device
        // flow). Return guidance so the FE button can explain the setup.
        Box::pin(async {
            Ok(serde_json::json!({
                "ok": false,
                "guidance": github_browse_ops::CONNECT_GUIDANCE,
            }))
        })
    }

    fn github_revoke(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        // No-op: the token is environment-owned, so there is nothing to revoke.
        Box::pin(async {
            Ok(serde_json::json!({
                "ok": false,
                "guidance": github_browse_ops::REVOKE_GUIDANCE,
            }))
        })
    }

    fn github_get_user(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.source_control.clone();
        Box::pin(async move {
            let sc = pr_ops::resolve_source_control(injected).await?;
            let user = sc.get_user().await.map_err(pr_ops::map_sc_err)?;
            Ok(serde_json::json!({ "user": github_browse_ops::user_to_wire(&user) }))
        })
    }

    // ========================================================================
    // linear.* read surface (PROTOCOL §5.28). Maps onto the `LinearEngine`
    // trait; the engine resolves the API key (`LINEAR_API_KEY` / keychain) and
    // talks to Linear's GraphQL API. A missing/invalid key → `Internal`
    // ("not configured", graceful). Validation/resolution glue lives in
    // `linear_ops`. The API key is never logged or returned over the wire.
    // ========================================================================

    fn linear_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let status = engine
                .auth_status()
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(status)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_list_issues(
        &self,
        filter: Option<String>,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let filter = linear_ops::parse_filter(filter)?;
            let engine = linear_ops::resolve_engine(injected).await?;
            let issues = engine
                .list_issues(filter, linear_ops::wire_limit(limit))
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(issues)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_search_issues(
        &self,
        query: String,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let issues = engine
                .search_issues(&query, linear_ops::wire_limit(limit))
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(issues)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_get_issue(
        &self,
        id_or_identifier: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let issue = engine
                .get_issue(&id_or_identifier)
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(issue)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_viewer(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let user = engine.viewer().await.map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(user)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_list_teams(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let teams = engine
                .list_teams(linear_ops::wire_limit(limit))
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(teams)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_list_workflow_states(
        &self,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let states = engine
                .list_workflow_states(linear_ops::wire_limit(limit))
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(states)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_list_projects(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let projects = engine
                .list_projects(linear_ops::wire_limit(limit))
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(projects)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_list_labels(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let engine = linear_ops::resolve_engine(injected).await?;
            let labels = engine
                .list_labels(linear_ops::wire_limit(limit))
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(labels)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_create_issue(
        &self,
        request: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let req = linear_ops::parse_create_issue(request)?;
            let engine = linear_ops::resolve_engine(injected).await?;
            let issue = engine
                .create_issue(req)
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(issue)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn linear_update_issue(
        &self,
        request: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.linear_engine.clone();
        Box::pin(async move {
            let req = linear_ops::parse_update_issue(request)?;
            let engine = linear_ops::resolve_engine(injected).await?;
            let issue = engine
                .update_issue(req)
                .await
                .map_err(linear_ops::map_linear_err)?;
            serde_json::to_value(issue)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    // ========================================================================
    // sentry.* read surface (PROTOCOL §5.29). Maps onto the `SentryEngine`
    // trait; the engine resolves the credential pair (org + token from
    // `SENTRY_ORG` / `SENTRY_API_TOKEN` / keychain) and talks to Sentry's REST
    // API. A missing/invalid pair → `Internal` ("not configured", graceful).
    // Validation/resolution glue lives in `sentry_ops`. The token is never
    // logged or returned over the wire.
    // ========================================================================

    fn sentry_auth_status(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let engine = sentry_ops::resolve_engine(injected).await?;
            let status = engine
                .auth_status()
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(status)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn sentry_list_issues(
        &self,
        project: Option<String>,
        status: Option<String>,
        query: Option<String>,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let status = sentry_ops::parse_status(status)?;
            let engine = sentry_ops::resolve_engine(injected).await?;
            let request = intent_sentry::FetchIssuesRequest {
                project,
                status,
                query,
                limit: sentry_ops::wire_limit(limit),
            };
            let issues = engine
                .list_issues(request)
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(issues)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn sentry_search_issues(
        &self,
        query: String,
        project: Option<String>,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let engine = sentry_ops::resolve_engine(injected).await?;
            let issues = engine
                .search_issues(&query, project.as_deref(), sentry_ops::wire_limit(limit))
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(issues)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn sentry_list_projects(&self, limit: Option<i64>) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let engine = sentry_ops::resolve_engine(injected).await?;
            let projects = engine
                .list_projects(sentry_ops::wire_limit(limit))
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(projects)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn sentry_get_issue(&self, id_or_short_id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let engine = sentry_ops::resolve_engine(injected).await?;
            let issue = engine
                .get_issue(&id_or_short_id)
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(issue)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn sentry_resolve_issue(&self, id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let engine = sentry_ops::resolve_engine(injected).await?;
            let issue = engine
                .resolve_issue(&id)
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(issue)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn sentry_ignore_issue(&self, id: String) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let engine = sentry_ops::resolve_engine(injected).await?;
            let issue = engine
                .ignore_issue(&id)
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(issue)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn sentry_assign_issue(
        &self,
        id: String,
        assigned_to: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let injected = self.sentry_engine.clone();
        Box::pin(async move {
            let engine = sentry_ops::resolve_engine(injected).await?;
            let issue = engine
                .assign_issue(&id, assigned_to.as_deref())
                .await
                .map_err(sentry_ops::map_sentry_err)?;
            serde_json::to_value(issue)
                .map_err(|e| Error::Internal(format!("serialize result failed: {e}")))
        })
    }

    fn file_tracking_init(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        // Tracking is attached at workspace-open / agent-edit time (§17.1); the
        // wire init is a no-op acknowledgement, matching the TS handler.
        let _ = workspace_id;
        Box::pin(async { Ok(serde_json::json!({ "ok": true })) })
    }

    fn file_tracking_sync(
        &self,
        workspace_id: WorkspaceId,
        force: bool,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // `force` is accepted for wire parity; reconciliation is idempotent.
            let _ = force;
            let synced = serde_json::json!({ "success": true, "synced": true });
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(_) => return Ok(synced),
            };
            if ws.is_remote {
                return Ok(synced);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(synced);
            };
            if !worktree.join(".git").exists() {
                return Ok(synced);
            }
            // Preserve existing attribution per path while reconciling the
            // unstaged worktree changes against live git (§17.4).
            let existing = store
                .list_tracked_changes(&workspace_id)
                .await
                .unwrap_or_default();
            let mut attribution: AttributionByPath = HashMap::new();
            for row in &existing {
                attribution.insert(
                    row.path.clone(),
                    (row.agent_id.clone(), row.session_id.clone(), row.turn),
                );
            }
            let files = intent_git::diff::diff_index_to_workdir(&worktree)?;
            for fd in files {
                let summary = crate::diffs::compute_and_store(
                    &store,
                    &worktree,
                    &workspace_id,
                    &fd.path,
                    false,
                )
                .await
                .ok()
                .flatten();
                let status = if fd.old_blob.is_none() {
                    "added"
                } else if fd.new_blob.is_none() {
                    "deleted"
                } else {
                    "modified"
                };
                let (agent_id, session_id, turn) = attribution
                    .get(&fd.path)
                    .cloned()
                    .unwrap_or((None, None, None));
                let change = intent_store::NewTrackedChange {
                    workspace_id: workspace_id.clone(),
                    path: fd.path.clone(),
                    stage: "unstaged".to_string(),
                    status: status.to_string(),
                    agent_id,
                    session_id,
                    turn,
                    commit_hash: None,
                    old_blob_sha: summary.as_ref().and_then(|s| s.old_blob_sha.clone()),
                    new_blob_sha: summary.as_ref().and_then(|s| s.new_blob_sha.clone()),
                    additions: summary.as_ref().map(|s| s.additions).unwrap_or(0),
                    deletions: summary.as_ref().map(|s| s.deletions).unwrap_or(0),
                };
                crate::file_tracking::track_change(&store, change).await?;
            }
            Ok(synced)
        })
    }

    fn file_tracking_load(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let worktree = ft_worktree(&store, &workspace_id).await;
            let rows = match store.list_tracked_changes(&workspace_id).await {
                Ok(r) => r,
                Err(_) => return Ok(empty_changes_result()),
            };
            Ok(file_tracking_ops::build_changes_result(
                rows,
                worktree.as_deref(),
                &file_tracking_ops::ChangeFilterParsed::default(),
            ))
        })
    }

    fn file_tracking_get_changes(
        &self,
        workspace_id: WorkspaceId,
        filter: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let parsed = file_tracking_ops::parse_filter(filter.as_ref());
            let worktree = ft_worktree(&store, &workspace_id).await;
            let rows = match store.list_tracked_changes(&workspace_id).await {
                Ok(r) => r,
                Err(_) => return Ok(empty_changes_result()),
            };
            Ok(file_tracking_ops::build_changes_result(
                rows,
                worktree.as_deref(),
                &parsed,
            ))
        })
    }

    fn file_tracking_load_commits(
        &self,
        workspace_id: WorkspaceId,
        limit: Option<i64>,
        page_token: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // TA-2 / §5.5: clamp the page size to [1,200] (default 50) and walk
            // backward through the (newest-first) first-parent history via an
            // opaque skip token. `nextToken` is additive to the existing object.
            let limit = pagination::clamp_limit(limit);
            let skip = pagination::parse_offset(page_token.as_deref());
            let empty = serde_json::json!({ "commits": [], "nextToken": serde_json::Value::Null });
            let ws = match store.get_workspace(&workspace_id).await {
                Ok(w) => w,
                Err(_) => return Ok(empty),
            };
            if ws.is_remote {
                return Ok(empty);
            }
            let Some(worktree) = git_ops::worktree_path(&ws) else {
                return Ok(empty);
            };
            if !worktree.join(".git").exists() {
                return Ok(empty);
            }
            // Fetch one past the page window to decide whether older commits remain.
            let commits = intent_git::history::history(&worktree, skip + limit + 1)?;
            let has_more = commits.len() > skip + limit;
            let values: Vec<serde_json::Value> = commits
                .iter()
                .skip(skip)
                .take(limit)
                .map(file_tracking_ops::commit_to_value)
                .collect();
            let next_token = if has_more {
                serde_json::Value::String(pagination::offset_token(skip + limit))
            } else {
                serde_json::Value::Null
            };
            Ok(serde_json::json!({ "commits": values, "nextToken": next_token }))
        })
    }

    fn file_tracking_get_line_stats(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let rows = store
                .list_tracked_changes(&workspace_id)
                .await
                .unwrap_or_default();
            let mut additions = 0i64;
            let mut deletions = 0i64;
            for row in &rows {
                additions += row.additions;
                deletions += row.deletions;
            }
            Ok(serde_json::json!({ "additions": additions, "deletions": deletions }))
        })
    }

    fn file_tracking_stage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let path_list = file_tracking_ops::parse_paths(&paths)?;
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to stage files: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to stage files: workspace has no worktree".to_string())
            })?;
            intent_git::stage::stage(&worktree, &path_list)?;
            // Preserve attribution: move each file's audit row unstaged → staged.
            for raw in &path_list {
                let rel = file_tracking_ops::worktree_relative(&worktree, raw);
                let key = crate::file_tracking::normalize_path(&rel);
                store
                    .set_tracked_change_stage(&workspace_id, &key, "unstaged", "staged")
                    .await?;
            }
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    fn file_tracking_unstage(
        &self,
        workspace_id: WorkspaceId,
        paths: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let path_list = file_tracking_ops::parse_paths(&paths)?;
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to unstage files: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to unstage files: workspace has no worktree".to_string())
            })?;
            intent_git::stage::unstage(&worktree, &path_list)?;
            // Preserve attribution: move each file's audit row staged → unstaged.
            for raw in &path_list {
                let rel = file_tracking_ops::worktree_relative(&worktree, raw);
                let key = crate::file_tracking::normalize_path(&rel);
                store
                    .set_tracked_change_stage(&workspace_id, &key, "staged", "unstaged")
                    .await?;
            }
            Ok(serde_json::json!({ "ok": true }))
        })
    }

    fn metrics_get_workspace_stats(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let Some(ws) = store.get_workspace_metrics(&workspace_id).await? else {
                return Ok(serde_json::Value::Null);
            };
            let agents = store
                .list_agent_metrics_for_workspace(&workspace_id)
                .await?;
            Ok(crate::metrics::workspace_metrics_value(&ws, &agents))
        })
    }

    fn metrics_get_agent_stats(
        &self,
        agent_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let rows = store.list_agent_metrics(&agent_id).await?;
            Ok(crate::metrics::agent_metrics_value(&rows))
        })
    }

    fn metrics_get_all_workspace_stats(&self) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            let workspaces = store.list_workspace_metrics().await?;
            let mut out = serde_json::Map::new();
            for ws in &workspaces {
                let agents = store
                    .list_agent_metrics_for_workspace(&ws.workspace_id)
                    .await?;
                out.insert(
                    ws.workspace_id.0.clone(),
                    crate::metrics::workspace_metrics_value(ws, &agents),
                );
            }
            Ok(serde_json::Value::Object(out))
        })
    }

    fn metrics_clear_agent_stats(
        &self,
        agent_id: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            store.delete_agent_metrics(&agent_id).await?;
            Ok(serde_json::json!({ "success": true }))
        })
    }

    // ========================================================================
    // accept-changes.* — commit→push→PR→merge orchestration (PROTOCOL §5.18).
    // ========================================================================

    fn accept_changes_get_status(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let svc = self.clone();
        Box::pin(async move { svc.ac_get_status(workspace_id).await })
    }

    fn accept_changes_prepare(
        &self,
        workspace_id: WorkspaceId,
        action: String,
        files: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let svc = self.clone();
        Box::pin(async move { svc.ac_prepare(workspace_id, action, files).await })
    }

    fn accept_changes_execute(
        &self,
        workspace_id: WorkspaceId,
        params: serde_json::Value,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let svc = self.clone();
        Box::pin(async move { svc.ac_execute(workspace_id, params).await })
    }

    fn accept_changes_merge_pr(
        &self,
        workspace_id: WorkspaceId,
        pr_number: u64,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let svc = self.clone();
        Box::pin(async move {
            svc.ac_merge_pr(
                workspace_id,
                pr_number,
                merge_method,
                commit_title,
                commit_message,
            )
            .await
        })
    }

    fn accept_changes_add_remote(
        &self,
        workspace_id: WorkspaceId,
        remote_url: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let svc = self.clone();
        Box::pin(async move { svc.ac_add_remote(workspace_id, remote_url).await })
    }

    fn upsert_client(
        &self,
        client_id: ClientId,
        name: Option<String>,
        capabilities: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<()>> {
        let svc = self.clone();
        Box::pin(async move { svc.client_hello_upsert(client_id, name, capabilities).await })
    }

    fn draft_get(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<Option<Draft>>> {
        let svc = self.clone();
        Box::pin(async move { svc.drafts_get(workspace_id, agent_id, client_id).await })
    }

    fn draft_set(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
        text: String,
    ) -> BoxFuture<'_, Result<Option<String>>> {
        let svc = self.clone();
        Box::pin(async move {
            svc.drafts_set(workspace_id, agent_id, client_id, text)
                .await
        })
    }

    fn draft_clear(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        client_id: ClientId,
    ) -> BoxFuture<'_, Result<()>> {
        let svc = self.clone();
        Box::pin(async move { svc.drafts_clear(workspace_id, agent_id, client_id).await })
    }

    /// `browser.exec` — agent-initiated CDP forward (REV-1, PROTOCOL §5.14/§12.4).
    ///
    /// When an agent triggers this via the MCP `ws.browser.exec` binding
    /// there is no per-connection reverse channel to use (the caller is the
    /// daemon-hosted MCP server, not a client connection), so we route the
    /// batch to the first-registered live client via the injected
    /// [`AgentReverseDispatch`]. Attribution fields (`workspaceId`, `agentId`,
    /// `tabId`) are threaded into the forwarded params so the FE sees the
    /// same envelope shape the client-triggered path already emits. Result
    /// shaping stays in [`browser_ops`] (byte-for-byte parity with the FE
    /// tool); failure modes surface as `Error::Internal` with the underlying
    /// reason so the MCP caller can distinguish "no client connected" from a
    /// proxy-side failure. An empty `actions` batch is rejected with
    /// `Error::InvalidParams` (JSON-RPC `-32602`) before any dispatch — the
    /// FE tool has no meaningful behavior on an empty batch and the
    /// client-triggered path already enforces the same guard. When no
    /// dispatcher is wired (unit tests, read-only wiring) the request
    /// short-circuits with `no client connected`.
    fn browser_exec(
        &self,
        workspace_id: WorkspaceId,
        actions: Vec<serde_json::Value>,
        tab_id: Option<String>,
        agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            if actions.is_empty() {
                return Err(Error::InvalidParams(
                    "browser.exec: actions must be a non-empty array".to_string(),
                ));
            }
            let Some(dispatch) = self.reverse_dispatch.clone() else {
                return Err(Error::Internal(
                    "browser.exec: no client connected".to_string(),
                ));
            };
            let args = browser_ops::BrowserExecArgs {
                actions,
                tab_id,
                agent_id: agent_id.map(|a| a.as_str().to_string()),
                workspace_id: Some(workspace_id.as_str().to_string()),
            };
            let forwarded = browser_ops::build_forward_params(&args);
            let response =
                dispatch
                    .dispatch("browser.exec", forwarded)
                    .await
                    .map_err(|e| match e {
                        intent_core::ReverseDispatchError::NoClient => {
                            Error::Internal("browser.exec: no client connected".to_string())
                        }
                        intent_core::ReverseDispatchError::Transport { message, .. } => {
                            Error::Internal(format!("browser.exec: {message}"))
                        }
                    })?;
            browser_ops::shape_result(response).map_err(|e| Error::Internal(e.message))
        })
    }
}

/// Sandbox provisioning and lifecycle for CoW agent isolation (direct-mode workspaces).
impl Services {
    /// Provision a sandbox for an agent in a direct-mode workspace.
    /// Returns `ProvisionOutcome::Supported` if CoW is available, or `Unsupported` for fallback.
    pub async fn provision_sandbox(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<sandbox_ops::ProvisionOutcome> {
        let config = sandbox_ops::ProvisionConfig {
            workspaces_root: self
                .workspaces_root
                .clone()
                .ok_or_else(|| Error::Internal("workspaces_root not configured".to_string()))?,
        };
        sandbox_ops::provision_sandbox(&self.store, workspace_id, agent_id, &config).await
    }

    /// Discard a sandbox: remove the directory and database record.
    pub async fn discard_sandbox(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) -> Result<()> {
        sandbox_ops::discard_sandbox(&self.store, workspace_id, agent_id).await
    }

    /// Garbage-collect orphaned sandboxes (startup GC).
    pub async fn gc_orphaned_sandboxes(&self) -> Result<()> {
        sandbox_ops::gc_orphaned_sandboxes(&self.store).await
    }
}

/// Orchestration backing the `accept-changes.*` methods (§5.18). Kept on
/// `Services` (not the trait) so the steps can share `self.store` /
/// `self.source_control` / `self.event_bus` / `self.worktree_locks` and reuse the
/// `pr_ops` / `accept_changes` glue.
impl Services {
    /// `accept-changes.getStatus`: the `WorkspaceGitStatus` for the panel.
    async fn ac_get_status(&self, workspace_id: WorkspaceId) -> Result<serde_json::Value> {
        let ws = self.store.get_workspace(&workspace_id).await.map_err(|_| {
            Error::Internal(format!("Workspace not found: {}", workspace_id.as_str()))
        })?;
        let trunk = accept_changes::trunk_branch(&ws);
        if ws.is_remote {
            return Ok(accept_changes::minimal_status_value(&ws, &trunk));
        }
        match git_ops::worktree_path(&ws) {
            Some(worktree) => accept_changes::build_git_status_value(&worktree, &ws),
            None => Ok(accept_changes::minimal_status_value(&ws, &trunk)),
        }
    }

    /// `accept-changes.prepare`: validate + suggest (PrepareResult).
    async fn ac_prepare(
        &self,
        workspace_id: WorkspaceId,
        action: String,
        files: Option<Vec<String>>,
    ) -> Result<serde_json::Value> {
        accept_changes::validate_action(&action)?;
        let ws = match self.store.get_workspace(&workspace_id).await {
            Ok(w) => w,
            Err(_) => return Ok(accept_changes::prepare_invalid("Workspace not found")),
        };
        let Some(worktree) = git_ops::worktree_path(&ws) else {
            return Ok(accept_changes::prepare_invalid("Workspace has no path"));
        };
        accept_changes::build_prepare_value(&worktree, &ws, &action, files.as_deref())
    }

    /// `accept-changes.execute`: dispatch the action under the worktree lock.
    async fn ac_execute(
        &self,
        workspace_id: WorkspaceId,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let action_raw = params
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::InvalidParams("Missing required parameter: action".to_string())
            })?;
        let action = accept_changes::validate_action(action_raw)?;

        let files = json_str_array(params.get("files"));
        let commit_message = json_opt_str(params.get("commitMessage"));
        let pr_title = json_opt_str(params.get("prTitle"));
        let pr_body = json_opt_str(params.get("prBody"));
        let target_branch = json_opt_str(params.get("targetBranch"));
        let options = params.get("options");
        let stage_unstaged = json_opt_bool(options, "stageUnstaged");
        let push_after = json_opt_bool(options, "pushAfterCommit");
        let create_pr_after = json_opt_bool(options, "createPRAfterPush");

        let ws = self.store.get_workspace(&workspace_id).await.map_err(|_| {
            Error::Internal(format!("Workspace not found: {}", workspace_id.as_str()))
        })?;
        let extras = AcExtras {
            trunk: accept_changes::trunk_branch(&ws),
            up_to_commit_hash: json_opt_str(params.get("upToCommitHash")),
            merge_strategy: json_opt_str(params.get("mergeStrategy")),
            rebase_first: json_opt_bool(options, "rebaseFirst"),
            local_only: json_opt_bool(options, "localOnly"),
            undo_commits_metadata: parse_undo_metadata(params.get("undoCommitsMetadata")),
        };
        if ws.is_remote {
            let msg = "remote workspaces are not supported by intentd yet";
            let step = accept_changes::step(action, "Execute", "failed", None, Some(msg));
            return Ok(accept_changes::accept_result(
                false,
                vec![step],
                None,
                Some(msg.to_string()),
            ));
        }
        let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
            Error::Internal(format!("Workspace has no path: {}", workspace_id.as_str()))
        })?;
        let branch = ws.branch.clone();

        let locks = self.worktree_locks.clone();
        locks
            .with_lock(&worktree, || async {
                self.ac_run_pipeline(
                    &workspace_id,
                    &worktree,
                    &branch,
                    action,
                    files,
                    commit_message,
                    pr_title,
                    pr_body,
                    target_branch,
                    stage_unstaged,
                    push_after,
                    create_pr_after,
                    extras,
                )
                .await
            })
            .await
    }

    /// Run the requested action's step sequence, accumulating per-step status.
    /// A failing step short-circuits with `success:false`; on success the
    /// recomputed metrics + refreshed git-status are emitted.
    #[allow(clippy::too_many_arguments)]
    async fn ac_run_pipeline(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        branch: &str,
        action: &str,
        files: Option<Vec<String>>,
        commit_message: Option<String>,
        pr_title: Option<String>,
        pr_body: Option<String>,
        target_branch: Option<String>,
        stage_unstaged: bool,
        push_after: bool,
        create_pr_after: bool,
        extras: AcExtras,
    ) -> Result<serde_json::Value> {
        let mut steps: Vec<serde_json::Value> = Vec::new();
        let mut result = serde_json::Map::new();

        match action {
            "commit" => {
                match self
                    .ac_commit(
                        workspace_id,
                        worktree,
                        commit_message,
                        files,
                        stage_unstaged,
                    )
                    .await
                {
                    Ok(hash) => {
                        let short = &hash[..hash.len().min(7)];
                        steps.push(accept_changes::step(
                            "commit",
                            "Commit changes",
                            "completed",
                            Some(&format!("Committed {short}")),
                            None,
                        ));
                        result.insert("commitHash".to_string(), serde_json::json!(hash));
                    }
                    Err(e) => return Ok(fail_step("commit", "Commit changes", steps, result, e)),
                }
                if push_after {
                    match self.ac_push(workspace_id, worktree, branch).await {
                        Ok(sha) => {
                            steps.push(accept_changes::step(
                                "push",
                                "Push to remote",
                                "completed",
                                None,
                                None,
                            ));
                            result.insert("pushedSha".to_string(), serde_json::json!(sha));
                        }
                        Err(e) => return Ok(fail_step("push", "Push to remote", steps, result, e)),
                    }
                }
                if create_pr_after {
                    match self
                        .ac_create_pr(workspace_id, worktree, pr_title, pr_body, target_branch)
                        .await
                    {
                        Ok((num, url)) => {
                            steps.push(accept_changes::step(
                                "create-pr",
                                "Create pull request",
                                "completed",
                                None,
                                None,
                            ));
                            result.insert("prNumber".to_string(), serde_json::json!(num));
                            result.insert("prUrl".to_string(), serde_json::json!(url));
                        }
                        Err(e) => {
                            return Ok(fail_step(
                                "create-pr",
                                "Create pull request",
                                steps,
                                result,
                                e,
                            ))
                        }
                    }
                }
            }
            "push" => match self.ac_push(workspace_id, worktree, branch).await {
                Ok(sha) => {
                    steps.push(accept_changes::step(
                        "push",
                        "Push to remote",
                        "completed",
                        None,
                        None,
                    ));
                    result.insert("pushedSha".to_string(), serde_json::json!(sha));
                }
                Err(e) => return Ok(fail_step("push", "Push to remote", steps, result, e)),
            },
            "create-pr" => {
                match self
                    .ac_create_pr(workspace_id, worktree, pr_title, pr_body, target_branch)
                    .await
                {
                    Ok((num, url)) => {
                        steps.push(accept_changes::step(
                            "create-pr",
                            "Create pull request",
                            "completed",
                            None,
                            None,
                        ));
                        result.insert("prNumber".to_string(), serde_json::json!(num));
                        result.insert("prUrl".to_string(), serde_json::json!(url));
                    }
                    Err(e) => {
                        return Ok(fail_step(
                            "create-pr",
                            "Create pull request",
                            steps,
                            result,
                            e,
                        ))
                    }
                }
            }
            "undo-commit" => {
                if let Err(failed) = self
                    .ac_undo_commit(
                        workspace_id,
                        worktree,
                        extras.up_to_commit_hash.as_deref(),
                        &extras.undo_commits_metadata,
                        &mut steps,
                    )
                    .await
                {
                    return Ok(failed);
                }
            }
            "undo-push" => {
                if let Err(failed) = self
                    .ac_undo_push(
                        worktree,
                        branch,
                        extras.up_to_commit_hash.as_deref(),
                        &mut steps,
                    )
                    .await
                {
                    return Ok(failed);
                }
            }
            "reset-to-trunk" => {
                if let Err(failed) = self
                    .ac_reset_to_trunk(worktree, &extras.trunk, &mut steps, &mut result)
                    .await
                {
                    return Ok(failed);
                }
            }
            "rebase-onto-trunk" => {
                if let Err(failed) = self
                    .ac_rebase_onto_trunk(worktree, branch, &extras.trunk, &mut steps, &mut result)
                    .await
                {
                    return Ok(failed);
                }
            }
            "merge" => {
                if let Err(failed) = self
                    .ac_merge(
                        workspace_id,
                        worktree,
                        branch,
                        &extras,
                        commit_message,
                        &mut steps,
                        &mut result,
                    )
                    .await
                {
                    return Ok(failed);
                }
            }
            other => {
                // `export` stays deferred: the TS export path copies the changed
                // files to an arbitrary target folder, which is not yet ported.
                let msg = format!("the '{other}' action is not supported by intentd yet");
                steps.push(accept_changes::step(
                    other,
                    "Execute",
                    "failed",
                    None,
                    Some(&msg),
                ));
                return Ok(accept_changes::accept_result(false, steps, None, Some(msg)));
            }
        }

        self.ac_emit_after_mutation(workspace_id, worktree).await;
        let result = if result.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(result))
        };
        Ok(accept_changes::accept_result(true, steps, result, None))
    }

    /// Stage (optionally) then commit, restoring attribution (staged/unstaged →
    /// committed) for the committed files. Returns the new commit SHA.
    async fn ac_commit(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        message: Option<String>,
        files: Option<Vec<String>>,
        stage_unstaged: bool,
    ) -> Result<String> {
        // accept-changes is a user-initiated action → bypass the auto-commit gate.
        git_ops::assert_agent_commit_allowed(&self.store, true).await?;
        let message = message
            .filter(|m| !m.trim().is_empty())
            .ok_or_else(|| Error::Internal("Commit message is required".to_string()))?;

        let to_stage = match files {
            Some(f) if !f.is_empty() => f,
            _ if stage_unstaged => intent_git::commit::all_changed_paths(worktree)?,
            _ => Vec::new(),
        };
        if !to_stage.is_empty() {
            intent_git::stage::stage(worktree, &to_stage)?;
        }
        let outcome = intent_git::commit::commit(worktree, &message)?;

        for path in &outcome.files {
            let key = crate::file_tracking::normalize_path(path);
            let _ = self
                .store
                .set_tracked_change_stage(workspace_id, &key, "staged", "committed")
                .await;
            let _ = self
                .store
                .set_tracked_change_stage(workspace_id, &key, "unstaged", "committed")
                .await;
        }
        Ok(outcome.hash)
    }

    /// Push the branch to `origin`, restoring attribution (committed → pushed).
    async fn ac_push(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        branch: &str,
    ) -> Result<String> {
        if intent_git::remote::origin_url(worktree)?.is_none() {
            return Err(Error::Internal(
                "No remote configured for this repository".to_string(),
            ));
        }
        let outcome = intent_git::push::push(worktree, "origin", branch, false)?;
        self.ac_move_stage(workspace_id, "committed", "pushed")
            .await;
        Ok(outcome.pushed_sha)
    }

    /// Create a PR via the forge (reusing an existing link), persist the linkage,
    /// emit `pr:linked`, and restore attribution (pushed → pull_request).
    async fn ac_create_pr(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        title: Option<String>,
        body: Option<String>,
        target: Option<String>,
    ) -> Result<(u64, String)> {
        let mut ws = self.store.get_workspace(workspace_id).await?;
        // An already-linked PR is reused (idempotent create).
        if let Some(number) = ws.pr_number {
            return Ok((number, ws.pr_url.clone().unwrap_or_default()));
        }
        let (owner, repo) = pr_ops::repo_of(&ws)
            .map_err(|_| Error::Internal("No remote configured for this repository".to_string()))?;
        let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
        let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);

        let branch = ws.branch.clone();
        let target_branch = target
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| accept_changes::trunk_branch(&ws));
        let title = title
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| branch.clone());
        let input = intent_sourcecontrol::NewPullRequest {
            title,
            body,
            source_branch: branch,
            target_branch,
            draft: false,
        };
        let pr = sc
            .create_pr(&repo_ref, input)
            .await
            .map_err(pr_ops::map_sc_err)?;

        let info = pr_ops::build_pr_info(&pr);
        ws.pr_number = Some(pr.number);
        ws.pr_url = Some(pr.url.clone());
        ws.pr_status = Some(info.status);
        ws.active_pull_request = Some(info);
        ws.updated_at = now_iso();
        self.store.update_workspace(&ws).await?;
        publish_event(&self.event_bus, pr_linked_event(&ws)).await;

        let _ = worktree; // attribution move below is workspace-scoped.
        self.ac_move_stage(workspace_id, "pushed", "pull_request")
            .await;
        Ok((pr.number, pr.url))
    }

    /// `accept-changes.mergePR`: merge the linked PR via the forge.
    async fn ac_merge_pr(
        &self,
        workspace_id: WorkspaceId,
        pr_number: u64,
        merge_method: Option<String>,
        commit_title: Option<String>,
        commit_message: Option<String>,
    ) -> Result<serde_json::Value> {
        let method = pr_ops::validate_merge_method(merge_method)?;
        let mut ws = self.store.get_workspace(&workspace_id).await.map_err(|_| {
            Error::Internal(format!("Workspace not found: {}", workspace_id.as_str()))
        })?;
        let (owner, repo) = pr_ops::repo_of(&ws)?;
        let sc = pr_ops::resolve_source_control(self.source_control.clone()).await?;
        let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
        let options = intent_sourcecontrol::MergeOptions {
            commit_title,
            commit_message,
        };
        match sc.merge_pr(&repo_ref, pr_number, method, options).await {
            Ok(outcome) => {
                ws.pr_status = Some(intent_core::PullRequestStatus::Merged);
                if let Some(info) = ws.active_pull_request.as_mut() {
                    info.status = intent_core::PullRequestStatus::Merged;
                }
                ws.updated_at = now_iso();
                let _ = self.store.update_workspace(&ws).await;
                publish_event(&self.event_bus, pr_updated_event(&ws)).await;
                self.ac_move_stage(&workspace_id, "pull_request", "merged")
                    .await;
                let step = accept_changes::step(
                    "merge",
                    "Merge PR",
                    "completed",
                    Some(&outcome.message),
                    None,
                );
                let result = serde_json::json!({
                    "prNumber": pr_number,
                    "mergeCommitHash": outcome.sha,
                });
                Ok(accept_changes::accept_result(
                    true,
                    vec![step],
                    Some(result),
                    None,
                ))
            }
            Err(e) => {
                let msg = e.to_string();
                let step = accept_changes::step("merge", "Merge PR", "failed", None, Some(&msg));
                Ok(accept_changes::accept_result(
                    false,
                    vec![step],
                    None,
                    Some(msg),
                ))
            }
        }
    }

    /// `accept-changes.addRemote`: add (and, if needed, init) `origin`, then
    /// return + broadcast the refreshed `WorkspaceGitStatus`.
    async fn ac_add_remote(
        &self,
        workspace_id: WorkspaceId,
        remote_url: String,
    ) -> Result<serde_json::Value> {
        let ws = self.store.get_workspace(&workspace_id).await.map_err(|_| {
            Error::Internal(format!("Workspace not found: {}", workspace_id.as_str()))
        })?;
        let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
            Error::Internal(format!("Workspace has no path: {}", workspace_id.as_str()))
        })?;
        let trimmed = remote_url.trim();
        if trimmed.is_empty() {
            return Err(Error::Internal("Remote URL cannot be empty".to_string()));
        }
        if !accept_changes::is_valid_git_remote_url(trimmed) {
            return Err(Error::Internal(
                "Invalid remote URL. Accepted formats: https://, http://, git@host:path, ssh://, or git://"
                    .to_string(),
            ));
        }
        let desired = if ws.branch.is_empty() {
            "main".to_string()
        } else {
            ws.branch.clone()
        };
        intent_git::remote::add_origin(&worktree, trimmed, &desired)?;

        let status = accept_changes::build_git_status_value(&worktree, &ws)?;
        publish_event(
            &self.event_bus,
            changes_git_status_event(&workspace_id, status.clone()),
        )
        .await;
        Ok(status)
    }

    /// Move every distinct tracked-change path for the workspace from `from`
    /// stage to `to` stage (attribution restore across a git stage transition).
    async fn ac_move_stage(&self, workspace_id: &WorkspaceId, from: &str, to: &str) {
        let rows = self
            .store
            .list_tracked_changes(workspace_id)
            .await
            .unwrap_or_default();
        let mut seen = HashSet::new();
        for row in rows {
            if row.stage == from && seen.insert(row.path.clone()) {
                let _ = self
                    .store
                    .set_tracked_change_stage(workspace_id, &row.path, from, to)
                    .await;
            }
        }
    }

    /// `undo-commit`: soft-reset `HEAD` to `up_to_commit_hash` (keeping the undone
    /// changes staged) and restore the agent attribution of the undone files.
    /// On success it appends a completed step (no result payload); the missing-hash
    /// and reset-failure paths return the parity-exact failure result.
    async fn ac_undo_commit(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        up_to_commit_hash: Option<&str>,
        metadata: &[UndoCommitMeta],
        steps: &mut Vec<serde_json::Value>,
    ) -> std::result::Result<(), serde_json::Value> {
        let Some(hash) = up_to_commit_hash.filter(|h| !h.is_empty()) else {
            // The TS early return pushes no step (empty `steps`).
            return Err(accept_changes::accept_result(
                false,
                steps.clone(),
                None,
                Some("Commit hash required for undo-commit".to_string()),
            ));
        };

        if let Err(e) = intent_git::reset::reset_soft(worktree, hash) {
            return Err(ac_step_failure(
                steps.clone(),
                "undo-commit",
                "Undo local commit",
                Some(&e.to_string()),
                "Failed to undo commit".to_string(),
            ));
        }

        self.ac_restore_undo_attribution(workspace_id, worktree, metadata)
            .await;

        steps.push(accept_changes::step(
            "undo-commit",
            "Undo local commit",
            "completed",
            None,
            None,
        ));
        Ok(())
    }

    /// Restore agent attribution for the files of the undone commits (mirrors the
    /// TS `recordAgentWrite` loop): after the soft reset each changed path is
    /// re-attributed to its original agent/task at whichever stage it now sits.
    /// Best-effort — failures never fail the undo.
    async fn ac_restore_undo_attribution(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        metadata: &[UndoCommitMeta],
    ) {
        if metadata.is_empty() {
            return;
        }
        let status = match intent_git::status::status(worktree) {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut by_path: HashMap<String, (bool, &'static str)> = HashMap::new();
        for f in &status.files {
            let word = match f.status {
                intent_core::GitFileStatus::Added | intent_core::GitFileStatus::Untracked => {
                    "added"
                }
                intent_core::GitFileStatus::Deleted => "deleted",
                _ => "modified",
            };
            let key = crate::file_tracking::normalize_path(&f.path);
            let entry = by_path.entry(key).or_insert((f.staged, word));
            if f.staged {
                *entry = (true, word);
            }
        }
        for meta in metadata {
            let Some(agent_id) = meta.agent_id.clone() else {
                continue;
            };
            for file in &meta.files {
                let key = crate::file_tracking::normalize_path(file);
                let Some((staged, word)) = by_path.get(&key) else {
                    continue;
                };
                let change = intent_store::NewTrackedChange {
                    workspace_id: workspace_id.clone(),
                    path: key.clone(),
                    stage: if *staged { "staged" } else { "unstaged" }.to_string(),
                    status: word.to_string(),
                    agent_id: Some(agent_id.clone()),
                    session_id: meta.linked_note_id.clone(),
                    turn: None,
                    commit_hash: None,
                    old_blob_sha: None,
                    new_blob_sha: None,
                    additions: 0,
                    deletions: 0,
                };
                let _ = crate::file_tracking::track_change(&self.store, change).await;
            }
        }
    }

    /// `undo-push`: force-push the earlier `up_to_commit_hash` onto the remote
    /// branch (rewinding the remote), then refresh the local tracking ref. On
    /// success it appends a completed step (no result payload).
    async fn ac_undo_push(
        &self,
        worktree: &Path,
        branch: &str,
        up_to_commit_hash: Option<&str>,
        steps: &mut Vec<serde_json::Value>,
    ) -> std::result::Result<(), serde_json::Value> {
        let Some(hash) = up_to_commit_hash.filter(|h| !h.is_empty()) else {
            return Err(accept_changes::accept_result(
                false,
                steps.clone(),
                None,
                Some("Commit hash required for undo-push".to_string()),
            ));
        };

        if let Err(e) = intent_git::push::push_refspec(worktree, "origin", hash, branch, true) {
            return Err(ac_step_failure(
                steps.clone(),
                "undo-push",
                "Undo pushed commits",
                Some(&e.to_string()),
                "Failed to undo push".to_string(),
            ));
        }
        // `push_refspec` already advances the local tracking ref; the follow-up
        // fetch (TS parity) is best-effort and never fails the undo. Driven
        // through `git_fetch_bounded` so a hung remote can't pin a runtime
        // worker for the shell-fetch inner deadline (100s).
        let _ = git_fetch_bounded(worktree, "origin", branch).await;

        steps.push(accept_changes::step(
            "undo-push",
            "Undo pushed commits",
            "completed",
            None,
            None,
        ));
        Ok(())
    }

    /// `reset-to-trunk`: hard-reset the branch to the trunk tip after verifying the
    /// worktree is porcelain-clean and the trunk ref name is safe. Fetches
    /// `origin/<trunk>` first (non-fatal). On success appends a completed step and
    /// records `{ newHeadSha }`.
    async fn ac_reset_to_trunk(
        &self,
        worktree: &Path,
        trunk: &str,
        steps: &mut Vec<serde_json::Value>,
        result: &mut serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<(), serde_json::Value> {
        let status = match intent_git::status::status(worktree) {
            Ok(s) => s,
            Err(_) => {
                return Err(ac_step_failure(
                    steps.clone(),
                    "reset-to-trunk",
                    "Reset to trunk",
                    Some("Failed to verify worktree state"),
                    "Unable to verify worktree is clean before reset. Please try again."
                        .to_string(),
                ));
            }
        };
        if !status.files.is_empty() {
            return Err(ac_step_failure(
                steps.clone(),
                "reset-to-trunk",
                "Reset to trunk",
                Some("Cannot reset: uncommitted or staged changes exist"),
                "Cannot reset while there are uncommitted or staged changes. Please commit or \
                 discard changes first."
                    .to_string(),
            ));
        }
        if !accept_changes::is_safe_ref(trunk) {
            return Err(ac_step_failure(
                steps.clone(),
                "reset-to-trunk",
                "Reset to trunk",
                Some("Invalid trunk branch name"),
                format!("Invalid trunk branch name: {trunk}"),
            ));
        }

        let has_remote = intent_git::remote::origin_url(worktree)
            .map(|u| u.is_some())
            .unwrap_or(false);
        if has_remote {
            let _ = git_fetch_bounded(worktree, "origin", trunk).await;
        }
        let reset_target = if has_remote {
            format!("origin/{trunk}")
        } else {
            trunk.to_string()
        };

        if let Err(e) = intent_git::reset::reset_hard(worktree, &reset_target) {
            return Err(ac_step_failure(
                steps.clone(),
                "reset-to-trunk",
                "Reset to trunk",
                Some(&e.to_string()),
                "Failed to reset to trunk".to_string(),
            ));
        }
        let new_head = match intent_git::refs::rev_parse(worktree, "HEAD") {
            Ok(h) => h,
            Err(e) => {
                return Err(ac_step_failure(
                    steps.clone(),
                    "reset-to-trunk",
                    "Reset to trunk",
                    Some(&e.to_string()),
                    "Failed to reset to trunk".to_string(),
                ));
            }
        };

        steps.push(accept_changes::step(
            "reset-to-trunk",
            "Reset to trunk",
            "completed",
            None,
            None,
        ));
        result.insert("newHeadSha".to_string(), serde_json::json!(new_head));
        Ok(())
    }

    /// `rebase-onto-trunk`: validate the ref names, fetch `origin/<trunk>`
    /// (non-fatal), abort early if merging would conflict, then rebase the branch
    /// onto the trunk with auto-stash. On success appends a completed step and
    /// records `{ autoRebased: true, newBaseSha? }`.
    async fn ac_rebase_onto_trunk(
        &self,
        worktree: &Path,
        branch: &str,
        trunk: &str,
        steps: &mut Vec<serde_json::Value>,
        result: &mut serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<(), serde_json::Value> {
        if !accept_changes::is_safe_ref(trunk) {
            return Err(ac_step_failure(
                steps.clone(),
                "rebase-onto-trunk",
                "Rebase onto trunk",
                Some("Invalid trunk branch name"),
                format!("Invalid trunk branch name: {trunk}"),
            ));
        }
        if !accept_changes::is_safe_ref(branch) {
            return Err(ac_step_failure(
                steps.clone(),
                "rebase-onto-trunk",
                "Rebase onto trunk",
                Some("Invalid branch name"),
                format!("Invalid branch name: {branch}"),
            ));
        }

        let has_remote = intent_git::remote::origin_url(worktree)
            .map(|u| u.is_some())
            .unwrap_or(false);
        let trunk_ref = if has_remote {
            format!("origin/{trunk}")
        } else {
            trunk.to_string()
        };
        if has_remote {
            let _ = git_fetch_bounded(worktree, "origin", trunk).await;
        }

        let has_conflicts =
            intent_git::conflicts::detect_merge_conflicts(worktree, branch, &trunk_ref)
                .map(|m| m.has_conflicts)
                .unwrap_or(false);
        if has_conflicts {
            return Err(ac_step_failure(
                steps.clone(),
                "rebase-onto-trunk",
                "Rebase onto trunk",
                Some("Conflicts detected. Please rebase manually."),
                "Conflicts detected. Please rebase manually.".to_string(),
            ));
        }

        let captured = intent_git::refs::rev_parse(worktree, &trunk_ref).ok();
        let outcome = match intent_git::rebase::rebase_with_autostash(worktree, &trunk_ref) {
            Ok(o) => o,
            Err(e) => {
                return Err(ac_step_failure(
                    steps.clone(),
                    "rebase-onto-trunk",
                    "Rebase onto trunk",
                    Some(&e.to_string()),
                    e.to_string(),
                ));
            }
        };
        if !outcome.success {
            let msg = outcome.error.clone().unwrap_or_else(|| {
                "Rebase onto trunk failed. Please try rebasing manually.".to_string()
            });
            return Err(ac_step_failure(
                steps.clone(),
                "rebase-onto-trunk",
                "Rebase onto trunk",
                Some(&msg),
                msg.clone(),
            ));
        }

        steps.push(accept_changes::step(
            "rebase-onto-trunk",
            "Rebase onto trunk",
            "completed",
            None,
            None,
        ));
        result.insert("autoRebased".to_string(), serde_json::json!(true));
        if let Some(sha) = captured {
            result.insert("newBaseSha".to_string(), serde_json::json!(sha));
        }
        Ok(())
    }

    /// `merge`: commit any staged changes, then advance the trunk to the branch
    /// (locally via `update-ref`, or on the remote via a refspec push), rebasing
    /// onto trunk first when the branch is behind. Mirrors the TS local-trunk /
    /// remote-trunk merge flow incl. the squash strategy and auto-rebase.
    #[allow(clippy::too_many_arguments)]
    async fn ac_merge(
        &self,
        workspace_id: &WorkspaceId,
        worktree: &Path,
        branch: &str,
        extras: &AcExtras,
        commit_message: Option<String>,
        steps: &mut Vec<serde_json::Value>,
        result: &mut serde_json::Map<String, serde_json::Value>,
    ) -> std::result::Result<(), serde_json::Value> {
        let trunk = extras.trunk.as_str();

        // Commit any staged changes first (separate `commit` step, like the TS).
        let status = match intent_git::status::status(worktree) {
            Ok(s) => s,
            Err(e) => {
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some(&e.to_string()),
                    e.to_string(),
                ));
            }
        };
        if status.files.iter().any(|f| f.staged) {
            let message = commit_message
                .clone()
                .filter(|m| !m.trim().is_empty())
                .unwrap_or_else(|| format!("Changes for merge to {trunk}"));
            match self
                .ac_commit(workspace_id, worktree, Some(message), None, false)
                .await
            {
                Ok(_) => steps.push(accept_changes::step(
                    "commit",
                    "Commit staged changes",
                    "completed",
                    None,
                    None,
                )),
                Err(e) => {
                    return Err(ac_step_failure(
                        steps.clone(),
                        "commit",
                        "Commit staged changes",
                        Some(&e.to_string()),
                        "Failed to commit staged changes".to_string(),
                    ));
                }
            }
        }

        let has_remote = intent_git::remote::origin_url(worktree)
            .map(|u| u.is_some())
            .unwrap_or(false);
        let is_pushed = has_remote
            && intent_git::remote::remote_tracking_exists(worktree, "origin", branch)
                .unwrap_or(false);
        let has_diverged = is_pushed
            && !intent_git::refs::is_ancestor(worktree, &format!("origin/{branch}"), branch)
                .unwrap_or(false);

        // Decide whether the remote carries the trunk branch.
        let has_remote_trunk = if extras.local_only || !has_remote {
            false
        } else {
            match intent_git::remote::ls_remote_has_branch(worktree, "origin", trunk) {
                Ok(intent_git::remote::RemoteBranch::Present) => true,
                Ok(intent_git::remote::RemoteBranch::Missing) => false,
                Err(_) => {
                    return Err(ac_step_failure(
                        steps.clone(),
                        "merge",
                        "Merge to trunk",
                        None,
                        "Unable to reach remote 'origin'. Check your network connection and \
                         authentication."
                            .to_string(),
                    ));
                }
            }
        };

        if has_remote_trunk {
            if let Err(e) = git_fetch_bounded(worktree, "origin", trunk).await {
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some(&e.to_string()),
                    e.to_string(),
                ));
            }
        }

        let trunk_ref = if has_remote_trunk {
            format!("origin/{trunk}")
        } else {
            trunk.to_string()
        };

        if extras.rebase_first {
            let outcome = match intent_git::rebase::rebase_with_autostash(worktree, &trunk_ref) {
                Ok(o) => o,
                Err(e) => {
                    return Err(ac_step_failure(
                        steps.clone(),
                        "merge",
                        "Merge to trunk",
                        Some(&e.to_string()),
                        e.to_string(),
                    ));
                }
            };
            if !outcome.success {
                let step_err = outcome
                    .error
                    .clone()
                    .unwrap_or_else(|| "Failed to rebase".to_string());
                let top_err = outcome
                    .error
                    .clone()
                    .unwrap_or_else(|| "Failed to rebase. Resolve conflicts manually.".to_string());
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some(&step_err),
                    top_err,
                ));
            }
        }

        // Push the branch (force when we just rebased a pushed branch or it has
        // diverged) so the remote feature ref matches local before the trunk move.
        if has_remote_trunk {
            let needs_force = (extras.rebase_first && is_pushed) || has_diverged;
            if let Err(e) =
                intent_git::push::push_refspec(worktree, "origin", "HEAD", branch, needs_force)
            {
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some(&e.to_string()),
                    e.to_string(),
                ));
            }
        }

        let can_fast_forward =
            intent_git::refs::is_ancestor(worktree, &trunk_ref, "HEAD").unwrap_or(false);

        let mut auto_rebased = false;
        let mut rebase_base_sha: Option<String> = None;

        if !can_fast_forward {
            let fresh_conflicts =
                intent_git::conflicts::detect_merge_conflicts(worktree, branch, &trunk_ref)
                    .map(|m| m.has_conflicts)
                    .unwrap_or(false);
            if fresh_conflicts {
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some("Conflicts detected. Please rebase manually."),
                    "Conflicts detected. Please rebase manually.".to_string(),
                ));
            }

            let captured = intent_git::refs::rev_parse(worktree, &trunk_ref).ok();
            let outcome = match intent_git::rebase::rebase_with_autostash(worktree, &trunk_ref) {
                Ok(o) => o,
                Err(e) => {
                    return Err(ac_step_failure(
                        steps.clone(),
                        "merge",
                        "Merge to trunk",
                        Some(&e.to_string()),
                        e.to_string(),
                    ));
                }
            };
            if !outcome.success {
                let msg = outcome.error.clone().unwrap_or_else(|| {
                    "Auto-rebase failed. Please try rebasing manually.".to_string()
                });
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some(&msg),
                    msg.clone(),
                ));
            }
            auto_rebased = true;
            rebase_base_sha = captured;

            if has_remote_trunk
                && intent_git::push::push_refspec(worktree, "origin", "HEAD", branch, true).is_err()
            {
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some("Failed to push after rebase."),
                    "Failed to push after rebase.".to_string(),
                ));
            }
        }

        // Advance the trunk to our work: a squash commit, or a fast-forward of the
        // current HEAD. The parent/merge-base is the trunk tip (an ancestor of HEAD
        // in every reachable path here, post fast-forward-check or rebase).
        let merge_commit_hash = if extras.merge_strategy.as_deref() == Some("squash") {
            let parent = match intent_git::refs::rev_parse(worktree, &trunk_ref) {
                Ok(p) => p,
                Err(e) => {
                    return Err(ac_step_failure(
                        steps.clone(),
                        "merge",
                        "Merge to trunk",
                        Some(&e.to_string()),
                        e.to_string(),
                    ));
                }
            };
            let message = commit_message
                .clone()
                .filter(|m| !m.trim().is_empty())
                .unwrap_or_else(|| format!("Squashed commit from {branch}"));
            let commit_hash = match intent_git::squash::commit_tree(worktree, &parent, &message) {
                Ok(h) => h,
                Err(e) => {
                    return Err(ac_step_failure(
                        steps.clone(),
                        "merge",
                        "Merge to trunk",
                        Some(&e.to_string()),
                        e.to_string(),
                    ));
                }
            };
            if let Err(e) = self
                .ac_advance_trunk(worktree, trunk, &commit_hash, has_remote_trunk)
                .await
            {
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some(&e.to_string()),
                    e.to_string(),
                ));
            }
            commit_hash
        } else {
            let current = match intent_git::refs::rev_parse(worktree, "HEAD") {
                Ok(h) => h,
                Err(e) => {
                    return Err(ac_step_failure(
                        steps.clone(),
                        "merge",
                        "Merge to trunk",
                        Some(&e.to_string()),
                        e.to_string(),
                    ));
                }
            };
            if let Err(e) = self
                .ac_advance_trunk(worktree, trunk, &current, has_remote_trunk)
                .await
            {
                return Err(ac_step_failure(
                    steps.clone(),
                    "merge",
                    "Merge to trunk",
                    Some(&e.to_string()),
                    e.to_string(),
                ));
            }
            current
        };

        steps.push(accept_changes::step(
            "merge",
            "Merge to trunk",
            "completed",
            None,
            None,
        ));
        result.insert(
            "mergeCommitHash".to_string(),
            serde_json::json!(merge_commit_hash),
        );
        if auto_rebased {
            result.insert("autoRebased".to_string(), serde_json::json!(true));
        }
        if let Some(sha) = rebase_base_sha {
            result.insert("newBaseSha".to_string(), serde_json::json!(sha));
        }
        Ok(())
    }

    /// Advance the trunk branch to `sha`: a fast-forward refspec push to the remote
    /// trunk when it exists, else a local `update-ref` of `refs/heads/<trunk>`.
    async fn ac_advance_trunk(
        &self,
        worktree: &Path,
        trunk: &str,
        sha: &str,
        has_remote_trunk: bool,
    ) -> Result<()> {
        if has_remote_trunk {
            intent_git::push::push_refspec(worktree, "origin", sha, trunk, false)?;
        } else {
            intent_git::squash::update_ref(worktree, &format!("refs/heads/{trunk}"), sha)?;
        }
        Ok(())
    }

    /// Recompute the workspace metrics and broadcast `changes:metrics-changed` +
    /// `changes:git-status` after a mutating accept-changes step.
    async fn ac_emit_after_mutation(&self, workspace_id: &WorkspaceId, worktree: &Path) {
        if let Err(e) = crate::metrics::recompute(&self.store, workspace_id).await {
            tracing::warn!(error = %e, "accept-changes: metrics recompute failed");
        }
        let metrics = match self.store.get_workspace_metrics(workspace_id).await {
            Ok(Some(ws)) => {
                let agents = self
                    .store
                    .list_agent_metrics_for_workspace(workspace_id)
                    .await
                    .unwrap_or_default();
                crate::metrics::workspace_metrics_value(&ws, &agents)
            }
            _ => serde_json::Value::Null,
        };
        publish_event(
            &self.event_bus,
            changes_metrics_changed_event(workspace_id, metrics),
        )
        .await;

        if let Ok(ws) = self.store.get_workspace(workspace_id).await {
            if let Ok(status) = accept_changes::build_git_status_value(worktree, &ws) {
                publish_event(
                    &self.event_bus,
                    changes_git_status_event(workspace_id, status),
                )
                .await;
            }
        }
    }
}

/// Build an `AcceptChangesResult` for a failed step: appends the failed step,
/// returns `success:false` with the error both on the step and at top level.
fn fail_step(
    id: &str,
    name: &str,
    mut steps: Vec<serde_json::Value>,
    result: serde_json::Map<String, serde_json::Value>,
    error: Error,
) -> serde_json::Value {
    let msg = error.to_string();
    steps.push(accept_changes::step(id, name, "failed", None, Some(&msg)));
    let result = if result.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(result))
    };
    accept_changes::accept_result(false, steps, result, Some(msg))
}

/// Build a failed `AcceptChangesResult` for a deferred accept-changes handler:
/// append a failed step (`step_error` may differ from, or be absent for, the
/// top-level `top_error`) and return `success:false` with no `result`. Mirrors
/// the TS handlers, which set a generic top-level error while the step carries
/// the specific cause (and a couple of paths set no step error at all).
fn ac_step_failure(
    mut steps: Vec<serde_json::Value>,
    id: &str,
    name: &str,
    step_error: Option<&str>,
    top_error: String,
) -> serde_json::Value {
    steps.push(accept_changes::step(id, name, "failed", None, step_error));
    accept_changes::accept_result(false, steps, None, Some(top_error))
}

/// Parse an optional JSON string-array param into `Vec<String>` (non-strings
/// skipped); absent/null/non-array → `None`.
fn json_str_array(value: Option<&serde_json::Value>) -> Option<Vec<String>> {
    value.and_then(|v| v.as_array()).map(|items| {
        items
            .iter()
            .filter_map(|v| v.as_str())
            .map(str::to_string)
            .collect()
    })
}

/// Parse an optional JSON string param (absent/null/non-string → `None`).
fn json_opt_str(value: Option<&serde_json::Value>) -> Option<String> {
    value.and_then(|v| v.as_str()).map(str::to_string)
}

/// Read a boolean flag from an `options` object (absent/non-bool → `false`).
fn json_opt_bool(options: Option<&serde_json::Value>, key: &str) -> bool {
    options
        .and_then(|o| o.get(key))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// One entry of the `undo-commit` `undoCommitsMetadata` list: the agent/task that
/// authored a now-undone commit and the files it touched, used to restore file
/// attribution after the soft reset (mirrors the TS `recordAgentWrite` loop).
struct UndoCommitMeta {
    agent_id: Option<String>,
    linked_note_id: Option<String>,
    files: Vec<String>,
}

/// Parse the `undoCommitsMetadata` param into [`UndoCommitMeta`] entries
/// (absent/non-array → empty); non-object entries are skipped.
fn parse_undo_metadata(value: Option<&serde_json::Value>) -> Vec<UndoCommitMeta> {
    value
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_object())
                .map(|obj| UndoCommitMeta {
                    agent_id: obj
                        .get("agentId")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    linked_note_id: obj
                        .get("linkedNoteId")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    files: obj
                        .get("files")
                        .and_then(|v| v.as_array())
                        .map(|fs| {
                            fs.iter()
                                .filter_map(|f| f.as_str())
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The execute-action inputs specific to the deferred mutating handlers
/// (`undo-commit` / `undo-push` / `reset-to-trunk` / `rebase-onto-trunk` /
/// `merge`), bundled so [`Services::ac_run_pipeline`] keeps a manageable arity.
struct AcExtras {
    /// Trunk branch name (`baseRef` minus `origin/`, else `main`).
    trunk: String,
    /// `upToCommitHash` for the undo handlers (the reset/rewind target).
    up_to_commit_hash: Option<String>,
    /// `mergeStrategy` (`"squash"` selects the squash-commit path).
    merge_strategy: Option<String>,
    /// `options.rebaseFirst` — rebase onto trunk before merging.
    rebase_first: bool,
    /// `options.localOnly` — skip all remote operations during merge.
    local_only: bool,
    /// `undoCommitsMetadata` — attribution restore inputs for `undo-commit`.
    undo_commits_metadata: Vec<UndoCommitMeta>,
}

/// Per-path attribution `(agent_id, session_id, turn)` carried across a
/// `file-tracking.sync` reconcile so reconciled rows keep their provenance.
type AttributionByPath = HashMap<String, (Option<String>, Option<String>, Option<i64>)>;

/// Resolve a workspace's worktree path for the `file-tracking.*` reads (used to
/// render the absolute `TrackedChange.file`). `None` for a missing/remote/
/// pathless workspace — the reads still return their persisted rows.
async fn ft_worktree(store: &Store, workspace_id: &WorkspaceId) -> Option<PathBuf> {
    let ws = store.get_workspace(workspace_id).await.ok()?;
    if ws.is_remote {
        return None;
    }
    git_ops::worktree_path(&ws)
}

/// The empty `file-tracking.load`/`getChanges` result (TS `emptyChangesResult`).
fn empty_changes_result() -> serde_json::Value {
    serde_json::json!({ "changes": [], "truncated": false, "totalCount": 0 })
}

/// Capture a `pr.waitForChanges` poll snapshot (TS `captureSnapshot`): the PR
/// plus its head-commit check-runs. Returns `None` when the PR cannot be
/// fetched (the caller treats the initial `None` as fatal, later ones as a
/// transient skip).
async fn capture_pr_snapshot(
    sc: &dyn intent_sourcecontrol::SourceControl,
    repo_ref: &intent_sourcecontrol::RepoRef,
    number: u64,
) -> Option<pr_ops::PrSnapshot> {
    let pr = sc.get_pr(repo_ref, number).await.ok()?;
    let checks = match pr.head_sha.as_deref().filter(|s| !s.is_empty()) {
        None => pr_ops::CheckFetch::NotAttempted,
        Some(sha) => match sc.check_runs(repo_ref, sha).await {
            Ok(runs) => pr_ops::CheckFetch::Ok(runs),
            Err(_) => pr_ops::CheckFetch::Failed,
        },
    };
    Some(pr_ops::build_snapshot(&pr, checks))
}

/// Load a workspace for a `pr.*` call, mapping a missing workspace onto the
/// "No active PR" error (→ `-32603`) so an unknown/unlinked workspace surfaces
/// like the TS `requirePrContext` guard (PROTOCOL §5.7).
async fn load_ws_for_pr(store: &Store, workspace_id: &WorkspaceId) -> Result<Workspace> {
    store
        .get_workspace(workspace_id)
        .await
        .map_err(|e| match e {
            Error::NotFound(_) => Error::Internal(pr_ops::NO_ACTIVE_PR.to_string()),
            other => other,
        })
}

/// Snake_case status word for a `TaskStatus` (matches the wire serialization).
fn status_word(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::NotStarted => "not_started",
        TaskStatus::Waiting => "waiting",
        TaskStatus::DiscussionNeeded => "discussion_needed",
        TaskStatus::InProgress => "in_progress",
        TaskStatus::ReviewRequired => "review_required",
        TaskStatus::Complete => "complete",
        TaskStatus::Cancelled => "cancelled",
    }
}

/// Wire word for an `AuthorType` (`user` / `agent`).
fn status_word_author(author_type: AuthorType) -> &'static str {
    match author_type {
        AuthorType::User => "user",
        AuthorType::Agent => "agent",
    }
}

// Core domain service modules (§3.1).
pub mod notes {}
pub mod tasks {}
pub mod comments {}
pub mod workspace {}
pub mod agent {}
pub mod git {}
pub mod pr {}
pub mod script {}
pub mod file {}
pub mod event {}

// Agent-Ecosystem modules (§18).
mod instructions;
mod mcp_oauth;
mod mcp_servers;
mod memories;
mod rules;
mod specialists;

// Code Changes Review modules (§17).
mod accept_changes;
pub mod diffs;
pub mod file_tracking;
mod file_tracking_ops;
pub mod metrics;

// Integrations & Ops modules (§19).
pub mod token_usage;
pub mod session_stats {}

/// Worktree setup-script detection and template generation (PROTOCOL §5.25).
/// Ports the reference `setup-scripts.ipc.ts` detection + template logic, with
/// the package-manager-specific `ProjectType` collapsed to the coarse protocol
/// enum (`node`/`python`/`go`/`rust`/`ruby`).
pub mod setup_scripts {
    use std::path::Path;

    use intent_core::{now_epoch_ms, ProjectType, SetupScript, SetupScriptGeneratedBy};

    /// Wrap a hand-authored script body into a `SetupScript` stamped
    /// `generatedBy: "user"` with a fresh `updatedAt` (used by create/update and
    /// `saveSetupScript`).
    pub fn user_script(script: String) -> SetupScript {
        SetupScript {
            script,
            project_type: None,
            updated_at: now_epoch_ms(),
            generated_by: Some(SetupScriptGeneratedBy::User),
        }
    }

    /// Classify the project rooted at `dir` from its manifest files (§5.25),
    /// mirroring the reference detection precedence (Node → Python → Go → Rust →
    /// Ruby, later checks win). Returns `None` when no known manifest is present.
    pub fn detect(dir: &Path) -> Option<ProjectType> {
        let has = |name: &str| dir.join(name).exists();
        let mut detected = None;
        if has("package.json") {
            detected = Some(ProjectType::Node);
        }
        if has("requirements.txt") || has("pyproject.toml") {
            detected = Some(ProjectType::Python);
        }
        if has("go.mod") {
            detected = Some(ProjectType::Go);
        }
        if has("Cargo.toml") {
            detected = Some(ProjectType::Rust);
        }
        if has("Gemfile") {
            detected = Some(ProjectType::Ruby);
        }
        detected
    }

    /// Render the deterministic template body for a (possibly absent) project
    /// type, mirroring the reference per-type templates (env-file copy from
    /// `$MAIN_CHECKOUT` + dependency install). The generic fallback copies common
    /// config files only.
    pub fn template_for(project_type: Option<ProjectType>) -> String {
        const HEADER: &str = "#!/usr/bin/env bash\nset -euo pipefail\n# Available variables: \
            $MAIN_CHECKOUT, $WORKTREE_PATH, $BRANCH_NAME, $SOURCE_BRANCH\n\n";
        const COPY_ENV: &str = "# Copy environment files from the main checkout\n\
            for envfile in .env .env.local .env.development .env.development.local; do\n  \
            if [ -f \"$MAIN_CHECKOUT/$envfile\" ]; then\n    \
            cp \"$MAIN_CHECKOUT/$envfile\" \"./$envfile\"\n    echo \"Copied $envfile\"\n  fi\ndone\n\n";
        let install = match project_type {
            Some(ProjectType::Node) => "echo \"Installing dependencies...\"\nnpm install\n",
            Some(ProjectType::Python) => {
                "echo \"Creating virtual environment...\"\npython3 -m venv venv\n\
                 source venv/bin/activate\nif [ -f requirements.txt ]; then\n  \
                 pip install -r requirements.txt\nfi\n"
            }
            Some(ProjectType::Go) => "echo \"Downloading Go modules...\"\ngo mod download\n",
            Some(ProjectType::Rust) => "echo \"Fetching Cargo dependencies...\"\ncargo fetch\n",
            Some(ProjectType::Ruby) => "echo \"Installing gems...\"\nbundle install\n",
            None => "echo \"Config files copied\"\n",
        };
        format!("{HEADER}{COPY_ENV}{install}")
    }

    /// Build the AI-assisted draft `SetupScript` for `project_type`, stamped
    /// `generatedBy: "agent"` (returned, not persisted — §5.25).
    pub fn generate(project_type: Option<ProjectType>) -> SetupScript {
        SetupScript {
            script: template_for(project_type),
            project_type,
            updated_at: now_epoch_ms(),
            generated_by: Some(SetupScriptGeneratedBy::Agent),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::fs;

        fn tmp() -> std::path::PathBuf {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!("setup-script-{}-{n}", std::process::id()));
            fs::create_dir_all(&dir).unwrap();
            dir
        }

        #[test]
        fn detect_returns_none_without_manifest() {
            let dir = tmp();
            assert_eq!(detect(&dir), None);
            fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn detect_classifies_each_manifest() {
            for (file, expected) in [
                ("package.json", ProjectType::Node),
                ("requirements.txt", ProjectType::Python),
                ("pyproject.toml", ProjectType::Python),
                ("go.mod", ProjectType::Go),
                ("Cargo.toml", ProjectType::Rust),
                ("Gemfile", ProjectType::Ruby),
            ] {
                let dir = tmp();
                fs::write(dir.join(file), "x").unwrap();
                assert_eq!(detect(&dir), Some(expected), "{file}");
                fs::remove_dir_all(&dir).ok();
            }
        }

        #[test]
        fn detect_precedence_rust_over_node() {
            let dir = tmp();
            fs::write(dir.join("package.json"), "{}").unwrap();
            fs::write(dir.join("Cargo.toml"), "[package]").unwrap();
            assert_eq!(detect(&dir), Some(ProjectType::Rust));
            fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn generate_stamps_agent_and_uses_template() {
            let s = generate(Some(ProjectType::Rust));
            assert_eq!(s.project_type, Some(ProjectType::Rust));
            assert_eq!(s.generated_by, Some(SetupScriptGeneratedBy::Agent));
            assert!(s.script.contains("cargo fetch"));
            assert!(s.script.starts_with("#!/usr/bin/env bash"));
        }

        #[test]
        fn user_script_stamps_user() {
            let s = user_script("echo hi".to_string());
            assert_eq!(s.generated_by, Some(SetupScriptGeneratedBy::User));
            assert_eq!(s.project_type, None);
            assert_eq!(s.script, "echo hi");
        }
    }
}

/// Iterate `![alt](url)` matches in `content` (markdown image syntax) and
/// return the URL portion of each match in document order. Malformed
/// syntax is skipped rather than raised so a note with mixed markdown
/// cannot break note-image resolution.
fn extract_markdown_image_urls(content: &str) -> Vec<String> {
    // Hand-rolled scan (avoids pulling in the `regex` crate here): match
    // literal `![`, read the alt text up to the closing `]`, require an
    // immediate `(`, then capture up to the matching `)`.
    let bytes = content.as_bytes();
    let mut urls: Vec<String> = Vec::new();
    let mut i = 0;
    while i + 3 < bytes.len() {
        if bytes[i] == b'!' && bytes[i + 1] == b'[' {
            let mut j = i + 2;
            while j < bytes.len() && bytes[j] != b']' {
                j += 1;
            }
            if j >= bytes.len() || j + 1 >= bytes.len() || bytes[j + 1] != b'(' {
                i += 1;
                continue;
            }
            let mut k = j + 2;
            while k < bytes.len() && bytes[k] != b')' {
                k += 1;
            }
            if k >= bytes.len() {
                break;
            }
            if let Ok(url) = std::str::from_utf8(&bytes[j + 2..k]) {
                if !url.is_empty() {
                    urls.push(url.to_string());
                }
            }
            i = k + 1;
        } else {
            i += 1;
        }
    }
    urls
}

/// Provider discovery with npx version probing (host.providerDiscovery support).
/// Returns JSON payload with providers array and npx status.
pub fn discover_providers_with_npx() -> serde_json::Value {
    let providers = intent_providers::discover_providers();
    let npx_status = intent_providers::probe_npx();

    // Probe npx version if we found the binary
    let (version, version_ok) = if let Some(ref npx_path) = npx_status.resolved_path {
        match probe_npx_version_internal(npx_path) {
            Some(v) => {
                let ok = parse_npx_version_ok(&v);
                (Some(v), ok)
            }
            None => (None, false),
        }
    } else {
        (None, false)
    };

    let provider_array: Vec<serde_json::Value> = providers
        .iter()
        .map(|p| {
            let mut obj = serde_json::Map::new();
            obj.insert("id".to_string(), serde_json::json!(p.id));
            obj.insert("displayName".to_string(), serde_json::json!(p.display_name));
            obj.insert("command".to_string(), serde_json::json!(p.command));
            obj.insert("installed".to_string(), serde_json::json!(p.installed));
            if let Some(ref path) = p.resolved_path {
                obj.insert(
                    "resolvedPath".to_string(),
                    serde_json::json!(path.display().to_string()),
                );
            }
            if let Some(ref reason) = p.gated_off {
                obj.insert("gatedOff".to_string(), serde_json::json!(reason));
            }
            obj.insert(
                "hasNpxFallback".to_string(),
                serde_json::json!(p.has_npx_fallback),
            );
            serde_json::Value::Object(obj)
        })
        .collect();

    let mut npx_obj = serde_json::Map::new();
    if let Some(ref path) = npx_status.resolved_path {
        npx_obj.insert(
            "resolvedPath".to_string(),
            serde_json::json!(path.display().to_string()),
        );
    } else {
        npx_obj.insert("resolvedPath".to_string(), serde_json::Value::Null);
    }
    if let Some(ref v) = version {
        npx_obj.insert("version".to_string(), serde_json::json!(v));
    } else {
        npx_obj.insert("version".to_string(), serde_json::Value::Null);
    }
    npx_obj.insert("versionOk".to_string(), serde_json::json!(version_ok));

    serde_json::json!({
        "providers": provider_array,
        "npx": serde_json::Value::Object(npx_obj),
    })
}

fn probe_npx_version_internal(npx_path: &Path) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    let mut child = Command::new(npx_path)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    let timeout = Duration::from_secs(3);
    let start = std::time::Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return None;
                }
                let mut stdout_handle = child.stdout.take()?;
                let mut output = Vec::new();
                stdout_handle.read_to_end(&mut output).ok()?;
                let stdout = String::from_utf8_lossy(&output);
                let first_line = stdout.lines().next()?.trim();
                if first_line.is_empty() {
                    return None;
                }
                return Some(first_line.to_string());
            }
            Ok(None) => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return None,
        }
    }
}

fn parse_npx_version_ok(version_str: &str) -> bool {
    version_str
        .split('.')
        .next()
        .and_then(|major| major.parse::<u32>().ok())
        .is_some_and(|maj| maj >= 7)
}
