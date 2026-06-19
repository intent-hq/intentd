//! intent-services — the shared business-logic surface (§3.1).
//!
//! Depends on core, store, git, sourcecontrol, acp, context, providers, pty,
//! and search (§3.2). Sibling feature modules never import each other; they
//! communicate through the store and the event bus (§3.2 rule 4). This slice
//! implements the read-only `WorkspaceApi` surface (`workspace.list` /
//! `note.list`) over `intent-store`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use base64::Engine as _;
use intent_core::events::{
    COMMENT_ADDED, NOTE_CREATED, NOTE_DELETED, NOTE_UPDATED, PR_LINKED, PR_UNLINKED, PR_UPDATED,
    TASK_STATUS_CHANGED, WORKSPACE_ATTENTION_CHANGED,
};
use intent_core::{
    iso_minutes_ago, now_iso, parse_iso, ActorType, AgentDelegateInput, AgentId, AgentLite,
    AuthorType, BoxFuture, Comment, CommentAddResult, CommentAnchor, CommentAnchorType,
    CommentDeleteResult, CommentGetThreadResult, CommentListResult, CommentLocation,
    CommentRespondResult, CommentRespondThread, CommentStatus, CommentThreadSummary, CommentType,
    CommentWire, ContentType, Event, EventQueryParams, EventSubscribeResult,
    EventUnsubscribeResult, FileActivity, Note, NoteAddInput, NoteAddResult, NoteCreate,
    NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult,
    NoteId, NoteSetContentResult, NoteTaskRow, NoteUpdateInput, NoteUpdateMetadataResult,
    NoteVisibility, ReadAssetResult, TaskAssignAgentResult, TaskConvertBlocksResult,
    TaskCreatePrerequisiteResult, TaskGetMyTaskResult, TaskMarkAsTaskResult, TaskMetadata,
    TaskStatus, TaskSubtask, TaskUpdateNoteStatusResult, TaskUpdateResult, TaskUpdateStatusResult,
    Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceCreate, WorkspaceEventSummary,
    WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
};
use intent_store::{EventQuery, NewEvent, Store};

pub use intent_core::{Error, Result, WorkspaceApi};

mod agent_manager;
mod agent_ops;
mod agent_session;
mod event_ops;
pub mod events;
mod git_ops;
mod note_ops;
mod pr_ops;

#[cfg(test)]
mod tests;

pub use agent_manager::{
    compute_process_cap, default_process_cap, AgentManager, BusEventSink, ProcessRegistry,
};
pub use events::{EventBus, FileWatcher, Subscription, SubscriptionFilter};
pub use pr_ops::PrRefreshOutcome;

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
            agent_manager: Arc::new(OnceLock::new()),
            source_control: None,
        }
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

    /// Configure the note-asset root directory (for `note.readAsset`).
    pub fn with_assets_root(mut self, root: PathBuf) -> Self {
        self.assets_root = Some(root);
        self
    }

    /// Wire the event bus so CRUD mutations publish change events (§10). The bus
    /// must share the same [`Store`] as this services handle so the broadcast and
    /// the durable log stay consistent.
    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        self.event_bus = Some(bus);
        self
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
        use pr_ops::PrRefreshOutcome;

        let mut ws = self.store.get_workspace(workspace_id).await?;
        if ws.is_remote || ws.archived {
            return Ok(PrRefreshOutcome::Skipped);
        }
        let (owner, repo) = match pr_ops::repo_of(&ws) {
            Ok(pair) => pair,
            Err(_) => return Ok(PrRefreshOutcome::Skipped),
        };
        let sc = pr_ops::resolve_source_control(self.source_control.clone())?;
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
                    .map_err(pr_ops::map_sc_err)?;
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

    /// Refresh every workspace that already has a linked PR (discovery stays
    /// on-demand). Errors are logged per workspace and never abort the sweep.
    async fn refresh_all_linked_prs(&self) {
        let workspaces = match self.store.list_workspaces(false).await {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!(error = %e, "pr refresh: listing workspaces failed");
                return;
            }
        };
        for ws in workspaces {
            if ws.pr_number.is_none() {
                continue;
            }
            if let Err(e) = self.refresh_workspace_pr(&ws.id).await {
                tracing::warn!(
                    workspace = %ws.id.as_str(),
                    error = %e,
                    "pr refresh: workspace refresh failed"
                );
            }
        }
    }

    /// Spawn the background PR refresh loop (§7.6): every `interval` it refreshes
    /// all linked PRs, persisting deltas and emitting `pr:*` events. The first
    /// sweep runs after one `interval`. Missed ticks are skipped (no pile-up).
    /// No-op-safe when source control is unconfigured (each refresh surfaces the
    /// missing-provider error, which is logged and swallowed). Returns the task
    /// handle so the composition root can hold/abort it.
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
                services.refresh_all_linked_prs().await;
            }
        })
    }
}

/// Fetch a note scoped to `workspace_id`; `NotFound` if absent or in another
/// workspace. Used by the CRUD `note.get`/`note.update` paths.
async fn fetch_note(store: &Store, workspace_id: &WorkspaceId, note_id: &NoteId) -> Result<Note> {
    match store.get_note(note_id).await {
        Ok(n) if &n.workspace_id == workspace_id => Ok(n),
        Ok(_) | Err(Error::NotFound(_)) => Err(Error::NotFound(format!("note {note_id}"))),
        Err(e) => Err(e),
    }
}

/// Like [`fetch_note`] but surfaces the TS peer's `Note not found: <id>` message
/// as [`Error::Internal`] (→ `-32603`), matching the `ws.note.*` edit peers.
async fn fetch_note_peer(
    store: &Store,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
) -> Result<Note> {
    match store.get_note(note_id).await {
        Ok(n) if &n.workspace_id == workspace_id => Ok(n),
        Ok(_) | Err(Error::NotFound(_)) => {
            Err(Error::Internal(format!("Note not found: {note_id}")))
        }
        Err(e) => Err(e),
    }
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
fn system_actor() -> intent_core::EventActor {
    intent_core::EventActor {
        actor_type: ActorType::System,
        id: Some("system".to_string()),
        name: Some("System".to_string()),
        ..Default::default()
    }
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
        data: serde_json::json!({
            "noteId": note_id.as_str(),
            "title": title,
            "action": action,
        }),
    }
}

/// Build a `task:status-changed` change event with the TS-parity payload
/// `{ noteId, noteTitle, previousStatus, newStatus, changedAt }` (the system
/// actor leaves `agentId` undefined, so it is omitted) (`notes.service.ts`).
fn task_status_changed_event(
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    note_title: &str,
    previous_status: TaskStatus,
    new_status: TaskStatus,
    changed_at: &str,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: TASK_STATUS_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        data: serde_json::json!({
            "noteId": note_id.as_str(),
            "noteTitle": note_title,
            "previousStatus": status_word(previous_status),
            "newStatus": status_word(new_status),
            "changedAt": changed_at,
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
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "attention": attention,
        }),
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
        data: serde_json::json!({
            "noteId": note_id.as_str(),
            "commentId": comment_id,
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
        data: serde_json::json!({ "workspaceId": workspace_id.as_str() }),
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

impl Services {
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
            task: Some(fresh_task_metadata(status, &now, peer_order)),
            created_at: now.clone(),
            updated_at: now,
        };
        self.store.insert_note(&note).await?;
        Ok(note)
    }
}

impl WorkspaceApi for Services {
    fn list_workspaces(&self, include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        let store = self.store.clone();
        Box::pin(async move { store.list_workspaces(include_archived).await })
    }

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move { store.get_workspace(&id).await })
    }

    fn create_workspace(&self, input: WorkspaceCreate) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let now = now_iso();
            let id = WorkspaceId::new();
            // The branch defaults to the workspace id, mirroring the TS service.
            let branch = input.branch.unwrap_or_else(|| id.0.clone());
            let ws = Workspace {
                id,
                title: input.title.unwrap_or_default(),
                branch,
                base_ref: input.base_ref,
                base_commit_sha: input.base_commit_sha,
                status: WorkspaceStatus::Active,
                status_message: input.status_message,
                // Derived, read-only; never persisted (§9.9).
                activity: WorkspaceActivity::Idle,
                attention: WorkspaceAttention::None,
                created_at: now.clone(),
                updated_at: now,
                last_activity: None,
                tags: input.tags.unwrap_or_default(),
                path: input.path,
                repository_owner: input.repository_owner,
                repository_name: input.repository_name,
                worktree_path: input.worktree_path,
                scope: input.scope,
                skip_worktree: input.skip_worktree.unwrap_or(false),
                setup_script: input.setup_script,
                is_remote: input.is_remote.unwrap_or(false),
                default_model: input.default_model,
                pr_number: None,
                pr_url: None,
                pr_status: None,
                active_pull_request: None,
                archived: false,
                archived_at: None,
            };
            store.insert_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            if let Some(v) = update.title {
                ws.title = v;
            }
            if let Some(v) = update.status_message {
                ws.status_message = Some(v);
            }
            if let Some(v) = update.branch {
                ws.branch = v;
            }
            if let Some(v) = update.base_ref {
                ws.base_ref = Some(v);
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
                ws.setup_script = Some(v);
            }
            if let Some(v) = update.is_remote {
                ws.is_remote = v;
            }
            if let Some(v) = update.default_model {
                ws.default_model = Some(v);
            }
            if let Some(v) = update.pr_number {
                ws.pr_number = Some(v);
            }
            if let Some(v) = update.pr_url {
                ws.pr_url = Some(v);
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
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        let store = self.store.clone();
        Box::pin(async move { store.delete_workspace(&id).await })
    }

    fn archive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            let now = now_iso();
            ws.status = WorkspaceStatus::Archived;
            ws.archived = true;
            ws.archived_at = Some(now.clone());
            ws.updated_at = now;
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        Box::pin(async move {
            let mut ws = store.get_workspace(&id).await?;
            ws.status = WorkspaceStatus::Active;
            ws.archived = false;
            ws.archived_at = None;
            ws.updated_at = now_iso();
            store.update_workspace(&ws).await?;
            Ok(ws)
        })
    }

    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
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

    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let store = self.store.clone();
        let id = workspace_id.clone();
        Box::pin(async move { store.list_notes(&id).await })
    }

    fn get_note(&self, workspace_id: WorkspaceId, note_id: NoteId) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        Box::pin(async move { fetch_note(&store, &workspace_id, &note_id).await })
    }

    fn create_note(
        &self,
        workspace_id: WorkspaceId,
        input: NoteCreate,
    ) -> BoxFuture<'_, Result<Note>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
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
                task: None,
                created_at: now.clone(),
                updated_at: now,
            };
            store.insert_note(&note).await?;
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
        Box::pin(async move {
            let mut note = fetch_note(&store, &workspace_id, &note_id).await?;
            // content present → raw full set; otherwise title/tags metadata.
            if let Some(content) = input.content {
                note.content = content;
            } else {
                if let Some(title) = input.title {
                    note.title = title;
                }
                if let Some(tags) = input.tags {
                    note.tags = tags;
                }
            }
            note.updated_at = now_iso();
            store.update_note(&note).await?;
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
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let (new_content, position) = note_ops::apply_add(
                &old_content,
                &input.content,
                input.heading.as_deref(),
                input.position.as_deref(),
            )?;
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
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
            Ok(NoteAddResult {
                ok: true,
                note_id: note.id,
                added_length: input.content.chars().count(),
                total_length: new_content.chars().count(),
                position,
                old_content,
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn edit_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
    ) -> BoxFuture<'_, Result<NoteEditResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
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
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
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
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn edit_note_lines(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
    ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let mut note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            let old_content = note.content.clone();
            let new_content =
                note_ops::apply_edit_lines(&old_content, input.start, input.end, &input.content)?;
            let total_lines_before = old_content.split('\n').count();
            let total_lines_after = new_content.split('\n').count();
            note.content = new_content.clone();
            note.updated_at = now_iso();
            store.update_note(&note).await?;
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
                new_content,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn set_note_content(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        confirm_replacement: bool,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
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
            let clean = note_ops::clean_set_content(&content)?;
            note.content = clean.clone();
            let now = now_iso();
            note.updated_at = now.clone();
            store.update_note(&note).await?;
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
                updated_at: now,
                old_content: Some(old_content),
                new_content: clean,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
            })
        })
    }

    fn update_note_metadata(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
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
            store.update_note(&note).await?;
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
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            // Scope-check first so a foreign/absent note yields the peer message.
            let note = fetch_note_peer(&store, &workspace_id, &note_id).await?;
            store.delete_note(&note_id).await?;
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

    fn task_update_status(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        task_text: String,
        status: String,
    ) -> BoxFuture<'_, Result<TaskUpdateStatusResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
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
    ) -> BoxFuture<'_, Result<TaskUpdateNoteStatusResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
            let new_status = parse_task_status_strict(&status)?;
            let mut note = fetch_note(&store, &workspace_id, &note_id).await?;
            let mut task = match note.task.clone() {
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
            note.task = Some(task);
            note.updated_at = now.clone();
            store.update_note(&note).await?;
            // Mirror `notes.service.ts`: emit only when the status actually changed.
            if previous_status != new_status {
                publish_event(
                    &bus,
                    task_status_changed_event(
                        &note.workspace_id,
                        &note.id,
                        &note.title,
                        previous_status,
                        new_status,
                        &now,
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

    fn get_my_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
    ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            let note = match store.get_note(&task_note_id).await {
                Ok(n) if n.workspace_id == workspace_id => n,
                Ok(_) | Err(Error::NotFound(_)) => {
                    return Err(Error::Internal("Task note not found".to_string()))
                }
                Err(e) => return Err(e),
            };
            let task = match note.task.clone() {
                Some(t) => t,
                None => return Err(Error::Internal("Note is not a task".to_string())),
            };
            let all = store.list_notes(&workspace_id).await?;
            let subtasks = all
                .iter()
                .filter(|n| n.parent_id.as_ref() == Some(&note.id) && n.task.is_some())
                .map(|n| TaskSubtask {
                    id: n.id.clone(),
                    title: n.title.clone(),
                    status: n
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
            match note.task.clone() {
                // Already a task with a changing status → preserve other fields.
                Some(mut existing) if existing.status != new_status => {
                    apply_status_transition(&mut existing, new_status, &now);
                    note.task = Some(existing);
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
                    note.task = Some(task);
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
    ) -> BoxFuture<'_, Result<TaskConvertBlocksResult>> {
        let services = self.clone();
        Box::pin(async move {
            let store = &services.store;
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
                        let child = services
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
            Ok(TaskConvertBlocksResult {
                ok: true,
                converted_count: created_note_ids.len() as i64,
                created_note_ids,
            })
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
            match store.get_note(&dependent_note_id).await {
                Ok(n) if n.workspace_id == workspace_id => {}
                Ok(_) | Err(Error::NotFound(_)) => {
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
        Box::pin(async move {
            if !is_valid_agent_id(&agent_id) {
                return Err(Error::Internal(format!(
                    "Invalid agentId format: \"{agent_id}\". Agent IDs must be in format \"agent-{{uuid}}\" (e.g., \"agent-b0a8044a-5eac-4b52-8456-15d3b784decb\"). To create a new agent and assign it to this task, use create_agent with taskNoteId=\"{note_id}\" instead."
                )));
            }
            let mut note = match store.get_note(&note_id).await {
                Ok(n) if n.workspace_id == workspace_id => n,
                Ok(_) | Err(Error::NotFound(_)) => {
                    return Err(Error::Internal(format!("Note not found: {note_id}")))
                }
                Err(e) => return Err(e),
            };
            let mut task = match note.task.clone() {
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
            if !already_assigned {
                task.assigned_agent_ids.push(agent.clone());
            }
            if should_update_status {
                apply_status_transition(&mut task, TaskStatus::InProgress, &now);
            }
            note.task = Some(task);
            note.updated_at = now;
            store.update_note(&note).await?;
            Ok(TaskAssignAgentResult {
                ok: true,
                note_id,
                agent_id: agent,
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
    ) -> BoxFuture<'_, Result<CommentAddResult>> {
        let store = self.store.clone();
        let bus = self.event_bus.clone();
        Box::pin(async move {
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
            let (from, to, line) =
                note_ops::find_and_anchor_text(&note.content, &search_context, &comment_target)?;
            let anchored_text = note.content[from..to].to_string();
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
                anchor_before: None,
                anchor_after: None,
                suggestion_original: None,
                suggestion_proposed: None,
                agent_id: None,
                created_at: now.clone(),
                updated_at: now,
            };
            store.insert_comment(&new_comment).await?;
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
            let all = store.list_comments(&note_id).await?;
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
                note_id: Some(note_id),
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
                created_at: now.clone(),
                updated_at: now,
            };
            store.insert_comment(&reply).await?;
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
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        comment_id: String,
    ) -> BoxFuture<'_, Result<CommentDeleteResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            match store.delete_comment(&comment_id).await {
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
    ) -> BoxFuture<'_, Result<Vec<Event>>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Mirror `buildQueryFilters`: each option is applied only when
            // truthy (empty strings / 0 are skipped); `limit || 50`.
            let mut q = EventQuery {
                workspace_id: Some(workspace_id),
                limit: Some(params.limit.filter(|&l| l != 0).unwrap_or(50)),
                ..Default::default()
            };
            if let Some(t) = params.event_type.filter(|s| !s.is_empty()) {
                q.event_types = vec![t];
            }
            if let Some(at) = params.actor_type.filter(|s| !s.is_empty()) {
                // An unrecognized actorType matches nothing (TS equals filter).
                match serde_json::from_value::<ActorType>(serde_json::Value::String(at)) {
                    Ok(parsed) => q.actor_type = Some(parsed),
                    Err(_) => return Ok(Vec::new()),
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
            store.query_events(&q).await
        })
    }

    fn event_subscribe(
        &self,
        _workspace_id: WorkspaceId,
        event_types: Vec<String>,
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

    fn git_get_branches(
        &self,
        repo_path: String,
        include_remote: bool,
    ) -> BoxFuture<'_, Result<intent_core::GitBranches>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Validate against known repos (archived included) to prevent
            // arbitrary filesystem access, matching the TS registry check.
            let workspaces = store.list_workspaces(true).await?;
            if !git_ops::is_known_repo(&workspaces, &repo_path) {
                return Err(Error::InvalidParams(
                    "Unknown or unauthorized repository path".to_string(),
                ));
            }
            intent_git::branches::get_branches(std::path::Path::new(&repo_path), include_remote)
        })
    }

    fn git_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
    ) -> BoxFuture<'_, Result<intent_core::GitCommitResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            // TS `ws.git.commit` gates on auto-commit (no userRequested bypass).
            git_ops::assert_agent_commit_allowed(false)?;
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
            Ok(intent_core::GitCommitResult {
                hash: outcome.hash,
                files: outcome.files,
            })
        })
    }

    fn git_agent_commit(
        &self,
        workspace_id: WorkspaceId,
        message: String,
        files: Option<Vec<String>>,
        user_requested: bool,
    ) -> BoxFuture<'_, Result<intent_core::GitAgentCommitResult>> {
        let store = self.store.clone();
        Box::pin(async move {
            // userRequested bypasses the auto-commit gate (TS parity).
            git_ops::assert_agent_commit_allowed(user_requested)?;
            let ws = store
                .get_workspace(&workspace_id)
                .await
                .map_err(|e| Error::Internal(format!("Failed to commit: {e}")))?;
            let worktree = git_ops::worktree_path(&ws).ok_or_else(|| {
                Error::Internal("Failed to commit: workspace has no worktree".to_string())
            })?;
            // PARITY NOTE: TS filters to the agent's own unstaged changes via the
            // file-tracking attribution pipeline (deferred — not yet ported). Until
            // it lands, an explicit `files` list is committed as-is; otherwise we
            // fall back to every changed path in the worktree.
            let to_commit = match files {
                Some(f) if !f.is_empty() => f,
                _ => intent_git::commit::all_changed_paths(&worktree)?,
            };
            if to_commit.is_empty() {
                return Err(Error::Internal(
                    "No uncommitted changes found for this agent".to_string(),
                ));
            }
            intent_git::stage::stage(&worktree, &to_commit)?;
            let outcome = intent_git::commit::commit(&worktree, &message)?;
            let file_count = to_commit.len() as i64;
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

    // ========================================================================
    // agent.* surface (PROTOCOL §5.5). Store/in-memory-backed; the live-runtime
    // coupling (spawning a turn from sendMessage, flipping `queued` mid-stream)
    // lands with the end-to-end orchestration flow. Helpers live in `agent_ops`.
    // ========================================================================

    fn agent_delegate(
        &self,
        workspace_id: WorkspaceId,
        input: AgentDelegateInput,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_delegate_op(workspace_id, input).await })
    }

    fn agent_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        Box::pin(async move { self.agent_list_op(workspace_id).await })
    }

    fn agent_get(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<AgentLite>> {
        Box::pin(async move { self.agent_get_op(agent_id).await })
    }

    fn agent_get_conversation(
        &self,
        agent_id: AgentId,
        limit: Option<i64>,
        _workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_get_conversation_op(agent_id, limit).await })
    }

    fn agent_create(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        _specialist_id: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_create_op(workspace_id, name, model).await })
    }

    fn agent_send_to_task(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        message: String,
        _priority: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_send_to_task_op(workspace_id, task_note_id, message)
                .await
        })
    }

    fn agent_send_message(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        content: String,
        message_id: Option<String>,
        _image_blocks: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // When the runtime manager is attached, drive a real spawn/turn loop;
            // otherwise fall back to the store-only persist (read-only wiring).
            match self.agent_manager() {
                Some(manager) => {
                    manager
                        .send_message(agent_id, workspace_id, content, message_id)
                        .await
                }
                None => {
                    self.agent_send_message_op(agent_id, content, message_id)
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
        _image_blocks: Option<serde_json::Value>,
        _note_ids: Option<serde_json::Value>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            match self.agent_manager() {
                Some(manager) => {
                    manager
                        .force_message(agent_id, workspace_id, message_id, content)
                        .await
                }
                None => {
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
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_queue_message_op(agent_id, content, image_blocks)
                .await
        })
    }

    fn agent_edit_queued_message(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_edit_queued_message_op(agent_id, message_id, content)
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

    fn agent_get_queue(&self, agent_id: AgentId) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_get_queue_op(agent_id).await })
    }

    fn agent_stop(&self, agent_id: AgentId) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            // Cancel the in-flight stream + kill the child via the manager when
            // attached; the result is `{ success: true }` either way (§5.5).
            if let Some(manager) = self.agent_manager() {
                manager.stop(&agent_id).await;
            }
            Ok(serde_json::json!({ "success": true }))
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

    fn agent_rename(
        &self,
        agent_id: AgentId,
        name: String,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_rename_op(agent_id, name).await })
    }

    fn agent_delete(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move { self.agent_delete_op(agent_id).await })
    }

    fn agent_wake_or_create(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        context_message: String,
        model: Option<String>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            self.agent_wake_or_create_op(workspace_id, task_note_id, context_message, model)
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
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let _ = (workspace_id, report);
            // No agent-caller context over the RPC dispatch path → never a
            // delegated agent (TS surfaces this as -32603).
            Err(Error::Internal(
                "report_to_parent is only available to delegated agents".to_string(),
            ))
        })
    }

    fn agent_get_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let _ = (workspace_id, agent_id);
            Ok(serde_json::json!({
                "subscriptions": [],
                "delegationGroups": [],
                "agentStatuses": {},
            }))
        })
    }

    fn agent_cancel_subscriptions(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            let _ = (workspace_id, agent_id);
            Ok(serde_json::json!({ "success": true }))
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
            let repo_ref = intent_sourcecontrol::RepoRef::new(owner, repo);
            match sc.get_review_threads(&repo_ref, number).await {
                Ok(mut threads) => {
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
                        .list_review_comments(&repo_ref, number)
                        .await
                        .map_err(pr_ops::map_sc_err)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        let injected = self.source_control.clone();
        Box::pin(async move {
            let method = pr_ops::validate_merge_method(merge_method)?;
            let ws = load_ws_for_pr(&store, &workspace_id).await?;
            let (owner, repo) = pr_ops::repo_of(&ws)?;
            let number = pr_ops::active_pr_number(&ws)?;
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
            let sc = pr_ops::resolve_source_control(injected)?;
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
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        let store = self.store.clone();
        Box::pin(async move {
            // Default 50 (TS), clamp to the PROTOCOL §5.19 cap of 200.
            let limit = limit.unwrap_or(50).clamp(0, 200) as usize;
            let empty = serde_json::json!({ "commits": [] });
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
            let commits = intent_git::history::history(&worktree, limit)?;
            let values: Vec<serde_json::Value> = commits
                .iter()
                .map(file_tracking_ops::commit_to_value)
                .collect();
            Ok(serde_json::json!({ "commits": values }))
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
pub mod drafts {} // §9.10

// Agent-Ecosystem modules (§18).
pub mod rules {}
pub mod specialists {}
pub mod mcp_servers {}
pub mod memories {}

// Code Changes Review modules (§17).
pub mod diffs;
pub mod file_tracking;
mod file_tracking_ops;
pub mod accept_changes {}
pub mod metrics;

// Integrations & Ops modules (§19).
pub mod token_usage {}
pub mod session_stats {}
pub mod setup_scripts {}
