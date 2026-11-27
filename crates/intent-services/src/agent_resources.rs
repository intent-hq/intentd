//! What each agent lifecycle event does to the resources an agent owns.
//!
//! Every per-agent resource implements [`AgentResource`]. Its methods have
//! no default bodies, so a new resource must state what it does on every
//! lifecycle event, and a new event must be handled by every resource.
//! Before this registry each lifecycle path cleaned up by hand, and the
//! paths drifted: agent temp folders went on delete only and stayed on disk
//! after a retire or an archive (intent-hq/intent#9359).
//!
//! Each lifecycle path calls the matching [`Services`] method below once,
//! after its state transition commits. Each [`AgentResource`] method states
//! what is already true when it runs (runtime stopped, fences held). Steps
//! that gate or must precede the transition stay in the paths:
//! script-monitor cancels (their failure aborts the transition), desktop
//! control, remote final stops and the runtime stop itself.

use std::collections::BTreeMap;
use std::path::PathBuf;

use intent_core::{AgentId, AgentTmpRemoval, HookId, PrMonitorId, WorkspaceId};

use crate::Services;

/// A deleted agent. `workspace_id` is `None` when its session row was
/// already gone (the read found no row): its leftovers are still released.
#[derive(Clone, Copy)]
pub(crate) struct DeletedAgent<'a> {
    pub workspace_id: Option<&'a WorkspaceId>,
    pub agent_id: &'a AgentId,
}

/// A deleted workspace and the agents it held.
#[derive(Clone, Copy)]
pub(crate) struct DeletedWorkspace<'a> {
    pub workspace_id: &'a WorkspaceId,
    pub agent_ids: &'a [AgentId],
}

/// The archive sweep. Handlers record what they cancelled or moved aside;
/// the archive path then sends one consolidated notice per owner and
/// deletes the moved-aside folders after its `workspace:updated` delta.
pub(crate) struct ArchiveSweep {
    pub workspace_id: WorkspaceId,
    /// The `archivedAt` this archive wrote: a handler that must not act on
    /// a workspace unarchived meanwhile compares it with the row.
    pub archived_at: Option<String>,
    pub cancelled_hooks: BTreeMap<AgentId, Vec<(String, HookId)>>,
    pub cancelled_monitors: BTreeMap<AgentId, Vec<(String, PrMonitorId)>>,
    pub detached_tmp_dirs: Vec<PathBuf>,
}

impl ArchiveSweep {
    pub(crate) fn new(workspace_id: WorkspaceId, archived_at: Option<String>) -> Self {
        Self {
            workspace_id,
            archived_at,
            cancelled_hooks: BTreeMap::new(),
            cancelled_monitors: BTreeMap::new(),
            detached_tmp_dirs: Vec::new(),
        }
    }
}

/// A resource an agent owns, and what each lifecycle event does to it.
/// Every method is required: write an empty body with the reason when an
/// event leaves the resource as it is.
pub(crate) trait AgentResource {
    /// `agent.delete`: the session row is confirmed gone and the runtime is
    /// stopped. Not called when the session read failed.
    async fn agent_deleted(&self, svc: &Services, agent: DeletedAgent<'_>);
    /// `agent.retire` (and its child cascade): the retired mark is set. A
    /// wire retire has aborted the worker; a self-retire's turn may still
    /// run. Runs under the agent's retirement gate.
    async fn agent_retired(&self, svc: &Services, agent_id: &AgentId);
    /// `agent.restore`: the retired mark is cleared. Runs under the agent's
    /// retirement gate.
    async fn agent_restored(&self, svc: &Services, agent_id: &AgentId);
    /// `workspace.archive`: the Archived flip committed and in-flight turns
    /// are interrupted (provider processes stay alive). Runs on the
    /// detached archive tail, outside the archive fence.
    async fn workspace_archived(&self, svc: &Services, sweep: &mut ArchiveSweep);
    /// `workspace.unarchive` (and revive): the flip committed. Runs UNDER
    /// the archive fence, so a handler must not take it.
    async fn workspace_unarchived(&self, svc: &Services, workspace_id: &WorkspaceId);
    /// `workspace.retire`: the Retired flip committed, after the archive
    /// sweep ran. Runs before the agents' runtimes stop.
    async fn workspace_retired(&self, svc: &Services, workspace_id: &WorkspaceId);
    /// `workspace.delete`: the agents are stopped and their `agent:deleted`
    /// events are out; runs before the workspace's rows are deleted.
    async fn workspace_deleted(&self, svc: &Services, workspace: DeletedWorkspace<'_>);
}

/// Runs `$method` on every registered resource, in order. The one list of
/// per-agent resources: add a new resource here (a resource left out is
/// never constructed, which the `dead_code` lint rejects).
///
/// Order matters: the retire path's single `waiting` recompute runs after
/// the registry, and hooks and PR monitors recompute `waiting` themselves,
/// so they come before `CompletionWatches`. `TempFolders` comes last: its
/// archive handler takes the archive fence, and its removals are the slow
/// part.
macro_rules! for_each_agent_resource {
    ($svc:expr, $method:ident, $arg:expr) => {{
        MemorySecrets.$method($svc, $arg).await;
        EventSubscriptions.$method($svc, $arg).await;
        Hooks.$method($svc, $arg).await;
        PrMonitors.$method($svc, $arg).await;
        CompletionWatches.$method($svc, $arg).await;
        RuntimeMemory.$method($svc, $arg).await;
        TempFolders.$method($svc, $arg).await;
    }};
}

impl Services {
    pub(crate) async fn release_deleted_agent(&self, agent: DeletedAgent<'_>) {
        for_each_agent_resource!(self, agent_deleted, agent);
    }

    pub(crate) async fn release_retired_agent(&self, agent_id: &AgentId) {
        for_each_agent_resource!(self, agent_retired, agent_id);
    }

    pub(crate) async fn reclaim_restored_agent(&self, agent_id: &AgentId) {
        for_each_agent_resource!(self, agent_restored, agent_id);
    }

    pub(crate) async fn release_archived_workspace_agents(&self, sweep: &mut ArchiveSweep) {
        for_each_agent_resource!(self, workspace_archived, &mut *sweep);
    }

    pub(crate) async fn reclaim_unarchived_workspace_agents(&self, workspace_id: &WorkspaceId) {
        for_each_agent_resource!(self, workspace_unarchived, workspace_id);
    }

    pub(crate) async fn release_retired_workspace_agents(&self, workspace_id: &WorkspaceId) {
        for_each_agent_resource!(self, workspace_retired, workspace_id);
    }

    pub(crate) async fn release_deleted_workspace_agents(&self, workspace: DeletedWorkspace<'_>) {
        for_each_agent_resource!(self, workspace_deleted, workspace);
    }
}

/// Memory-held secrets and pending secret requests (§5.56).
struct MemorySecrets;

impl AgentResource for MemorySecrets {
    async fn agent_deleted(&self, svc: &Services, agent: DeletedAgent<'_>) {
        // After the row delete: a concurrent `ws.secret.request` either
        // inserted before this sweep or re-reads the session and drops itself.
        svc.secret_vault.clear_agent(agent.agent_id);
    }

    async fn agent_retired(&self, svc: &Services, agent_id: &AgentId) {
        svc.secret_vault.clear_agent(agent_id);
    }

    async fn agent_restored(&self, _svc: &Services, _agent_id: &AgentId) {
        // A restored agent starts with no secrets.
    }

    async fn workspace_archived(&self, svc: &Services, sweep: &mut ArchiveSweep) {
        svc.secret_vault.clear_workspace(&sweep.workspace_id);
        // Its remote agents' secrets are on their machines.
        svc.forget_remote_secrets_in(&sweep.workspace_id);
    }

    async fn workspace_unarchived(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Unarchive starts with no secrets.
    }

    async fn workspace_retired(&self, svc: &Services, workspace_id: &WorkspaceId) {
        svc.secret_vault.clear_workspace(workspace_id);
    }

    async fn workspace_deleted(&self, svc: &Services, workspace: DeletedWorkspace<'_>) {
        svc.secret_vault.clear_workspace(workspace.workspace_id);
    }
}

/// Event subscriptions (`ws.event.subscribe`).
struct EventSubscriptions;

impl AgentResource for EventSubscriptions {
    async fn agent_deleted(&self, svc: &Services, agent: DeletedAgent<'_>) {
        // The wake target is gone, so matching for it is pure leak
        // (monorepo#937).
        svc.remove_event_subscriptions_for_agent(agent.agent_id)
            .await;
    }

    async fn agent_retired(&self, svc: &Services, agent_id: &AgentId) {
        svc.remove_event_subscriptions_for_agent(agent_id).await;
    }

    async fn agent_restored(&self, _svc: &Services, _agent_id: &AgentId) {
        // Not resurrected: the agent subscribes again if it still cares.
    }

    async fn workspace_archived(&self, _svc: &Services, _sweep: &mut ArchiveSweep) {
        // Kept: their wakes park behind the archived gates until unarchive.
    }

    async fn workspace_unarchived(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Kept through the archive; nothing to restore.
    }

    async fn workspace_retired(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Kept, as for archive: a retired workspace keeps its rows.
    }

    async fn workspace_deleted(&self, svc: &Services, workspace: DeletedWorkspace<'_>) {
        // Every subscription scoped to the workspace can never match again
        // (monorepo#947). After the `agent:deleted` fan-out, so a chief
        // subscription on this workspace can still see those events.
        svc.remove_event_subscriptions_for_workspace(workspace.workspace_id)
            .await;
    }
}

/// Background hooks (`ws.hook.schedule`).
struct Hooks;

impl AgentResource for Hooks {
    async fn agent_deleted(&self, _svc: &Services, _agent: DeletedAgent<'_>) {
        // The hook rows cascade with the session row (FK); each scheduler
        // task sees its row gone on its next tick.
    }

    async fn agent_retired(&self, svc: &Services, agent_id: &AgentId) {
        // Cancelled silently: the owner retired itself, so no wake notice.
        svc.ports.runtime().cancel_agent_hooks(svc, agent_id).await;
    }

    async fn agent_restored(&self, _svc: &Services, _agent_id: &AgentId) {
        // Not resurrected: the agent schedules again if it still cares.
    }

    async fn workspace_archived(&self, svc: &Services, sweep: &mut ArchiveSweep) {
        // The archive path wakes each owner once with what was cancelled.
        sweep.cancelled_hooks = svc
            .ports
            .runtime()
            .cancel_workspace_hooks(svc, &sweep.workspace_id)
            .await;
    }

    async fn workspace_unarchived(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Not resurrected: the archive notice tells owners to reschedule.
    }

    async fn workspace_retired(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // The archive sweep already cancelled them.
    }

    async fn workspace_deleted(&self, svc: &Services, workspace: DeletedWorkspace<'_>) {
        // Abort the live scheduler tasks before the row cascade, so none
        // lingers until its next tick.
        svc.ports
            .runtime()
            .abort_workspace_hook_tasks(svc, workspace.workspace_id)
            .await;
    }
}

/// PR monitors (`ws.pr.monitor`).
struct PrMonitors;

impl AgentResource for PrMonitors {
    async fn agent_deleted(&self, _svc: &Services, _agent: DeletedAgent<'_>) {
        // The monitor rows cascade with the session row (FK).
    }

    async fn agent_retired(&self, svc: &Services, agent_id: &AgentId) {
        svc.cancel_agent_pr_monitors(agent_id).await;
    }

    async fn agent_restored(&self, _svc: &Services, _agent_id: &AgentId) {
        // Not resurrected: the agent monitors again if it still cares.
    }

    async fn workspace_archived(&self, svc: &Services, sweep: &mut ArchiveSweep) {
        // Without this an archived workspace's displayStatus reads
        // `in_progress` off the active-monitor signal (monorepo#1828).
        sweep.cancelled_monitors = svc.cancel_workspace_pr_monitors(&sweep.workspace_id).await;
    }

    async fn workspace_unarchived(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Not resurrected: the archive notice tells owners to re-monitor.
    }

    async fn workspace_retired(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // The archive sweep already cancelled them.
    }

    async fn workspace_deleted(&self, _svc: &Services, _workspace: DeletedWorkspace<'_>) {
        // The monitor rows cascade with the workspace row (FK).
    }
}

/// The agent's own outgoing completion watches and delegation groups.
/// Watches ON the agent are settled by its `agent:deleted` /
/// `agent:retired` event instead.
struct CompletionWatches;

impl AgentResource for CompletionWatches {
    async fn agent_deleted(&self, svc: &Services, agent: DeletedAgent<'_>) {
        // A gone watcher can never consume a wake (monorepo#4183).
        svc.remove_all_for_parent(agent.agent_id);
        svc.remove_groups_for_parent(agent.agent_id);
        if let Some(workspace_id) = agent.workspace_id {
            svc.maybe_emit_waiting_changed(workspace_id).await;
        }
    }

    async fn agent_retired(&self, svc: &Services, agent_id: &AgentId) {
        // A retired watcher fails every delivery on the inertness gate, so
        // an armed watch would feed the retry loop forever (monorepo#4183).
        // The retire path recomputes `waiting` once its sweeps are done.
        svc.remove_all_for_parent(agent_id);
        svc.remove_groups_for_parent(agent_id);
    }

    async fn agent_restored(&self, _svc: &Services, _agent_id: &AgentId) {
        // Not resurrected: the agent watches again if it still cares.
    }

    async fn workspace_archived(&self, _svc: &Services, _sweep: &mut ArchiveSweep) {
        // Kept so unarchive resumes the work.
    }

    async fn workspace_unarchived(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Kept through the archive; nothing to restore.
    }

    async fn workspace_retired(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Kept, as for archive.
    }

    async fn workspace_deleted(&self, _svc: &Services, _workspace: DeletedWorkspace<'_>) {
        // Swept by `delete_workspace_with_claim` itself: every watch whose
        // parent lives in the workspace goes BEFORE its `agent:deleted`
        // deliveries (so no deleted agent is woken), and every watch on its
        // agents after them. The registry runs later than both.
    }
}

/// Per-agent in-memory runtime state: message queue, live-turn slot and
/// the bookkeeping maps keyed by agent id.
struct RuntimeMemory;

impl AgentResource for RuntimeMemory {
    async fn agent_deleted(&self, svc: &Services, agent: DeletedAgent<'_>) {
        svc.forget_agent_runtime_memory(std::slice::from_ref(agent.agent_id));
    }

    async fn agent_retired(&self, _svc: &Services, _agent_id: &AgentId) {
        // Kept: restore may drain the parked queue later.
    }

    async fn agent_restored(&self, _svc: &Services, _agent_id: &AgentId) {
        // The restore path kicks the parked queue.
    }

    async fn workspace_archived(&self, _svc: &Services, _sweep: &mut ArchiveSweep) {
        // Kept so unarchive resumes the work.
    }

    async fn workspace_unarchived(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // The unarchive path kicks the parked queues.
    }

    async fn workspace_retired(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // Kept: a revived workspace resumes its queues.
    }

    async fn workspace_deleted(&self, svc: &Services, workspace: DeletedWorkspace<'_>) {
        // A same-slug recreate must observe no ghost state.
        svc.forget_agent_runtime_memory(workspace.agent_ids);
    }
}

/// The agent's private temp folder (`TMPDIR`).
struct TempFolders;

impl AgentResource for TempFolders {
    async fn agent_deleted(&self, svc: &Services, agent: DeletedAgent<'_>) {
        if let Some(manager) = svc.agent_manager() {
            manager
                .remove_agent_tmp_dirs(
                    std::slice::from_ref(agent.agent_id),
                    AgentTmpRemoval::Deleted,
                )
                .await;
        }
    }

    async fn agent_retired(&self, svc: &Services, agent_id: &AgentId) {
        // Goes now, or when a still-running process ends.
        if let Some(manager) = svc.agent_manager() {
            manager
                .remove_agent_tmp_dirs(std::slice::from_ref(agent_id), AgentTmpRemoval::Retired)
                .await;
        }
    }

    async fn agent_restored(&self, svc: &Services, agent_id: &AgentId) {
        // Under the retirement gate, like the retire's removal. An
        // archive's removal stands.
        if let Some(manager) = svc.agent_manager() {
            manager.keep_agent_tmp_dirs(std::slice::from_ref(agent_id), AgentTmpRemoval::Retired);
        }
    }

    async fn workspace_archived(&self, svc: &Services, sweep: &mut ArchiveSweep) {
        // Detach under the fence so an unarchive's keep runs after it, and
        // only while this archive is still current. The archive path
        // deletes the detached folders once its fence is released.
        let _fence = svc
            .archive_fence
            .acquire_or_local(&sweep.workspace_id)
            .await;
        let still_archived = match svc.store.get_workspace(&sweep.workspace_id).await {
            Ok(row) => row.archived && row.archived_at == sweep.archived_at,
            Err(e) => {
                tracing::warn!(
                    workspace = %sweep.workspace_id.as_str(),
                    error = %e,
                    "workspace.archive: row re-read failed before detaching temp folders"
                );
                true
            }
        };
        if still_archived {
            let detached = svc
                .detach_workspace_agent_tmp_dirs(&sweep.workspace_id)
                .await;
            sweep.detached_tmp_dirs.extend(detached);
        }
    }

    async fn workspace_unarchived(&self, svc: &Services, workspace_id: &WorkspaceId) {
        // Keep the folders the archive left to running processes.
        svc.keep_workspace_agent_tmp_dirs(workspace_id).await;
    }

    async fn workspace_retired(&self, _svc: &Services, _workspace_id: &WorkspaceId) {
        // The archive already removed them; a retire's removal stands.
    }

    async fn workspace_deleted(&self, svc: &Services, workspace: DeletedWorkspace<'_>) {
        // The agents' trees are killed and fenced from respawn.
        if let Some(manager) = svc.agent_manager() {
            manager
                .remove_agent_tmp_dirs(workspace.agent_ids, AgentTmpRemoval::Deleted)
                .await;
        }
    }
}

impl Services {
    /// Drop `agent_ids`' in-memory runtime state (deleted agents). Recovers
    /// poisoned locks: delete is the last chance to unlink this state.
    fn forget_agent_runtime_memory(&self, agent_ids: &[AgentId]) {
        for agent_id in agent_ids {
            self.live_turns
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(agent_id);
            self.agent_queues
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(agent_id);
            // Silent-tail record (monorepo#2669), truncation-redrive counter
            // and handoff flag (monorepo#2863).
            self.clear_turn_silent_tail(agent_id);
            self.clear_truncation_redrives(agent_id);
            self.take_truncation_redrive(agent_id);
            // A parked mid-turn attention raise.
            self.take_deferred_attention(agent_id);
            // Context occupancy (intent-hq/intent#3797).
            self.clear_context_usage(agent_id);
            // Failure streak and failure-wake dedup records naming the agent
            // as parent OR child (monorepo#840), and the streaming path's
            // terminal-error stash (monorepo#2050).
            self.clear_failure_streak(agent_id);
            self.clear_failure_wake_dedup_all_roles(agent_id);
            self.discard_pending_terminal_error(agent_id);
            // Pending-question registries (monorepo#3179). In-flight marker
            // mutations keep their own Arc of the mutex.
            self.pending_question_mutation_locks.remove(agent_id);
        }
    }
}
