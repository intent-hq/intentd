//! Central workspace display-status derivation and emission (PROTOCOL §6.5).
//!
//! Everything that computes or publishes the derived workspace
//! `displayStatus` lives here: the pure precedence derivation
//! ([`compute_display_status`] / [`compute_base_display_status`]), the
//! per-workspace needs-attention probe, the last-observed
//! [`DisplayStatusCache`], and the `workspace:displayStatus-changed` event
//! constructor.
//!
//! Invariant: **status is never mutated directly; mutations call
//! recompute.** Read paths (list/get enrichment) derive the wire value via
//! [`Services::enrich_display_status`], which seeds the last-observed
//! baseline without emitting; mutation paths call
//! [`Services::maybe_emit_display_status_changed`], which recomputes and
//! publishes only on an actual transition. The event constructor and the
//! cache internals are private to this module, so nothing outside it can
//! emit `workspace:displayStatus-changed` or touch the baseline.

use std::collections::HashMap;
use std::sync::Mutex;

use intent_core::events::{WORKSPACE_DISPLAY_STATUS_CHANGED, WORKSPACE_WAITING_CHANGED};
use intent_core::{
    now_iso, PullRequestInfo, PullRequestStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
    WorkspaceDisplayStatus, WorkspaceId, WorkspaceTaskStats,
};
use intent_store::NewEvent;

use crate::{compute_task_stats, publish_event, system_actor, Services};

/// Last-observed derived value per workspace: the recompute-and-compare seam
/// behind [`Services::maybe_emit_display_status_changed`] (as
/// [`DisplayStatusCache`], PROTOCOL §6.5) and
/// [`Services::maybe_emit_waiting_changed`] (as [`WaitingStatusCache`],
/// §5.1). A mutation that can move the derivation recomputes it and
/// publishes the matching change event only on an actual transition, so
/// no-op recomputes never spam the bus. Seeded lazily (first recompute after
/// a mutation, or an emit-path enrichment) — a first observation records
/// without emitting. In-memory only; a daemon restart re-seeds on first
/// touch. Shared across clones (behind `Arc`) so every service handle
/// compares against the same last-emitted value. The map is private to this
/// module: outside code can neither read nor write the baseline.
///
/// Evict-vs-in-flight-compute race: a recompute/enrichment reads the store,
/// awaits, then writes the cache — a `workspace.delete` eviction can land in
/// between, and the late write would resurrect a baseline for the deleted id
/// (a leaked entry, and a stale comparison for an importer re-insert of the
/// same id). Guard: `evictions` counts evictions under the same lock; writers
/// snapshot it via [`LastObservedCache::generation`] *before* their store
/// reads and their write is dropped when the counter moved and no entry
/// survived for the id (an entry that still exists was not evicted — or was
/// legitimately re-seeded — so the write proceeds). A dropped write is always
/// safe: the next transition recomputes from fresh state.
pub(crate) struct LastObservedCache<T>(Mutex<CacheInner<T>>);

/// Last-observed derived `displayStatus` per workspace (PROTOCOL §6.5).
pub(crate) type DisplayStatusCache = LastObservedCache<WorkspaceDisplayStatus>;

/// Last-observed orthogonal `waiting` flag per workspace (PROTOCOL §5.1).
pub(crate) type WaitingStatusCache = LastObservedCache<bool>;

struct CacheInner<T> {
    map: HashMap<WorkspaceId, T>,
    /// Total evictions since startup; see the eviction-race guard above.
    evictions: u64,
}

impl<T> Default for LastObservedCache<T> {
    fn default() -> Self {
        Self(Mutex::new(CacheInner {
            map: HashMap::new(),
            evictions: 0,
        }))
    }
}

impl<T: Copy + PartialEq> LastObservedCache<T> {
    /// Snapshot the eviction generation. Writers capture this *before* their
    /// store reads and pass it back to [`seed`](Self::seed) /
    /// [`record`](Self::record), which drop the write when an eviction
    /// intervened. A poisoned lock returns `u64::MAX` (never matches a live
    /// generation, and the write path bails on the poisoned lock anyway).
    fn generation(&self) -> u64 {
        self.0.lock().map_or(u64::MAX, |g| g.evictions)
    }

    /// Seed the baseline when absent (read paths): records the first
    /// observation without reporting a transition, so the first post-read
    /// mutation compares against it. `generation` is the pre-read snapshot;
    /// a stale seed for an id with no surviving entry is dropped (eviction
    /// race, see the type docs). Best-effort — a poisoned lock is ignored.
    fn seed(&self, workspace_id: &WorkspaceId, value: T, generation: u64) {
        if let Ok(mut inner) = self.0.lock() {
            if inner.evictions != generation && !inner.map.contains_key(workspace_id) {
                return;
            }
            inner.map.entry(workspace_id.clone()).or_insert(value);
        }
    }

    /// Record `value` and report whether it transitioned since the last
    /// observation: `Some(false)` on a first observation (a seed has no
    /// baseline to transition from), `None` on a poisoned lock or when the
    /// write was dropped by the eviction-race guard (the caller skips
    /// emission). `generation` is the pre-read snapshot from
    /// [`generation`](Self::generation).
    fn record(&self, workspace_id: &WorkspaceId, value: T, generation: u64) -> Option<bool> {
        match self.0.lock() {
            Ok(mut inner) => {
                if inner.evictions != generation && !inner.map.contains_key(workspace_id) {
                    return None;
                }
                Some(match inner.map.insert(workspace_id.clone(), value) {
                    Some(previous) => previous != value,
                    None => false,
                })
            }
            Err(_) => None,
        }
    }

    /// Drop the baseline for a deleted workspace so the entry does not leak
    /// for the daemon's lifetime (and an importer re-insert of the same id
    /// starts from a fresh seed instead of a stale baseline). Bumps the
    /// eviction generation so in-flight computes that read the store before
    /// the delete cascade cannot write a baseline back for the deleted id.
    /// Best-effort — a poisoned lock is ignored.
    fn evict(&self, workspace_id: &WorkspaceId) {
        if let Ok(mut inner) = self.0.lock() {
            inner.map.remove(workspace_id);
            inner.evictions += 1;
        }
    }

    /// Read the last-observed baseline for `workspace_id`, when one exists.
    /// Best-effort — a poisoned lock reads as no baseline.
    fn get(&self, workspace_id: &WorkspaceId) -> Option<T> {
        self.0
            .lock()
            .ok()
            .and_then(|inner| inner.map.get(workspace_id).copied())
    }

    /// Test-only visibility into whether a baseline exists for `workspace_id`.
    #[cfg(test)]
    pub(crate) fn contains(&self, workspace_id: &WorkspaceId) -> bool {
        self.0
            .lock()
            .expect("lock cache")
            .map
            .contains_key(workspace_id)
    }
}

impl Services {
    /// Derive and attach the "current cycle" `displayStatus` on the list/get
    /// enrichment paths, over the active/latest PR and the row's `taskStats`;
    /// never persisted. Only populated when `taskStats` was computable: on a
    /// transient notes-read failure the field stays absent (clients fall back
    /// to local derivation on a missing field) and the last-observed cache is
    /// left untouched, so a None-compute can never misreport
    /// `not_started`/`pr_*` or pollute the baseline. Seeds the last-observed
    /// cache when absent so the first post-read mutation compares against
    /// this baseline (a seed never emits; see
    /// [`Services::maybe_emit_display_status_changed`]).
    ///
    /// `sessions` — the workspace's session summaries when the caller already
    /// fetched them (the aggregate enrichment path does, for `agentSummary` /
    /// `lastActivity`); passed through to the attention probe so the hot
    /// list/get emit never re-issues the same per-workspace session query
    /// (monorepo#3058). `None` lets the probe fetch its own. The summaries
    /// cannot answer the unread derivation — `SESSION_SUMMARY_COLUMNS`
    /// deliberately omits the `last_message_id`/`last_message_role` preview
    /// columns — which is what `unread` is for.
    ///
    /// `unread` — the workspace's unread derivation when the caller already
    /// computed it (the list-shaped paths batch it in ONE statement for the
    /// whole list via `workspaces_with_unread_top_level_sessions`, so the
    /// hot RPC's statement count stays independent of the workspace count —
    /// AGENTS.md RPC cost contract). `None` (single-row paths: get, mutation
    /// responses, event emits) runs the bounded per-workspace EXISTS probe.
    pub(crate) async fn enrich_display_status(
        &self,
        ws: &mut Workspace,
        sessions: Option<&[intent_core::AgentSession]>,
        unread: Option<bool>,
    ) {
        // Served `attention` is DERIVED on this same emit path (§5.1):
        // `unread` = any top-level (non-background, non-deleted) session
        // whose newest user/assistant message is an unseen assistant message
        // (per-agent seen marker, `agent.markSeen` §5.5). A stored
        // `review_required` still wins (it is the persistent review flag,
        // retired only by `workspace.dismissAttention`); the stored `unread`
        // flag is no longer the read-path source of truth — the turn-end
        // raise still writes it (back-compat + the transition emit), but a
        // stale stored value can neither show nor hide the blue dot.
        // Archived rows keep the stored value: the turn-end raise skips
        // archived workspaces (no blue dot until unarchive, intentd#1075)
        // and the derivation honors the same rule. One bounded EXISTS over
        // persisted session columns — or the caller's batch-derived value —
        // a probe failure keeps the stored value (degrade, never fail the
        // read).
        if ws.attention != WorkspaceAttention::ReviewRequired
            && ws.status != intent_core::WorkspaceStatus::Archived
        {
            let derived = match unread {
                Some(unread) => Some(unread),
                None => self
                    .store
                    .workspace_has_unread_top_level_session(&ws.id)
                    .await
                    .ok(),
            };
            if let Some(unread) = derived {
                ws.attention = if unread {
                    WorkspaceAttention::Unread
                } else {
                    WorkspaceAttention::None
                };
            }
        }
        // The orthogonal `waiting` flag rides the same emit path but is
        // independent of the `taskStats` gate below: it is populated even
        // when a transient notes-read failure leaves `displayStatus` absent.
        // Served from the last-observed cache (rung 1 of the derived-field
        // ladder: the hook/monitor/watch mutation choke points keep it
        // current via [`Services::maybe_emit_waiting_changed`], so hot
        // list/get reads cost one in-memory lookup, no per-row store
        // fan-out); only a cache miss — first touch after startup — probes
        // the store and seeds the `workspace:waiting-changed` baseline.
        ws.waiting = if let Some(waiting) = self.last_waiting_statuses.get(&ws.id) {
            waiting
        } else {
            // Pre-read generation snapshot: a `workspace.delete`
            // eviction racing the probe must not have this seed
            // resurrect the baseline.
            let waiting_generation = self.last_waiting_statuses.generation();
            let waiting = self.workspace_is_waiting(&ws.id).await;
            self.last_waiting_statuses
                .seed(&ws.id, waiting, waiting_generation);
            waiting
        };
        if ws.task_stats.is_none() {
            return;
        }
        // Pre-read generation snapshot: a `workspace.delete` eviction racing
        // the awaits below must not have this seed resurrect the baseline.
        let generation = self.last_display_statuses.generation();
        // Derive from the row's own `activity` (set by every caller just
        // before enrichment) so a single response can never pair
        // `activity: "agent_running"` with `displayStatus: "idle"`. Wait
        // signals (hooks/subscriptions) no longer fold into the promotion —
        // they surface as the orthogonal `waiting` flag above — but
        // agent-monitored PRs DO feed the PR rungs: an active monitor on an
        // open PR (including cross-repo) reads as an open-PR signal.
        let display_status = compute_display_status(
            self.workspace_attention_signals(&ws.id, ws.attention, sessions)
                .await,
            ws.activity == WorkspaceActivity::AgentRunning,
            ws.active_pull_request.as_ref(),
            ws.pull_requests.as_deref().unwrap_or_default(),
            ws.pr_status,
            self.workspace_monitor_pr_signals(&ws.id).await,
            ws.task_stats.as_ref(),
        );
        self.last_display_statuses
            .seed(&ws.id, display_status, generation);
        ws.display_status = Some(display_status);
    }

    /// The orthogonal wait probe behind `Workspace.waiting` (§5.1): true when
    /// the workspace has any of ACTIVE background hooks, ACTIVE PR monitors,
    /// or waiting agent subscriptions (undelivered child completion watches
    /// held by top-level foreground agents — agent-owned `event.subscribe`
    /// registrations deliberately do NOT count: they watch in-workspace
    /// activity, not an external condition, matching the documented v6.17
    /// signal set). Short-circuits on the first live signal;
    /// best-effort/fail-open like the probes it reuses (a store read
    /// failure reads `false`, never wedges list/get emission).
    pub(crate) async fn workspace_is_waiting(&self, workspace_id: &WorkspaceId) -> bool {
        self.workspace_has_active_hooks(workspace_id).await
            || self.workspace_has_active_pr_monitors(workspace_id).await
            || self
                .workspace_has_waiting_agent_subscriptions(workspace_id)
                .await
    }

    /// Recompute a workspace's derived `displayStatus` and publish
    /// `workspace:displayStatus-changed` iff it transitioned since the last
    /// observation (PROTOCOL §6.5). Called after the mutations that can move
    /// the derivation (task/note status updates, task-note deletion, PR
    /// link/status changes, agent activity begin/debounced end, hook
    /// lifecycle transitions) — never from a polling loop. The first
    /// observation for a workspace seeds the cache without emitting (no
    /// baseline to transition from); a read failure skips the recompute
    /// entirely so a transient store error can never fake a transition.
    /// Best-effort: errors are swallowed, the mutation's own result is the
    /// contract. Concurrent callers (e.g. the
    /// debounced idle demotion racing an `agent_activity_begin` promotion)
    /// can in principle invert: the compute-then-insert is not atomic, so a
    /// stale compute inserted second would emit outdated and leave the
    /// baseline stale until the next transition. The activity read and cache
    /// insert have no await between them, so the window is negligible and
    /// self-heals on the next transition.
    pub(crate) async fn maybe_emit_display_status_changed(&self, workspace_id: &WorkspaceId) {
        // Pre-read generation snapshot: an eviction (workspace.delete)
        // landing between the store reads below and the cache write must
        // drop this compute rather than re-insert a baseline for the
        // deleted id (see the `DisplayStatusCache` docs).
        let generation = self.last_display_statuses.generation();
        let Ok(ws) = self.store.get_workspace(workspace_id).await else {
            return;
        };
        let Ok(notes) = self.store.list_notes(workspace_id).await else {
            return;
        };
        let task_stats = compute_task_stats(&notes);
        let signals = self
            .workspace_attention_signals(workspace_id, ws.attention, None)
            .await;
        // Wait signals (hooks/subscriptions) do not fold into the promotion
        // — they surface as the orthogonal `waiting` flag on the read paths
        // ([`Services::workspace_is_waiting`]); only a live agent turn
        // promotes here. Agent-monitored PRs feed the PR rungs, so the
        // monitor lifecycle choke points (register/complete/cancel) route
        // through this recompute.
        let status = compute_display_status(
            signals,
            self.workspace_activity(workspace_id) == WorkspaceActivity::AgentRunning,
            ws.active_pull_request.as_ref(),
            ws.pull_requests.as_deref().unwrap_or_default(),
            ws.pr_status,
            self.workspace_monitor_pr_signals(workspace_id).await,
            Some(&task_stats),
        );
        let Some(transitioned) =
            self.last_display_statuses
                .record(workspace_id, status, generation)
        else {
            return;
        };
        if transitioned {
            publish_event(
                self.event_bus.as_ref(),
                display_status_changed_event(workspace_id, status),
            )
            .await;
        }
    }

    /// Recompute a workspace's orthogonal `waiting` flag and publish
    /// `workspace:waiting-changed` iff it transitioned since the last
    /// observation (PROTOCOL §5.1 / §6.5). Called after the lifecycle transitions
    /// that can move the derivation — hook create/dispatch/cancel/evict/
    /// expire, PR monitor register/complete/cancel, completion-watch
    /// register/retire/settlement/cancel — never from a polling loop. Same
    /// contract as [`Services::maybe_emit_display_status_changed`]: the
    /// first observation seeds without emitting, a workspace-read failure
    /// (deleted workspace) skips the recompute entirely, and the whole path
    /// is best-effort — the mutation's own result is the contract. The wait
    /// probe itself fails open to `false` on store errors, which the dedup
    /// baseline absorbs: a transient flap emits at most one pair of
    /// transitions, and the next recompute converges on truth.
    pub(crate) async fn maybe_emit_waiting_changed(&self, workspace_id: &WorkspaceId) {
        // Chief is a fixed virtual workspace synthesized on read
        // (`workspace.get` returns `chief_workspace()` before enrichment and
        // `workspace.list` excludes it), so its rows can never carry
        // `waiting` — never emit a transition its re-read cannot confirm.
        // Chief-anchored completion watches are deliberately invisible here.
        if workspace_id.is_chief() {
            return;
        }
        // Pre-read generation snapshot: an eviction (workspace.delete)
        // landing between the reads below and the cache write must drop
        // this compute rather than re-insert a baseline for the deleted id
        // (see the `LastObservedCache` docs).
        let generation = self.last_waiting_statuses.generation();
        // Deleted-workspace guard: the wait probes read hook/monitor rows
        // directly, so without this read a post-delete recompute could
        // fabricate a baseline for a gone workspace.
        if self.store.get_workspace(workspace_id).await.is_err() {
            return;
        }
        let waiting = self.workspace_is_waiting(workspace_id).await;
        let Some(transitioned) =
            self.last_waiting_statuses
                .record(workspace_id, waiting, generation)
        else {
            return;
        };
        if transitioned {
            publish_event(
                self.event_bus.as_ref(),
                waiting_changed_event(workspace_id, waiting),
            )
            .await;
        }
    }

    /// Evict a deleted workspace's last-observed baselines (G7): called from
    /// `workspace.delete` after the store cascade so the in-memory maps do
    /// not leak entries for the daemon's lifetime. Bumps each cache's eviction
    /// generation, so a recompute that read the workspace before the cascade
    /// drops its late write instead of resurrecting the baseline. Workspace
    /// ids are never recycled by `workspace.create` (tombstoned via
    /// `deleted_workspace_id`), so a stale-baseline collision could only
    /// come from such a late write — which the generation guard prevents.
    pub(crate) fn evict_display_status_baseline(&self, workspace_id: &WorkspaceId) {
        self.last_display_statuses.evict(workspace_id);
        self.last_waiting_statuses.evict(workspace_id);
    }

    /// Recompute after a spec-body write. The spec's markdown gates
    /// `taskStats` (`extract_spec_task_ids`: with links present, only linked
    /// child tasks count), so editing the spec body — adding/removing task
    /// links, checkbox rewrites, version restores — can move the derived
    /// rollup without any task-note mutation. Non-spec notes skip the probe
    /// entirely; the dedup cache suppresses no-op spec writes.
    pub(crate) async fn maybe_emit_display_status_for_spec_write(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &intent_core::NoteId,
    ) {
        if note_id.as_str() == "spec" {
            self.maybe_emit_display_status_changed(workspace_id).await;
        }
    }

    /// Probe the workspace's attention axes over **top-level** agent
    /// sessions (no `parent_agent_id`, not background, not deleted) plus the
    /// dismissible workspace `attention` flag (PROTOCOL §6.5):
    ///
    /// - `failed` — a top-level session parked in `error` (awaiting
    ///   `agent.retry`).
    /// - `blocked` — a top-level pending `blocker` attention request.
    /// - `needs_attention` — a top-level pending non-blocker attention
    ///   request (`discussion`), pending structured questions
    ///   ([`Services::questions_pending`] — pending until answered or
    ///   dismissed, so a question the user walked away from keeps the
    ///   workspace flagged across the agent's later turns and daemon
    ///   restarts), or the workspace `attention` flag at `review_required`.
    ///
    /// The `unread` workspace attention flag never feeds the signals — it
    /// is the flag's own contract (§9.9), not a displayStatus axis.
    /// Child/background sessions never count — their attention surface is
    /// the parent/subscriber (attention-retire taxonomy). A pending request
    /// raised MID-TURN whose surfacing is still parked on the
    /// deferred-attention registry does not count either: the workspace
    /// stays `in_progress` until the raising agent's turn-end flush
    /// surfaces the request. The cheap metadata
    /// checks run over every candidate first, so the per-session pending reads
    /// only happen when `needs_attention` is still undecided. Best-effort: a
    /// store read failure fails open — session-derived signals read `false`
    /// (and `questions_pending` fails open itself) so list/get emission
    /// is never wedged; the flag-derived signal needs no store read.
    ///
    /// `sessions` — the workspace's session summaries when the caller already
    /// fetched them (the list/get enrichment path does); avoids re-issuing
    /// the same per-workspace query on the hot emit path (monorepo#3058).
    /// `None` fetches fresh summaries.
    pub(crate) async fn workspace_attention_signals(
        &self,
        workspace_id: &WorkspaceId,
        attention: WorkspaceAttention,
        sessions: Option<&[intent_core::AgentSession]>,
    ) -> AttentionSignals {
        let mut signals = AttentionSignals {
            needs_attention: attention == WorkspaceAttention::ReviewRequired,
            ..AttentionSignals::default()
        };
        let fetched;
        let sessions: &[intent_core::AgentSession] = match sessions {
            Some(sessions) => sessions,
            None => match self.store.list_agent_session_summaries(workspace_id).await {
                Ok(list) => {
                    fetched = list;
                    &fetched
                }
                Err(_) => return signals,
            },
        };
        let top_level: Vec<_> = sessions
            .iter()
            .filter(|s| {
                s.parent_agent_id.is_none()
                    && !s.is_background
                    && s.status != intent_core::AgentStatus::Deleted
            })
            .collect();
        for s in &top_level {
            if s.status == intent_core::AgentStatus::Error {
                signals.failed = true;
            }
            // A pending request whose surfacing is parked awaiting the idle
            // flush does not promote the displayStatus yet — the workspace
            // reads `in_progress` while the raising turn is still running,
            // and the turn-end flush's recompute promotes it (the marker is
            // consumed there first, so this read flips at exactly the
            // surfacing point).
            if self.attention_surfacing_deferred(&s.id) {
                continue;
            }
            match s.attention_request_kind.as_deref() {
                Some("blocker") => signals.blocked = true,
                Some(_) => signals.needs_attention = true,
                None => {}
            }
        }
        if !signals.needs_attention {
            for session in top_level {
                // The summaries already carry the session `metadata`, so a
                // written pending-questions marker is decided right here with
                // no extra store read (monorepo#3058) — same derivation as
                // [`Services::questions_pending`]. Only pre-upgrade
                // sessions (marker key never written) fall back to the full
                // per-session probe, which also materializes the marker.
                let pending = if session.pending_questions_marker_written() {
                    match session.pending_questions_message_id() {
                        Some(pending) => session.dismissed_questions_message_id() != Some(pending),
                        None => false,
                    }
                } else {
                    self.questions_pending(&session.id).await
                };
                if pending {
                    signals.needs_attention = true;
                    break;
                }
            }
        }
        signals
    }
}

/// Attention-axis inputs to [`compute_display_status`], probed by
/// [`Services::workspace_attention_signals`]. Each field is one canonical
/// precedence rung (§6.5): `failed` > `blocked` > `needs_attention` >
/// (running agent) > the PR/task rollup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AttentionSignals {
    /// A top-level non-background agent is parked in `error`.
    pub(crate) failed: bool,
    /// A top-level pending `blocker` attention request.
    pub(crate) blocked: bool,
    /// A top-level pending `discussion` request, pending structured
    /// questions, or the `review_required` workspace attention flag.
    pub(crate) needs_attention: bool,
}

/// Open/merged PR signals derived from the workspace's agent-owned PR
/// monitors (persisted `pr_monitor` rows; probed by
/// [`Services::workspace_monitor_pr_signals`]), so a workspace watching a PR
/// via `ws.pr.monitor` — including a cross-repo PR that never appears in the
/// workspace's own PR linkage — participates in the PR rungs of
/// [`compute_display_status`]. Derived purely from persisted
/// `state`/`last_snapshot` columns: no forge calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct MonitorPrSignals {
    /// An ACTIVE monitor's last snapshot shows an open (non-draft) PR that
    /// sits in the forge's merge queue (`requirements.isInMergeQueue`) — the
    /// `pr_queued` mapping; outranks `ready` (a queued PR is being handled
    /// by the queue, no action needed).
    pub(crate) queued: bool,
    /// An ACTIVE monitor's last snapshot shows an open (non-draft) PR whose
    /// full merge-requirements checklist is clear — truly mergeable, not
    /// merely conflict-free (see
    /// [`crate::pr_monitor::fold_monitor_pr_signals`]) — the `pr_ready`
    /// mapping.
    pub(crate) ready: bool,
    /// An ACTIVE monitor's last snapshot shows an open or draft PR.
    pub(crate) open: bool,
    /// A COMPLETED monitor's final snapshot shows the PR merged.
    pub(crate) merged: bool,
}

/// Derive a workspace's `displayStatus` (canonical precedence, spec
/// "Decision: BE-owned displayStatus"), folding in live agent activity
/// (previously a client-side overlay) and the attention axes probed by
/// [`Services::workspace_attention_signals`]:
/// 0. `failed` → `failed`: a top-level agent parked in `error` outranks
///    everything — the workspace cannot make progress until `agent.retry`.
/// 1. `blocked` → `blocked`: a top-level pending `blocker` attention
///    request (infrastructure/environment problem).
/// 2. `needs_attention` → `needs_attention`: a top-level agent waiting on
///    the user (discussion request or pending structured questions) or the
///    `review_required` workspace attention flag — outranks a running agent.
/// 3. `agent_running` → `in_progress`: a live agent always reads as active
///    work, whatever the PR/task rollup says. Wait signals (active hooks,
///    waiting agent subscriptions) do NOT fold in — an idle workspace
///    watching an external condition keeps its base rollup and surfaces the
///    orthogonal `Workspace.waiting` flag instead
///    ([`Services::workspace_is_waiting`]).
/// 4. Active PR — the linked `activePullRequest` when open/draft, else the
///    most recently updated open/draft entry in `pullRequests` — yields
///    `pr_queued` when the PR sits in the forge's merge queue
///    (`mergeable_state == "queued"`, not draft), `pr_ready` only when truly
///    mergeable (`mergeable == Some(true)` AND `mergeable_state == "clean"`,
///    not draft), else `pr_open`. GitHub's `mergeable` flag alone only means
///    "no merge conflicts" — a PR blocked by required checks or reviews
///    still reports `mergeable: true` — so a missing/unknown
///    `mergeable_state` conservatively reads `pr_open`.
///    An ACTIVE PR monitor whose last snapshot shows an open/draft PR
///    (`monitor_prs`) is the same rung: `pr_queued` when the snapshot's PR
///    is in the merge queue (not draft), `pr_ready` when the snapshot's full
///    merge-requirements checklist is clear and the PR is not draft, else
///    `pr_open` — so a workspace watching an open PR (including cross-repo)
///    never falls through to `complete`/`idle`. When none of those carries
///    an open/draft entry but the workspace `prStatus` column is
///    `Open`/`Draft`, that column is the fallback PR-stage signal and
///    yields `pr_open` (never `pr_ready`: the column carries no mergeable
///    info).
/// 5. Open tasks remain (`completed < total`) → `in_progress` when any task
///    has started, else `not_started`.
/// 6. Latest PR (linked, else most recently updated entry) merged — or
///    `prStatus == Merged`, or a COMPLETED monitor whose final snapshot
///    shows the PR merged — → `pr_merged`.
/// 7. All tasks complete → `complete`; else `not_started`.
/// 8. Without a running agent, a task-stage rollup (`in_progress` /
///    `not_started` from steps 5/7) demotes to `idle`; the PR stages and
///    `complete` pass through unchanged.
///
/// The dismissible `unread` workspace attention flag (§9.9) never feeds the
/// derivation. A merged PR in history never masks an open PR (step 4 scans
/// `pullRequests` and the monitor signals for open/draft entries) or open
/// tasks (step 5 precedes the merged check).
fn compute_display_status(
    signals: AttentionSignals,
    agent_running: bool,
    active_pr: Option<&PullRequestInfo>,
    pull_requests: &[PullRequestInfo],
    pr_status: Option<PullRequestStatus>,
    monitor_prs: MonitorPrSignals,
    task_stats: Option<&WorkspaceTaskStats>,
) -> WorkspaceDisplayStatus {
    if signals.failed {
        return WorkspaceDisplayStatus::Failed;
    }
    if signals.blocked {
        return WorkspaceDisplayStatus::Blocked;
    }
    if signals.needs_attention {
        return WorkspaceDisplayStatus::NeedsAttention;
    }
    if agent_running {
        return WorkspaceDisplayStatus::InProgress;
    }
    match compute_base_display_status(active_pr, pull_requests, pr_status, monitor_prs, task_stats)
    {
        WorkspaceDisplayStatus::InProgress | WorkspaceDisplayStatus::NotStarted => {
            WorkspaceDisplayStatus::Idle
        }
        other => other,
    }
}

/// PR/task-only precedence behind [`compute_display_status`] (steps 2–5);
/// the caller applies the attention/agent-activity promotion/demotion
/// around it.
fn compute_base_display_status(
    active_pr: Option<&PullRequestInfo>,
    pull_requests: &[PullRequestInfo],
    pr_status: Option<PullRequestStatus>,
    monitor_prs: MonitorPrSignals,
    task_stats: Option<&WorkspaceTaskStats>,
) -> WorkspaceDisplayStatus {
    let is_open = |pr: &&PullRequestInfo| {
        matches!(
            pr.status,
            PullRequestStatus::Open | PullRequestStatus::Draft
        )
    };
    let open_pr = active_pr.filter(is_open).or_else(|| {
        pull_requests
            .iter()
            .filter(is_open)
            .max_by(|a, b| a.updated_at.cmp(&b.updated_at))
    });
    if let Some(pr) = open_pr {
        let draft = pr.status == PullRequestStatus::Draft || pr.is_draft == Some(true);
        // GitHub reports `mergeable_state: "queued"` for a PR sitting in
        // the merge queue — it is beyond "ready", the queue is handling it.
        let queued = pr.mergeable_state.as_deref() == Some("queued");
        // `mergeable` alone only rules out conflicts; only a "clean"
        // `mergeable_state` means the forge would actually accept the merge
        // (blocked/behind/dirty/unstable/unknown/absent all read `pr_open`).
        let clean = pr.mergeable == Some(true) && pr.mergeable_state.as_deref() == Some("clean");
        return if queued && !draft {
            WorkspaceDisplayStatus::PrQueued
        } else if clean && !draft {
            WorkspaceDisplayStatus::PrReady
        } else {
            WorkspaceDisplayStatus::PrOpen
        };
    }
    // Agent-monitored PRs are the same rung as the linked open PR above: an
    // ACTIVE monitor on an open PR reads `pr_queued`/`pr_ready`/`pr_open`
    // even when the PR belongs to another repo and never enters the
    // workspace linkage. A linked open PR wins first only because it carries
    // richer data; the mapping is identical.
    if monitor_prs.queued {
        return WorkspaceDisplayStatus::PrQueued;
    }
    if monitor_prs.ready {
        return WorkspaceDisplayStatus::PrReady;
    }
    if monitor_prs.open {
        return WorkspaceDisplayStatus::PrOpen;
    }
    if matches!(
        pr_status,
        Some(PullRequestStatus::Open | PullRequestStatus::Draft)
    ) {
        return WorkspaceDisplayStatus::PrOpen;
    }
    let (total, completed, in_progress) = task_stats
        .map(|s| (s.total, s.completed, s.in_progress))
        .unwrap_or_default();
    if total > 0 && completed < total {
        return if in_progress > 0 || completed > 0 {
            WorkspaceDisplayStatus::InProgress
        } else {
            WorkspaceDisplayStatus::NotStarted
        };
    }
    let latest_pr = active_pr.or_else(|| {
        pull_requests
            .iter()
            .max_by(|a, b| a.updated_at.cmp(&b.updated_at))
    });
    if latest_pr.map(|pr| pr.status) == Some(PullRequestStatus::Merged)
        || pr_status == Some(PullRequestStatus::Merged)
        || monitor_prs.merged
    {
        return WorkspaceDisplayStatus::PrMerged;
    }
    if total > 0 && completed == total {
        return WorkspaceDisplayStatus::Complete;
    }
    WorkspaceDisplayStatus::NotStarted
}

/// Build a `workspace:displayStatus-changed` change event with the
/// self-sufficient payload `{ workspaceId, displayStatus }` (PROTOCOL §6.5).
/// Private to this module: the only emitter is
/// [`Services::maybe_emit_display_status_changed`].
fn display_status_changed_event(
    workspace_id: &WorkspaceId,
    display_status: WorkspaceDisplayStatus,
) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_DISPLAY_STATUS_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "displayStatus": display_status,
        }),
    }
}

/// Build a `workspace:waiting-changed` change event with the self-sufficient
/// payload `{ workspaceId, waiting }` (PROTOCOL §6.5 / §6.7). Private to
/// this module: the only emitter is
/// [`Services::maybe_emit_waiting_changed`].
fn waiting_changed_event(workspace_id: &WorkspaceId, waiting: bool) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_WAITING_CHANGED.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "waiting": waiting,
        }),
    }
}

/// Unit tests for the pure `compute_display_status` derivation (canonical
/// precedence): failed → blocked → `needs_attention` → running agent →
/// active/latest open PR (linked or monitor-signalled) → open tasks →
/// merged PR → `complete/not_started`.
#[cfg(test)]
mod display_status {
    use intent_core::{
        PullRequestInfo, PullRequestStatus, WorkspaceDisplayStatus, WorkspaceTaskStats,
    };

    use super::{AttentionSignals, MonitorPrSignals};

    /// The pre-monitor-signals shape most tests use: no monitor signals.
    /// Monitor-specific tests call [`super::compute_display_status`]
    /// directly.
    fn compute_display_status(
        signals: AttentionSignals,
        agent_running: bool,
        active_pr: Option<&PullRequestInfo>,
        pull_requests: &[PullRequestInfo],
        pr_status: Option<PullRequestStatus>,
        task_stats: Option<&WorkspaceTaskStats>,
    ) -> WorkspaceDisplayStatus {
        super::compute_display_status(
            signals,
            agent_running,
            active_pr,
            pull_requests,
            pr_status,
            MonitorPrSignals::default(),
            task_stats,
        )
    }

    /// Legacy-shaped signal bundle: only the `needs_attention` axis set.
    fn sig(needs_attention: bool) -> AttentionSignals {
        AttentionSignals {
            needs_attention,
            ..AttentionSignals::default()
        }
    }

    fn failed() -> AttentionSignals {
        AttentionSignals {
            failed: true,
            ..AttentionSignals::default()
        }
    }

    fn blocked() -> AttentionSignals {
        AttentionSignals {
            blocked: true,
            ..AttentionSignals::default()
        }
    }

    fn pr(status: PullRequestStatus, updated_at: &str) -> PullRequestInfo {
        PullRequestInfo {
            id: format!("pr-{updated_at}"),
            number: 1,
            url: "https://github.com/o/r/pull/1".to_string(),
            title: "PR".to_string(),
            status,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: updated_at.to_string(),
            base_ref: None,
            head_ref: None,
            head_sha: None,
            author: None,
            mergeable: None,
            mergeable_state: None,
            is_draft: None,
        }
    }

    fn stats(total: usize, completed: usize, in_progress: usize) -> WorkspaceTaskStats {
        WorkspaceTaskStats {
            total,
            completed,
            in_progress,
        }
    }

    #[test]
    fn no_prs_no_tasks_is_idle() {
        assert_eq!(
            compute_display_status(sig(false), false, None, &[], None, None),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(sig(false), false, None, &[], None, Some(&stats(0, 0, 0))),
            WorkspaceDisplayStatus::Idle
        );
    }

    #[test]
    fn no_prs_task_stage_demotes_to_idle_without_agent() {
        // The base rollup is in_progress / not_started, but without a
        // running agent the task-stage statuses demote to idle.
        assert_eq!(
            compute_display_status(sig(false), false, None, &[], None, Some(&stats(3, 0, 0))),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(sig(false), false, None, &[], None, Some(&stats(3, 0, 1))),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(sig(false), false, None, &[], None, Some(&stats(3, 1, 0))),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(sig(false), false, None, &[], None, Some(&stats(3, 3, 0))),
            WorkspaceDisplayStatus::Complete
        );
    }

    #[test]
    fn running_agent_promotes_to_in_progress_unconditionally() {
        // A live agent wins over every PR/task rollup.
        assert_eq!(
            compute_display_status(sig(false), true, None, &[], None, None),
            WorkspaceDisplayStatus::InProgress
        );
        assert_eq!(
            compute_display_status(sig(false), true, None, &[], None, Some(&stats(3, 3, 0))),
            WorkspaceDisplayStatus::InProgress
        );
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        ready.mergeable_state = Some("clean".into());
        assert_eq!(
            compute_display_status(sig(false), true, Some(&ready), &[], None, None),
            WorkspaceDisplayStatus::InProgress
        );
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(sig(false), true, Some(&merged), &[], None, None),
            WorkspaceDisplayStatus::InProgress
        );
    }

    #[test]
    fn needs_attention_wins_over_everything() {
        // Step 0: the needs-attention signal outranks a running agent, every
        // PR stage, and every task rollup.
        assert_eq!(
            compute_display_status(sig(true), false, None, &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            compute_display_status(sig(true), true, None, &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        ready.mergeable_state = Some("clean".into());
        assert_eq!(
            compute_display_status(sig(true), false, Some(&ready), &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(
                sig(true),
                true,
                Some(&merged),
                &[],
                None,
                Some(&stats(3, 3, 0))
            ),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            compute_display_status(
                sig(true),
                false,
                None,
                &[],
                Some(PullRequestStatus::Open),
                Some(&stats(3, 1, 1))
            ),
            WorkspaceDisplayStatus::NeedsAttention
        );
    }

    #[test]
    fn failed_outranks_blocked() {
        // Precedence boundary: failed > blocked — with both axes set the
        // rollup reads failed.
        let both = AttentionSignals {
            failed: true,
            blocked: true,
            ..AttentionSignals::default()
        };
        assert_eq!(
            compute_display_status(both, false, None, &[], None, None),
            WorkspaceDisplayStatus::Failed
        );
        // Alone, each axis yields its own status.
        assert_eq!(
            compute_display_status(failed(), false, None, &[], None, None),
            WorkspaceDisplayStatus::Failed
        );
        assert_eq!(
            compute_display_status(blocked(), false, None, &[], None, None),
            WorkspaceDisplayStatus::Blocked
        );
    }

    #[test]
    fn blocked_outranks_needs_attention() {
        // Precedence boundary: blocked > needs_attention.
        let both = AttentionSignals {
            blocked: true,
            needs_attention: true,
            ..AttentionSignals::default()
        };
        assert_eq!(
            compute_display_status(both, false, None, &[], None, None),
            WorkspaceDisplayStatus::Blocked
        );
    }

    #[test]
    fn failed_and_blocked_win_over_everything_below() {
        // failed/blocked outrank a running agent, every PR stage, and every
        // task rollup.
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        ready.mergeable_state = Some("clean".into());
        for (signals, expected) in [
            (failed(), WorkspaceDisplayStatus::Failed),
            (blocked(), WorkspaceDisplayStatus::Blocked),
        ] {
            assert_eq!(
                compute_display_status(signals, true, None, &[], None, None),
                expected
            );
            assert_eq!(
                compute_display_status(signals, false, Some(&ready), &[], None, None),
                expected
            );
            assert_eq!(
                compute_display_status(signals, false, None, &[], None, Some(&stats(3, 3, 0))),
                expected
            );
        }
    }

    #[test]
    fn needs_attention_outranks_running_agent() {
        // Precedence boundary: needs_attention > in_progress (running agent).
        assert_eq!(
            compute_display_status(sig(true), true, None, &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            compute_display_status(sig(true), false, None, &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
    }

    #[test]
    fn pr_stages_and_complete_pass_through_without_agent() {
        // The idle demotion only applies to the task-stage rollups; PR
        // stages and complete are untouched.
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        ready.mergeable_state = Some("clean".into());
        assert_eq!(
            compute_display_status(sig(false), false, Some(&ready), &[], None, None),
            WorkspaceDisplayStatus::PrReady
        );
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(sig(false), false, Some(&merged), &[], None, None),
            WorkspaceDisplayStatus::PrMerged
        );
        assert_eq!(
            compute_display_status(sig(false), false, None, &[], None, Some(&stats(2, 2, 0))),
            WorkspaceDisplayStatus::Complete
        );
    }

    #[test]
    fn open_active_pr_mergeable_is_pr_ready() {
        let mut open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        open.mergeable = Some(true);
        open.mergeable_state = Some("clean".into());
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&open),
                &[],
                None,
                Some(&stats(2, 0, 1))
            ),
            WorkspaceDisplayStatus::PrReady
        );
    }

    /// Regression (observed with intent-hq/intentd#1350): GitHub reports
    /// `mergeable: true` for a PR still blocked by required checks or
    /// reviews — only `mergeable_state == "clean"` reads as truly
    /// mergeable. Any other or missing state derives `pr_open`.
    #[test]
    fn open_active_pr_mergeable_but_not_clean_is_pr_open() {
        for state in ["blocked", "behind", "dirty", "unstable", "unknown"] {
            let mut open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
            open.mergeable = Some(true);
            open.mergeable_state = Some(state.into());
            assert_eq!(
                compute_display_status(sig(false), false, Some(&open), &[], None, None),
                WorkspaceDisplayStatus::PrOpen,
                "mergeable_state {state}"
            );
        }
        // Absent `mergeable_state` is conservative too: never `pr_ready`.
        let mut open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        open.mergeable = Some(true);
        assert_eq!(
            compute_display_status(sig(false), false, Some(&open), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
    }

    #[test]
    fn open_active_pr_not_mergeable_or_draft_is_pr_open() {
        let open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(sig(false), false, Some(&open), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
        let mut draft = pr(PullRequestStatus::Draft, "2026-01-02T00:00:00Z");
        draft.mergeable = Some(true);
        draft.mergeable_state = Some("clean".into());
        assert_eq!(
            compute_display_status(sig(false), false, Some(&draft), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
        let mut flagged = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        flagged.mergeable = Some(true);
        flagged.mergeable_state = Some("clean".into());
        flagged.is_draft = Some(true);
        assert_eq!(
            compute_display_status(sig(false), false, Some(&flagged), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
    }

    /// A linked open PR sitting in the merge queue (REST
    /// `mergeable_state: "queued"`) reads `pr_queued` — regardless of the
    /// `mergeable` flag, and outranking the `clean` → `pr_ready` mapping —
    /// while a draft never reads queued.
    #[test]
    fn open_active_pr_in_merge_queue_is_pr_queued() {
        for mergeable in [Some(true), Some(false), None] {
            let mut queued = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
            queued.mergeable = mergeable;
            queued.mergeable_state = Some("queued".into());
            assert_eq!(
                compute_display_status(
                    sig(false),
                    false,
                    Some(&queued),
                    &[],
                    None,
                    Some(&stats(2, 2, 0))
                ),
                WorkspaceDisplayStatus::PrQueued,
                "mergeable {mergeable:?}"
            );
        }
        // Found via the `pullRequests` scan too, not just the linked PR.
        let mut queued = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        queued.mergeable_state = Some("queued".into());
        assert_eq!(
            compute_display_status(sig(false), false, None, &[queued], None, None),
            WorkspaceDisplayStatus::PrQueued
        );
        // Drafts never read queued.
        let mut draft = pr(PullRequestStatus::Draft, "2026-01-02T00:00:00Z");
        draft.mergeable_state = Some("queued".into());
        assert_eq!(
            compute_display_status(sig(false), false, Some(&draft), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
        let mut flagged = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        flagged.mergeable_state = Some("queued".into());
        flagged.is_draft = Some(true);
        assert_eq!(
            compute_display_status(sig(false), false, Some(&flagged), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
        // Attention axes and a running agent still outrank a queued PR.
        let mut queued = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        queued.mergeable_state = Some("queued".into());
        assert_eq!(
            compute_display_status(sig(true), false, Some(&queued), &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            compute_display_status(sig(false), true, Some(&queued), &[], None, None),
            WorkspaceDisplayStatus::InProgress
        );
    }

    #[test]
    fn merged_pr_never_masks_open_tasks() {
        // Open tasks keep the rollup off pr_merged; without a running agent
        // the resulting task-stage status reads as idle.
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&merged),
                std::slice::from_ref(&merged),
                None,
                Some(&stats(3, 1, 1))
            ),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&merged),
                std::slice::from_ref(&merged),
                None,
                Some(&stats(3, 0, 0))
            ),
            WorkspaceDisplayStatus::Idle
        );
    }

    #[test]
    fn merged_pr_never_masks_open_pr_in_list() {
        let merged = pr(PullRequestStatus::Merged, "2026-01-03T00:00:00Z");
        let open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        let list = vec![merged.clone(), open.clone()];
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&merged),
                &list,
                None,
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrOpen
        );
        let mut ready = open;
        ready.mergeable = Some(true);
        ready.mergeable_state = Some("clean".into());
        let list = vec![merged.clone(), ready];
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&merged),
                &list,
                None,
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrReady
        );
    }

    #[test]
    fn open_pr_fallback_picks_most_recently_updated() {
        let stale = pr(PullRequestStatus::Open, "2026-01-01T00:00:00Z");
        let mut fresh = pr(PullRequestStatus::Open, "2026-01-05T00:00:00Z");
        fresh.mergeable = Some(true);
        fresh.mergeable_state = Some("clean".into());
        let list = vec![stale, fresh];
        assert_eq!(
            compute_display_status(sig(false), false, None, &list, None, None),
            WorkspaceDisplayStatus::PrReady
        );
    }

    #[test]
    fn merged_with_all_tasks_done_is_pr_merged() {
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&merged),
                &[],
                None,
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrMerged
        );
        assert_eq!(
            compute_display_status(sig(false), false, Some(&merged), &[], None, None),
            WorkspaceDisplayStatus::PrMerged
        );
    }

    #[test]
    fn merged_latest_from_list_without_active_pr() {
        let closed = pr(PullRequestStatus::Closed, "2026-01-01T00:00:00Z");
        let merged = pr(PullRequestStatus::Merged, "2026-01-04T00:00:00Z");
        let list = vec![closed, merged];
        assert_eq!(
            compute_display_status(sig(false), false, None, &list, None, None),
            WorkspaceDisplayStatus::PrMerged
        );
    }

    #[test]
    fn closed_pr_falls_through_to_task_logic() {
        let closed = pr(PullRequestStatus::Closed, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&closed),
                &[],
                None,
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::Complete
        );
        assert_eq!(
            compute_display_status(sig(false), false, Some(&closed), &[], None, None),
            WorkspaceDisplayStatus::Idle
        );
    }

    #[test]
    fn pr_status_open_or_draft_without_pr_objects_is_pr_open() {
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Open),
                None
            ),
            WorkspaceDisplayStatus::PrOpen
        );
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Draft),
                None
            ),
            WorkspaceDisplayStatus::PrOpen
        );
    }

    #[test]
    fn pr_status_open_or_draft_wins_over_open_tasks() {
        // The pr_status fallback is a step-1 PR-stage signal: it precedes the
        // open-tasks check, so in-progress or not-started tasks never mask it.
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Open),
                Some(&stats(3, 1, 1))
            ),
            WorkspaceDisplayStatus::PrOpen
        );
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Open),
                Some(&stats(3, 0, 0))
            ),
            WorkspaceDisplayStatus::PrOpen
        );
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Draft),
                Some(&stats(3, 1, 1))
            ),
            WorkspaceDisplayStatus::PrOpen
        );
    }

    #[test]
    fn pr_status_merged_participates_in_merged_check() {
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Merged),
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrMerged
        );
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Merged),
                None
            ),
            WorkspaceDisplayStatus::PrMerged
        );
    }

    #[test]
    fn pr_status_merged_never_masks_open_tasks() {
        // Open tasks keep the rollup off pr_merged; without a running agent
        // the task-stage status reads as idle.
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &[],
                Some(PullRequestStatus::Merged),
                Some(&stats(3, 1, 1))
            ),
            WorkspaceDisplayStatus::Idle
        );
    }

    #[test]
    fn pr_info_objects_take_precedence_over_pr_status() {
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        ready.mergeable_state = Some("clean".into());
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                Some(&ready),
                &[],
                Some(PullRequestStatus::Merged),
                None
            ),
            WorkspaceDisplayStatus::PrReady
        );
        let list = vec![ready];
        assert_eq!(
            compute_display_status(
                sig(false),
                false,
                None,
                &list,
                Some(PullRequestStatus::Merged),
                None
            ),
            WorkspaceDisplayStatus::PrReady
        );
    }

    /// Monitor-signal shorthand for the tests below.
    fn monitors(open: bool, ready: bool, merged: bool) -> MonitorPrSignals {
        MonitorPrSignals {
            queued: false,
            ready,
            open,
            merged,
        }
    }

    /// Step 4 via monitors: an ACTIVE monitor on an open PR sitting in the
    /// merge queue yields `pr_queued`, outranking `ready` on the same rung
    /// (a queued PR is still checklist-blocked in practice, but a stale
    /// `ready` never wins over `queued`) — while attention axes, a running
    /// agent, and a linked open PR keep their precedence.
    #[test]
    fn active_monitor_queued_pr_is_pr_queued() {
        let queued = MonitorPrSignals {
            queued: true,
            ..monitors(true, false, false)
        };
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                None,
                &[],
                None,
                queued,
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrQueued
        );
        let queued_and_ready = MonitorPrSignals {
            queued: true,
            ..monitors(true, true, true)
        };
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                None,
                &[],
                None,
                queued_and_ready,
                None
            ),
            WorkspaceDisplayStatus::PrQueued
        );
        assert_eq!(
            super::compute_display_status(sig(true), false, None, &[], None, queued, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            super::compute_display_status(sig(false), true, None, &[], None, queued, None),
            WorkspaceDisplayStatus::InProgress
        );
        // A linked open PR wins the shared rung even over a queued monitor.
        let open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        assert_eq!(
            super::compute_display_status(sig(false), false, Some(&open), &[], None, queued, None),
            WorkspaceDisplayStatus::PrOpen
        );
    }

    /// Step 4 via monitors: an ACTIVE monitor on an open PR yields
    /// `pr_open` (or `pr_ready` when the snapshot says mergeable, not
    /// draft) even when the workspace has no PR linkage at all and every
    /// task is complete — the cross-repo watch case.
    #[test]
    fn active_monitor_open_pr_is_pr_open_or_pr_ready() {
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                None,
                &[],
                None,
                monitors(true, false, false),
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrOpen
        );
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                None,
                &[],
                None,
                monitors(true, true, false),
                None
            ),
            WorkspaceDisplayStatus::PrReady
        );
    }

    /// Step 6 via monitors: a COMPLETED monitor whose final snapshot shows
    /// the PR merged reads `pr_merged` once all tasks are done — but open
    /// tasks still mask it (step 5 precedes the merged check), and an
    /// open-PR monitor signal outranks it on the same inputs.
    #[test]
    fn completed_merged_monitor_is_pr_merged_after_tasks_complete() {
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                None,
                &[],
                None,
                monitors(false, false, true),
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrMerged
        );
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                None,
                &[],
                None,
                monitors(false, false, true),
                Some(&stats(3, 1, 1))
            ),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                None,
                &[],
                None,
                monitors(true, false, true),
                None
            ),
            WorkspaceDisplayStatus::PrOpen
        );
    }

    /// Precedence unchanged above the PR rungs: attention axes and a
    /// running agent still outrank every monitor signal.
    #[test]
    fn attention_and_running_agent_outrank_monitor_signals() {
        assert_eq!(
            super::compute_display_status(
                sig(true),
                false,
                None,
                &[],
                None,
                monitors(true, true, false),
                None
            ),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            super::compute_display_status(
                sig(false),
                true,
                None,
                &[],
                None,
                monitors(true, true, false),
                None
            ),
            WorkspaceDisplayStatus::InProgress
        );
    }

    /// A linked open PR and an open-monitor signal are the same rung: the
    /// linked PR's richer mapping wins first, so a non-mergeable linked PR
    /// reads `pr_open` even when a monitor signals ready.
    #[test]
    fn linked_open_pr_wins_the_shared_rung() {
        let open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        assert_eq!(
            super::compute_display_status(
                sig(false),
                false,
                Some(&open),
                &[],
                None,
                monitors(true, true, false),
                None
            ),
            WorkspaceDisplayStatus::PrOpen
        );
    }
}

/// Per-workspace attention-axis probe (`Services::workspace_attention_signals`,
/// PROTOCOL §6.5): `needs_attention` is true iff a **top-level** session (no
/// parent, not background, not deleted) carries a pending discussion request
/// or pending structured questions (or the workspace flag is
/// `review_required`); `blocked` iff one carries a pending blocker request;
/// `failed` iff one is parked in `error`. The `unread` workspace flag never
/// feeds the signals. Child/background/deleted sessions never count.
#[cfg(test)]
mod workspace_needs_attention {
    use intent_core::{
        now_iso, AgentId, AgentSession, AgentStatus, WorkspaceAttention, WorkspaceId,
    };
    use intent_store::Store;
    use serde_json::json;

    use super::AttentionSignals;
    use crate::tests::{workspace, TempDb};
    use crate::Services;

    /// Probe the session-derived axes with a `None` workspace flag.
    async fn signals(svc: &Services, ws: &WorkspaceId) -> AttentionSignals {
        svc.workspace_attention_signals(ws, WorkspaceAttention::None, None)
            .await
    }

    pub(super) fn mk_session(ws: &WorkspaceId, id: &str) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: id.to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Waiting,
            is_active: true,
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
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
        }
    }

    /// Assistant content carrying one structured-question resource block
    /// (the shape `question_block_count` matches).
    pub(super) fn question_content() -> serde_json::Value {
        json!([{
            "type": "resource",
            "resource": {
                "mimeType": intent_acp::mcp_server::QUESTION_RESOURCE_MIME_TYPE,
                "uri": "question://q-1",
                "text": "{\"questions\":[]}"
            }
        }])
    }

    async fn setup() -> (Services, WorkspaceId, TempDb) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        (Services::new(store), ws, tmp)
    }

    #[tokio::test]
    async fn no_sessions_is_all_false() {
        let (svc, ws, _tmp) = setup().await;
        assert_eq!(signals(&svc, &ws).await, AttentionSignals::default());
    }

    #[tokio::test]
    async fn plain_top_level_session_is_all_false() {
        let (svc, ws, _tmp) = setup().await;
        svc.store
            .insert_agent_session(&mk_session(&ws, "agent-plain"))
            .await
            .unwrap();
        assert_eq!(signals(&svc, &ws).await, AttentionSignals::default());
    }

    #[tokio::test]
    async fn top_level_attention_requests_split_by_kind() {
        // A discussion request drives needs_attention; a blocker drives
        // blocked — the axes are independent.
        let (svc, ws, _tmp) = setup().await;
        let mut discuss = mk_session(&ws, "agent-discussion");
        discuss.attention_request_kind = Some("discussion".to_string());
        svc.store.insert_agent_session(&discuss).await.unwrap();
        let s = signals(&svc, &ws).await;
        assert!(s.needs_attention);
        assert!(!s.blocked);

        let mut blocker = mk_session(&ws, "agent-blocker");
        blocker.attention_request_kind = Some("blocker".to_string());
        svc.store.insert_agent_session(&blocker).await.unwrap();
        let s = signals(&svc, &ws).await;
        assert!(s.needs_attention);
        assert!(s.blocked);
        assert!(!s.failed);
    }

    #[tokio::test]
    async fn top_level_error_session_is_failed() {
        let (svc, ws, _tmp) = setup().await;
        let mut errored = mk_session(&ws, "agent-error");
        errored.status = AgentStatus::Error;
        svc.store.insert_agent_session(&errored).await.unwrap();
        let s = signals(&svc, &ws).await;
        assert!(s.failed);
        assert!(!s.blocked);
        assert!(!s.needs_attention);
    }

    #[tokio::test]
    async fn workspace_attention_flag_maps_to_axes() {
        // review_required → needs_attention (flag-only signal: no sessions
        // required); unread feeds nothing — the flag is not a displayStatus
        // axis.
        let (svc, ws, _tmp) = setup().await;
        let s = svc
            .workspace_attention_signals(&ws, WorkspaceAttention::ReviewRequired, None)
            .await;
        assert!(s.needs_attention);
        let s = svc
            .workspace_attention_signals(&ws, WorkspaceAttention::Unread, None)
            .await;
        assert_eq!(s, AttentionSignals::default());
    }

    #[tokio::test]
    async fn delegated_background_or_deleted_sessions_never_count() {
        let (svc, ws, _tmp) = setup().await;
        let mut child = mk_session(&ws, "agent-child");
        child.parent_agent_id = Some(AgentId::from("agent-parent"));
        child.attention_request_kind = Some("blocker".to_string());
        svc.store.insert_agent_session(&child).await.unwrap();

        let mut background = mk_session(&ws, "agent-bg");
        background.is_background = true;
        background.attention_request_kind = Some("discussion".to_string());
        svc.store.insert_agent_session(&background).await.unwrap();

        let mut deleted = mk_session(&ws, "agent-deleted");
        deleted.status = AgentStatus::Deleted;
        deleted.attention_request_kind = Some("discussion".to_string());
        svc.store.insert_agent_session(&deleted).await.unwrap();

        // A failed child/background session never drives `failed` either.
        let mut failed_child = mk_session(&ws, "agent-failed-child");
        failed_child.parent_agent_id = Some(AgentId::from("agent-parent"));
        failed_child.status = AgentStatus::Error;
        svc.store.insert_agent_session(&failed_child).await.unwrap();

        let mut failed_bg = mk_session(&ws, "agent-failed-bg");
        failed_bg.is_background = true;
        failed_bg.status = AgentStatus::Error;
        svc.store.insert_agent_session(&failed_bg).await.unwrap();

        assert_eq!(signals(&svc, &ws).await, AttentionSignals::default());
    }

    #[tokio::test]
    async fn pending_questions_on_top_level_session_is_needs_attention() {
        let (svc, ws, _tmp) = setup().await;
        let session = mk_session(&ws, "agent-q");
        svc.store.insert_agent_session(&session).await.unwrap();
        svc.store
            .append_agent_message(&session.id, "assistant", &question_content(), &now_iso())
            .await
            .unwrap();
        assert!(signals(&svc, &ws).await.needs_attention);
    }

    #[tokio::test]
    async fn superseded_or_dismissed_questions_are_false() {
        let (svc, ws, _tmp) = setup().await;

        // Pre-upgrade session (no pending-questions marker): the legacy
        // tail-walk fallback still reads a trailing user reply as resolving.
        let answered = mk_session(&ws, "agent-answered");
        svc.store.insert_agent_session(&answered).await.unwrap();
        svc.store
            .append_agent_message(&answered.id, "assistant", &question_content(), &now_iso())
            .await
            .unwrap();
        svc.store
            .append_agent_message(
                &answered.id,
                "user",
                &json!([{ "type": "text", "text": "answer" }]),
                &now_iso(),
            )
            .await
            .unwrap();

        // A persisted dismissal marker for the question message id.
        let dismissed = mk_session(&ws, "agent-dismissed");
        svc.store.insert_agent_session(&dismissed).await.unwrap();
        let msg = svc
            .store
            .append_agent_message(&dismissed.id, "assistant", &question_content(), &now_iso())
            .await
            .unwrap();
        let mut updated = dismissed.clone();
        updated.metadata = Some(json!({
            (intent_core::DISMISSED_QUESTIONS_MESSAGE_ID_KEY): msg.id
        }));
        svc.store.update_agent_session(&ws, &updated).await.unwrap();

        assert!(!signals(&svc, &ws).await.needs_attention);
    }

    /// A store read failure fails open (list/get emission must never be
    /// wedged by the attention probe): session-derived axes read `false`,
    /// while the flag-derived axes survive (no store read involved).
    #[tokio::test]
    async fn store_read_failure_fails_open() {
        let (svc, ws, _tmp) = setup().await;
        let mut s = mk_session(&ws, "agent-attn");
        s.attention_request_kind = Some("blocker".to_string());
        svc.store.insert_agent_session(&s).await.unwrap();
        assert!(signals(&svc, &ws).await.blocked);

        // Force list_agent_session_summaries to fail.
        sqlx::query("DROP TABLE agent_session")
            .execute(svc.store.write_pool())
            .await
            .expect("drop agent_session table");
        assert_eq!(signals(&svc, &ws).await, AttentionSignals::default());
        let s = svc
            .workspace_attention_signals(&ws, WorkspaceAttention::ReviewRequired, None)
            .await;
        assert!(
            s.needs_attention,
            "flag-derived axes survive a store read failure"
        );
    }

    /// Caller-supplied session summaries are authoritative (monorepo#3058):
    /// the probe derives from the given slice with no store re-read, so the
    /// hot list/get emit path reuses its `agentSummary`/`lastActivity` fetch.
    /// Proven by dropping the table first — a `None` caller would fail open
    /// to defaults, while the supplied slice still yields the signals.
    #[tokio::test]
    async fn supplied_sessions_skip_the_store_read() {
        let (svc, ws, _tmp) = setup().await;
        let mut s = mk_session(&ws, "agent-attn");
        s.attention_request_kind = Some("blocker".to_string());
        sqlx::query("DROP TABLE agent_session")
            .execute(svc.store.write_pool())
            .await
            .expect("drop agent_session table");
        let sessions = vec![s];
        let got = svc
            .workspace_attention_signals(&ws, WorkspaceAttention::None, Some(&sessions))
            .await;
        assert!(
            got.blocked,
            "derived from the supplied slice, no store read"
        );
    }

    /// A written pending-questions marker on the summary decides the
    /// pending set inline (monorepo#3058): no per-session store probe, so
    /// pendingness reads correctly even with the message log unavailable. Set
    /// marker → pending; marker matching the dismissal → not pending; cleared
    /// (empty-written) marker → not pending and NO tail-walk fallback.
    #[tokio::test]
    async fn written_markers_decide_pending_questions_without_store_reads() {
        let (svc, ws, _tmp) = setup().await;
        let pending = mk_session(&ws, "agent-pending");
        let mut pending = pending;
        pending.metadata = Some(json!({
            (intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY): "msg-1"
        }));
        let mut resolved = mk_session(&ws, "agent-resolved");
        resolved.metadata = Some(json!({
            (intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY): "msg-2",
            (intent_core::DISMISSED_QUESTIONS_MESSAGE_ID_KEY): "msg-2"
        }));
        let mut cleared = mk_session(&ws, "agent-cleared");
        cleared.metadata = Some(json!({
            (intent_core::PENDING_QUESTIONS_MESSAGE_ID_KEY): ""
        }));

        // No sessions/messages persisted at all: every derivation below runs
        // off the supplied summaries alone.
        let holds = svc
            .workspace_attention_signals(
                &ws,
                WorkspaceAttention::None,
                Some(std::slice::from_ref(&pending)),
            )
            .await;
        assert!(holds.needs_attention, "set marker is pending");
        for (name, session) in [("resolved", resolved), ("cleared", cleared)] {
            let s = svc
                .workspace_attention_signals(
                    &ws,
                    WorkspaceAttention::None,
                    Some(std::slice::from_ref(&session)),
                )
                .await;
            assert!(!s.needs_attention, "{name} marker must not be pending");
        }
    }
}

/// Transition-only emission of `workspace:displayStatus-changed` (PROTOCOL
/// §6.5): the recompute-and-compare seam behind
/// `maybe_emit_display_status_changed` publishes exactly on a derived-status
/// transition — a first observation seeds without emitting and a no-op
/// recompute stays silent.
#[cfg(test)]
mod display_status_events {
    use std::time::Duration;

    use intent_core::{
        now_iso, ContentType, Note, NoteId, NoteMetadata, NoteVisibility, TaskMetadata, TaskStatus,
        WorkspaceApi, WorkspaceDisplayStatus, WorkspaceId,
    };
    use intent_store::Store;
    use serde_json::{json, Value};

    use super::DisplayStatusCache;
    use crate::tests::{workspace, DebounceEnvGuard, TempDb};
    use crate::{EventBus, Services, Subscription, SubscriptionFilter};

    struct Harness {
        _tmp: TempDb,
        store: Store,
        services: Services,
        bus: EventBus,
        ws: WorkspaceId,
    }

    async fn harness() -> Harness {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone()).with_event_bus(bus.clone());
        Harness {
            _tmp: tmp,
            store,
            services,
            bus,
            ws,
        }
    }

    /// Direct child task note of the spec, so it counts into `taskStats`.
    fn task_note(ws: &WorkspaceId, id: &str, status: TaskStatus) -> Note {
        let ts = now_iso();
        Note {
            id: NoteId::from(id),
            workspace_id: ws.clone(),
            title: format!("Task {id}"),
            content: String::new(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: Some(NoteId::from("spec")),
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata {
                task: Some(TaskMetadata {
                    status,
                    ..Default::default()
                }),
            },
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        }
    }

    /// Subscribe to only `workspace:displayStatus-changed` for this workspace.
    fn subscribe(h: &Harness) -> Subscription {
        h.bus.subscribe(SubscriptionFilter {
            workspace_id: Some(h.ws.0.clone()),
            event_types: vec!["workspace:displayStatus-changed".to_string()],
            ..Default::default()
        })
    }

    async fn recv_one(sub: &mut Subscription) -> Value {
        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        assert_eq!(batch.len(), 1, "expected exactly one event");
        serde_json::to_value(&batch[0]).expect("serialize event")
    }

    async fn assert_silent(sub: &mut Subscription) {
        let res = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(res.is_err(), "expected no displayStatus event: {res:?}");
    }

    /// A task-completion transition (`in_progress` → complete over
    /// `task.updateNoteStatus`) emits the event with the self-sufficient
    /// `{ workspaceId, displayStatus }` payload.
    #[tokio::test]
    async fn task_completion_transition_emits() {
        let h = harness().await;
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::InProgress))
            .await
            .expect("insert task");
        // Seed the last-observed cache (first observation never emits).
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .task_update_note_status(
                h.ws.clone(),
                NoteId::from("t1"),
                "complete".to_string(),
                None,
                None,
            )
            .await
            .expect("update status");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(ev["workspaceId"], h.ws.0);
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "complete" })
        );
    }

    /// A task-status change that does not move the derived rollup (a second
    /// task flipping `not_started` → `in_progress` while the rollup is already
    /// `in_progress`) publishes no displayStatus event.
    #[tokio::test]
    async fn no_op_recompute_stays_silent() {
        let h = harness().await;
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::InProgress))
            .await
            .expect("insert t1");
        h.store
            .insert_note(&task_note(&h.ws, "t2", TaskStatus::NotStarted))
            .await
            .expect("insert t2");
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .task_update_note_status(
                h.ws.clone(),
                NoteId::from("t2"),
                "in_progress".to_string(),
                None,
                None,
            )
            .await
            .expect("update status");
        assert_silent(&mut sub).await;
    }

    /// The first observation for a workspace seeds the cache without emitting;
    /// the next actual transition emits.
    #[tokio::test]
    async fn first_observation_seeds_without_emitting() {
        let h = harness().await;
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::InProgress))
            .await
            .expect("insert task");

        let mut sub = subscribe(&h);
        h.services.maybe_emit_display_status_changed(&h.ws).await;
        assert_silent(&mut sub).await;

        // Repeat recompute with no underlying change: still silent.
        h.services.maybe_emit_display_status_changed(&h.ws).await;
        assert_silent(&mut sub).await;
    }

    /// The lite list path (workspace.subscribe seq-0 snapshot) seeds the
    /// last-observed baseline the same way the enriched path does — a seed
    /// never emits — so the first post-boot mutation emits the transition
    /// against that baseline.
    #[tokio::test]
    async fn lite_list_seeds_baseline_then_first_mutation_emits() {
        let h = harness().await;
        // Hermetic root: the lite path probes the workspaces root for
        // `cowSupported`, and tests must never touch `~/intent/workspaces`.
        let root = crate::tests::WorkspacesRoot::new();
        let services = h
            .services
            .clone()
            .with_workspaces_root(root.path().to_path_buf());
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::InProgress))
            .await
            .expect("insert task");

        let mut sub = subscribe(&h);
        let list = services.list_workspaces_lite(true).await.expect("lite");
        let row = list.iter().find(|w| w.id == h.ws).expect("row");
        assert!(row.display_status.is_some(), "lite row carries the status");
        assert_silent(&mut sub).await;

        // First post-boot mutation transitions against the seeded baseline.
        services
            .task_update_note_status(
                h.ws.clone(),
                NoteId::from("t1"),
                "complete".to_string(),
                None,
                None,
            )
            .await
            .expect("update status");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "complete" })
        );
    }

    /// Deleting a spec-child task note that moves the derived rollup
    /// (complete → idle once the only completed task is gone) emits the
    /// transition event: `note.delete` goes through the same
    /// recompute+maybe-emit hook as the task-status mutations.
    #[tokio::test]
    async fn task_note_delete_transition_emits() {
        let h = harness().await;
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::Complete))
            .await
            .expect("insert task");
        // Seed the last-observed cache (first observation never emits).
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .delete_note(h.ws.clone(), NoteId::from("t1"), None)
            .await
            .expect("delete note");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// Agent activity folds into the derivation: `agent_activity_begin`
    /// promotes the rollup to `in_progress` and emits the transition
    /// immediately; the debounced idle flip after `agent_activity_end`
    /// demotes it back to `idle` and emits again — in lockstep with the
    /// `workspace:activity-changed` debounce (no early idle emission).
    #[tokio::test]
    async fn agent_activity_transitions_emit() {
        let _guard = DebounceEnvGuard::new("500");
        let h = harness().await;
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::InProgress))
            .await
            .expect("insert task");
        // Seed the last-observed cache: no agent running → idle.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services.agent_activity_begin(&h.ws).await;
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "in_progress" })
        );

        // End the run: during the grace window the status stays in_progress
        // (workspace_activity still reports AgentRunning) — no event yet
        // (assert_silent's 300ms watch sits inside the 500ms window).
        h.services.agent_activity_end(&h.ws);
        assert_silent(&mut sub).await;

        // After the debounce window the demotion to idle emits.
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// When `taskStats` is unavailable (transient notes-read failure), the
    /// enrich path leaves `displayStatus` absent — clients fall back to local
    /// derivation on a missing field — and never seeds the last-observed
    /// cache from the stats-free compute.
    #[tokio::test]
    async fn enrich_omits_display_status_when_task_stats_unavailable() {
        let h = harness().await;
        // Hermetic root: enrichment probes the workspaces root for
        // `cowSupported`, and tests must never touch `~/intent/workspaces`.
        let root = tempfile::tempdir().expect("temp workspaces root");
        let services = h
            .services
            .clone()
            .with_workspaces_root(root.path().to_path_buf());
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::InProgress))
            .await
            .expect("insert task");
        let mut ws = h.store.get_workspace(&h.ws).await.expect("get ws");
        // Force list_notes to fail so taskStats is not computable.
        sqlx::query("DROP TABLE note")
            .execute(h.store.write_pool())
            .await
            .expect("drop note table");

        services.enrich_workspace_aggregates(&mut ws).await;
        assert!(ws.task_stats.is_none(), "taskStats must be absent");
        assert!(ws.display_status.is_none(), "displayStatus must be absent");
        let seeded = services.last_display_statuses.contains(&h.ws);
        assert!(
            !seeded,
            "cache must not be seeded from a stats-free compute"
        );
    }

    /// Attention raise/retire triggers (§6.5 step 0): a top-level
    /// `agent.requestAttention` raise promotes the derived rollup to
    /// `needs_attention` and emits; the turn-begin clear
    /// (`clear_attention_request_if_present`) retires it and emits the
    /// demotion.
    #[tokio::test]
    async fn attention_raise_and_retire_transitions_emit() {
        use std::sync::Arc;
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-attn");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        // Seed the last-observed cache (first observation never emits).
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .agent_request_attention_op(
                h.ws.clone(),
                "discussion".to_string(),
                "need user input".to_string(),
                Some(session.id.clone()),
            )
            .await
            .expect("raise attention");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "needs_attention" })
        );

        // Retire via the runtime's turn-begin clear hook.
        let sink: Arc<dyn intent_acp::EventSink> =
            Arc::new(crate::BusEventSink::new(h.bus.clone()));
        let manager = Arc::new(crate::agent_manager::AgentManager::new(
            h.services.clone(),
            sink,
            4,
        ));
        manager
            .clear_attention_request_if_present(&session.id, &h.ws)
            .await;
        let ev = recv_one(&mut sub).await;
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// G1: `agent.delete` recomputes the derived rollup — deleting the
    /// top-level agent whose pending attention request drives
    /// `needs_attention` emits the demotion.
    #[tokio::test]
    async fn agent_delete_transition_emits() {
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-del");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        h.store
            .set_attention_request(&h.ws, &session.id, "blocker", "stuck", &now_iso())
            .await
            .expect("set attention");
        // Seed: baseline needs_attention.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .agent_delete_op(session.id.clone(), Some(h.ws.clone()))
            .await
            .expect("delete agent");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// G2: `agent.update` recomputes when a status-relevant field changes —
    /// flipping the attention-holding agent to `isBackground: true` removes
    /// it from the `needs_attention` derivation and emits the demotion.
    #[tokio::test]
    async fn agent_update_is_background_transition_emits() {
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-bg");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        h.store
            .set_attention_request(&h.ws, &session.id, "discussion", "input", &now_iso())
            .await
            .expect("set attention");
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .agent_update_op(session.id.clone(), json!({ "isBackground": true }))
            .await
            .expect("update agent");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// Question-resolution trigger via `agent.dismissQuestions` (§6.5 step 0):
    /// persisting the dismissal marker retires the pending set and emits the
    /// `needs_attention` → idle demotion.
    #[tokio::test]
    async fn question_dismiss_transition_emits() {
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-q");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        let msg = h
            .store
            .append_agent_message(
                &session.id,
                "assistant",
                &super::workspace_needs_attention::question_content(),
                &now_iso(),
            )
            .await
            .expect("append question");
        // Seed: the pending question makes the baseline needs_attention.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .agent_dismiss_questions_op(h.ws.clone(), session.id.clone(), msg.id)
            .await
            .expect("dismiss questions");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// Bare spec note (no task metadata) whose body carries the task links
    /// that gate `taskStats`.
    fn spec_note(ws: &WorkspaceId, content: &str) -> Note {
        let ts = now_iso();
        Note {
            id: NoteId::from("spec"),
            workspace_id: ws.clone(),
            title: "Spec".to_string(),
            content: content.to_string(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: true,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        }
    }

    const LINK_T1: &str = "- [x] [Task t1](intent://local/task/t1)";
    const LINK_T2: &str = "- [ ] [Task t2](intent://local/task/t2)";

    /// G3: a spec-body write over `note.update` that changes the linked task
    /// set moves the link-gated `taskStats` rollup and emits the transition.
    #[tokio::test]
    async fn spec_body_update_transition_emits() {
        let h = harness().await;
        h.store
            .insert_note(&spec_note(&h.ws, LINK_T1))
            .await
            .expect("insert spec");
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::Complete))
            .await
            .expect("insert t1");
        h.store
            .insert_note(&task_note(&h.ws, "t2", TaskStatus::NotStarted))
            .await
            .expect("insert t2");
        // Baseline: only t1 is linked → all complete.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .update_note(
                h.ws.clone(),
                NoteId::from("spec"),
                intent_core::NoteUpdateInput {
                    content: Some(format!("{LINK_T1}\n{LINK_T2}")),
                    ..Default::default()
                },
            )
            .await
            .expect("update spec body");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// G4: `note.restoreVersion` on the spec re-gates `taskStats` from the
    /// restored body and emits the transition.
    #[tokio::test]
    async fn spec_restore_version_transition_emits() {
        let h = harness().await;
        h.store
            .insert_note(&spec_note(&h.ws, "empty"))
            .await
            .expect("insert spec");
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::Complete))
            .await
            .expect("insert t1");
        h.store
            .insert_note(&task_note(&h.ws, "t2", TaskStatus::NotStarted))
            .await
            .expect("insert t2");
        // v1 links only the complete task; v2 links both.
        h.services
            .update_note(
                h.ws.clone(),
                NoteId::from("spec"),
                intent_core::NoteUpdateInput {
                    content: Some(LINK_T1.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("write v1");
        h.services
            .update_note(
                h.ws.clone(),
                NoteId::from("spec"),
                intent_core::NoteUpdateInput {
                    content: Some(format!("{LINK_T1}\n{LINK_T2}")),
                    ..Default::default()
                },
            )
            .await
            .expect("write v2");
        // Baseline: both linked → open tasks remain → idle.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .restore_note_version(h.ws.clone(), NoteId::from("spec"), 1, None)
            .await
            .expect("restore v1");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "complete" })
        );
    }

    /// G5: a spec checkbox-line rewrite over `task.update` that strips a
    /// task link re-gates `taskStats` and emits the transition.
    #[tokio::test]
    async fn spec_task_line_update_transition_emits() {
        let h = harness().await;
        h.store
            .insert_note(&spec_note(&h.ws, &format!("{LINK_T1}\n{LINK_T2}")))
            .await
            .expect("insert spec");
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::Complete))
            .await
            .expect("insert t1");
        h.store
            .insert_note(&task_note(&h.ws, "t2", TaskStatus::NotStarted))
            .await
            .expect("insert t2");
        // Baseline: both linked → open tasks remain → idle.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .task_update(
                h.ws.clone(),
                NoteId::from("spec"),
                2,
                Some("plain text, link removed".to_string()),
                None,
                None,
                None,
            )
            .await
            .expect("rewrite checkbox line");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "complete" })
        );
    }

    /// G6: `task.createPrerequisite` with the spec as dependent adds a fresh
    /// open spec-child task and emits the transition.
    #[tokio::test]
    async fn create_prerequisite_on_spec_transition_emits() {
        let h = harness().await;
        h.store
            .insert_note(&spec_note(&h.ws, "no links"))
            .await
            .expect("insert spec");
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::Complete))
            .await
            .expect("insert t1");
        // Baseline: fallback mode, one complete child → complete.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .create_prerequisite(
                h.ws.clone(),
                NoteId::from("spec"),
                "Fresh prerequisite".to_string(),
                None,
                None,
                None,
            )
            .await
            .expect("create prerequisite");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// G7: `workspace.delete` evicts the last-observed baseline so the
    /// in-memory cache does not leak deleted-workspace entries.
    #[tokio::test]
    async fn workspace_delete_evicts_baseline() {
        let h = harness().await;
        // Hermetic root: the delete path sweeps the workspaces root, and
        // tests must never touch `~/intent/workspaces`.
        let root = crate::tests::WorkspacesRoot::new();
        let services = h
            .services
            .clone()
            .with_workspaces_root(root.path().to_path_buf());
        services.maybe_emit_display_status_changed(&h.ws).await;
        assert!(services.last_display_statuses.contains(&h.ws));

        services
            .delete_workspace(h.ws.clone())
            .await
            .expect("delete workspace");
        assert!(
            !services.last_display_statuses.contains(&h.ws),
            "deleted workspace's baseline must be evicted"
        );
    }

    /// Eviction-race guard (PR #928 review): a compute whose generation
    /// snapshot predates an eviction must drop its cache write — neither
    /// `record` nor `seed` may resurrect a baseline for the deleted id.
    #[tokio::test]
    async fn stale_compute_after_eviction_drops_write() {
        let cache = DisplayStatusCache::default();
        let ws = WorkspaceId::new();

        // In-flight recompute snapshots the generation, then the delete's
        // eviction lands before its write.
        let generation = cache.generation();
        cache.record(&ws, WorkspaceDisplayStatus::InProgress, generation);
        assert!(cache.contains(&ws));
        cache.evict(&ws);
        let stale = cache.record(&ws, WorkspaceDisplayStatus::Idle, generation);
        assert_eq!(stale, None, "stale record must be dropped, not compared");
        assert!(
            !cache.contains(&ws),
            "stale record must not resurrect the evicted baseline"
        );

        // Same for the read-path seed.
        cache.seed(&ws, WorkspaceDisplayStatus::Idle, generation);
        assert!(
            !cache.contains(&ws),
            "stale seed must not resurrect the evicted baseline"
        );

        // A fresh snapshot taken after the eviction writes normally (the
        // importer re-insert / post-delete read path seeds fresh).
        let fresh = cache.generation();
        assert_eq!(
            cache.record(&ws, WorkspaceDisplayStatus::Idle, fresh),
            Some(false),
            "fresh compute seeds without emitting"
        );
        assert!(cache.contains(&ws));

        // An unrelated eviction does not drop writes for ids whose entry
        // survives: the guard only bites when the id's own entry is gone.
        let generation = cache.generation();
        cache.evict(&WorkspaceId::new());
        assert_eq!(
            cache.record(&ws, WorkspaceDisplayStatus::Complete, generation),
            Some(true),
            "surviving entry still records a transition"
        );
    }

    /// G8: `workspace.update` carrying a PR field recomputes — a `prStatus`
    /// flip to open moves the derived rollup to `pr_open` and emits.
    #[tokio::test]
    async fn workspace_update_pr_status_transition_emits() {
        let h = harness().await;
        // Baseline: no tasks, no PR → not_started → idle.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .update_workspace(
                h.ws.clone(),
                intent_core::WorkspaceUpdate {
                    pr_status: Some(Some(intent_core::PullRequestStatus::Open)),
                    ..Default::default()
                },
            )
            .await
            .expect("update workspace");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "pr_open" })
        );

        // A non-PR update (title) does not probe: no event even if nothing
        // else changed (and no spurious emission either).
        h.services
            .update_workspace(
                h.ws.clone(),
                intent_core::WorkspaceUpdate {
                    title: Some("Renamed".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("rename workspace");
        assert_silent(&mut sub).await;
    }

    /// Cross-agent isolation: agent B's turn-begin clear
    /// (`clear_attention_request_if_present`) must not retire agent A's
    /// pending request — the workspace stays `needs_attention` (no demotion
    /// event) until A's own request is cleared.
    #[tokio::test]
    async fn other_agents_clear_preserves_needs_attention() {
        use std::sync::Arc;
        let h = harness().await;
        let a = super::workspace_needs_attention::mk_session(&h.ws, "agent-a");
        let b = super::workspace_needs_attention::mk_session(&h.ws, "agent-b");
        h.store.insert_agent_session(&a).await.expect("session a");
        h.store.insert_agent_session(&b).await.expect("session b");
        h.store
            .set_attention_request(&h.ws, &a.id, "discussion", "A needs input", &now_iso())
            .await
            .expect("raise on A");
        // Seed: baseline needs_attention (from A's pending request).
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        let sink: Arc<dyn intent_acp::EventSink> =
            Arc::new(crate::BusEventSink::new(h.bus.clone()));
        let manager = Arc::new(crate::agent_manager::AgentManager::new(
            h.services.clone(),
            sink,
            4,
        ));
        // B's turn-begin clear: no request pending on B → no-op, no event.
        manager
            .clear_attention_request_if_present(&b.id, &h.ws)
            .await;
        assert_silent(&mut sub).await;
        let reloaded = h.store.get_agent_session(&a.id).await.expect("reload A");
        assert_eq!(
            reloaded.attention_request_kind.as_deref(),
            Some("discussion"),
            "A's pending request survives B's clear"
        );
        assert!(
            h.services
                .workspace_attention_signals(&h.ws, intent_core::WorkspaceAttention::None, None)
                .await
                .needs_attention
        );

        // Clearing A's own request retires the hold and emits the demotion.
        manager
            .clear_attention_request_if_present(&a.id, &h.ws)
            .await;
        let ev = recv_one(&mut sub).await;
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// An agent run ending must not demote a surviving `blocked`: the
    /// pending blocker request outranks both the running promotion and the
    /// post-debounce idle recompute, so the whole begin→end cycle stays
    /// silent and the read path keeps serving `blocked`.
    #[tokio::test]
    async fn agent_end_preserves_surviving_blocked() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-hold");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        h.store
            .set_attention_request(&h.ws, &session.id, "blocker", "stuck", &now_iso())
            .await
            .expect("raise");
        // Seed: baseline blocked.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services.agent_activity_begin(&h.ws).await;
        h.services.agent_activity_end(&h.ws);
        // Wait out the debounced idle recompute: blocked outranks both
        // transitions, so nothing emits (assert_silent's 300ms watch
        // covers the 100ms debounce window).
        assert_silent(&mut sub).await;
        assert!(
            h.services
                .workspace_attention_signals(&h.ws, intent_core::WorkspaceAttention::None, None)
                .await
                .blocked
        );
    }

    /// Idle-demotion vs activity-begin race: an `agent_activity_begin`
    /// landing inside the grace window cancels the pending idle flip — no
    /// bogus `idle` demotion emits and the status stays `in_progress`.
    #[tokio::test]
    async fn activity_begin_in_grace_window_cancels_idle_demotion() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::InProgress))
            .await
            .expect("insert task");
        // Seed: no agent running → idle baseline.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services.agent_activity_begin(&h.ws).await;
        let ev = recv_one(&mut sub).await;
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "in_progress" })
        );

        // End then immediately begin again: the second begin cancels the
        // pending idle debounce, so no demotion ever emits (assert_silent's
        // 300ms watch covers the 100ms window).
        h.services.agent_activity_end(&h.ws);
        h.services.agent_activity_begin(&h.ws).await;
        assert_silent(&mut sub).await;
    }

    /// Question-resolution trigger (§6.5 step 0): only the ANSWER — a user
    /// row tagged `question_answers` for the marked question message — clears
    /// the pending-questions marker and emits the demotion (store-only
    /// `agent.sendMessage` path). A PLAIN user message leaves the Q&A pending,
    /// so the workspace stays `needs_attention` and nothing emits.
    #[tokio::test]
    async fn user_answer_retires_pending_questions_and_emits() {
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-q2");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        let asked = h
            .store
            .append_agent_message(
                &session.id,
                "assistant",
                &super::workspace_needs_attention::question_content(),
                &now_iso(),
            )
            .await
            .expect("append question");
        h.services
            .record_pending_questions_marker(&h.ws, &session.id, &asked.id)
            .await;
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .agent_send_message_op(
                session.id.clone(),
                "unrelated aside".to_string(),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("send plain message");
        assert!(
            h.services.questions_pending(&session.id).await,
            "a plain user message must not resolve the pending Q&A"
        );
        assert_silent(&mut sub).await;

        h.services
            .agent_send_message_op(
                session.id.clone(),
                "here is my answer".to_string(),
                None,
                None,
                None,
                Some(json!({
                    "type": "question_answers",
                    "answeredQuestionsMessageId": asked.id,
                })),
            )
            .await
            .expect("send answer");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// Blocker raise/retire triggers (§6.5 step 1): a top-level
    /// `agent.requestAttention` blocker promotes the derived rollup to
    /// `blocked` (not `needs_attention`) and emits; the turn-begin clear
    /// retires it and emits the demotion.
    #[tokio::test]
    async fn blocker_raise_and_retire_transitions_emit() {
        use std::sync::Arc;
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-blk");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        // Seed the last-observed cache (first observation never emits).
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .agent_request_attention_op(
                h.ws.clone(),
                "blocker".to_string(),
                "sandbox broken".to_string(),
                Some(session.id.clone()),
            )
            .await
            .expect("raise blocker");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "blocked" })
        );

        // Retire via the runtime's turn-begin clear hook.
        let sink: Arc<dyn intent_acp::EventSink> =
            Arc::new(crate::BusEventSink::new(h.bus.clone()));
        let manager = Arc::new(crate::agent_manager::AgentManager::new(
            h.services.clone(),
            sink,
            4,
        ));
        manager
            .clear_attention_request_if_present(&session.id, &h.ws)
            .await;
        let ev = recv_one(&mut sub).await;
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// Failed park/retire triggers (§6.5 step 0): a top-level agent parked
    /// in `error` drives the derived rollup to `failed`; `agent.retry`
    /// clears the park and emits the demotion.
    #[tokio::test]
    async fn agent_retry_retires_failed_and_emits() {
        use std::sync::Arc;
        let h = harness().await;
        let mut session = super::workspace_needs_attention::mk_session(&h.ws, "agent-err");
        session.status = intent_core::AgentStatus::Error;
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        // Seed: baseline failed.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        let sink: Arc<dyn intent_acp::EventSink> =
            Arc::new(crate::BusEventSink::new(h.bus.clone()));
        let manager = Arc::new(crate::agent_manager::AgentManager::new(
            h.services.clone(),
            sink,
            4,
        ));
        let result = manager
            .agent_retry(session.id.clone(), h.ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], true);
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }

    /// The documented "fresh `agent.sendMessage`" recovery path for an
    /// errored (non-poisoned) agent: the direct-send emits the visible
    /// `failed → in_progress` transition. Pins the recompute after the
    /// user-row persist in `send_message` — the earlier recompute inside
    /// `try_begin` still reads `status = Error` and is a no-op, so without
    /// this one the redriven turn would stay `failed` on the event stream.
    #[tokio::test]
    async fn send_message_redrive_emits_failed_to_in_progress() {
        use std::sync::Arc;
        let h = harness().await;
        let mut session = super::workspace_needs_attention::mk_session(&h.ws, "agent-redrive");
        session.status = intent_core::AgentStatus::Error;
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        // Seed: baseline failed.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        let sink: Arc<dyn intent_acp::EventSink> =
            Arc::new(crate::BusEventSink::new(h.bus.clone()));
        let manager = Arc::new(crate::agent_manager::AgentManager::new(
            h.services.clone(),
            sink,
            4,
        ));
        let result = manager
            .send_message(
                session.id.clone(),
                h.ws.clone(),
                "try again".to_string(),
                None,
                crate::agent_manager::TurnOptions::default(),
            )
            .await
            .expect("send message");
        assert_eq!(result["queued"], false, "direct send: {result}");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "in_progress" })
        );
    }

    /// Regression: the `unread` flag is not a displayStatus axis. A turn-end
    /// `raise_attention(Unread)` on an idle workspace and the later
    /// `workspace.markSeen` both leave the derived rollup at `idle` — no
    /// `workspace:displayStatus-changed` — while the flag's own
    /// `workspace:attention-changed` events still fire on raise and clear.
    #[tokio::test]
    async fn unread_raise_and_mark_seen_never_move_display_status() {
        let h = harness().await;
        // Seed: idle baseline (no agents, no PR, no tasks).
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        let mut attn_sub = h.bus.subscribe(SubscriptionFilter {
            workspace_id: Some(h.ws.0.clone()),
            event_types: vec!["workspace:attention-changed".to_string()],
            ..Default::default()
        });
        h.services
            .raise_attention(&h.ws, intent_core::WorkspaceAttention::Unread)
            .await
            .expect("raise unread");
        assert_silent(&mut sub).await;
        let ev = recv_one(&mut attn_sub).await;
        assert_eq!(ev["type"], "workspace:attention-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "attention": "unread" })
        );

        h.services.mark_seen(h.ws.clone()).await.expect("mark seen");
        assert_silent(&mut sub).await;
        let ev = recv_one(&mut attn_sub).await;
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "attention": "none" })
        );
    }

    /// Raising the unread flag while the workspace is `in_progress` also
    /// stays silent — the flag feeds no displayStatus axis regardless of
    /// the base state.
    #[tokio::test]
    async fn unread_raise_during_active_run_stays_silent() {
        let h = harness().await;
        h.services.agent_activity_begin(&h.ws).await;
        // Seed: in_progress baseline.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .raise_attention(&h.ws, intent_core::WorkspaceAttention::Unread)
            .await
            .expect("raise unread");
        assert_silent(&mut sub).await;
    }

    /// Regression: a terminal `complete` base with the unread flag raised
    /// serves `displayStatus: complete` — the turn-end blue dot never masks
    /// the real terminal state (raise and markSeen both stay silent).
    #[tokio::test]
    async fn unread_flag_never_masks_complete() {
        let h = harness().await;
        h.store
            .insert_note(&task_note(&h.ws, "t1", TaskStatus::Complete))
            .await
            .expect("insert task");
        // Seed: complete baseline.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .raise_attention(&h.ws, intent_core::WorkspaceAttention::Unread)
            .await
            .expect("raise unread");
        assert_silent(&mut sub).await;
        let mut ws = h.store.get_workspace(&h.ws).await.expect("reload");
        ws.task_stats = Some(h.services.cheap_task_stats(&h.ws).await.expect("stats"));
        h.services.enrich_display_status(&mut ws, None, None).await;
        assert_eq!(ws.display_status, Some(WorkspaceDisplayStatus::Complete));

        h.services.mark_seen(h.ws.clone()).await.expect("mark seen");
        assert_silent(&mut sub).await;
    }

    /// Regression (intentd#945 review): the turn-end `raise_attention(Unread)`
    /// never downgrades a persistent `review_required` flag — the raise is a
    /// guarded no-op (no `attention-changed`), and a later
    /// `workspace.markSeen` (guarded on `unread`) leaves the review-required
    /// attention in place.
    #[tokio::test]
    async fn unread_raise_never_downgrades_review_required() {
        let h = harness().await;
        h.services
            .update_workspace(
                h.ws.clone(),
                intent_core::WorkspaceUpdate {
                    attention: Some(intent_core::WorkspaceAttention::ReviewRequired),
                    ..Default::default()
                },
            )
            .await
            .expect("set review_required");
        // Seed: needs_attention baseline.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .raise_attention(&h.ws, intent_core::WorkspaceAttention::Unread)
            .await
            .expect("raise unread");
        assert_silent(&mut sub).await;
        let ws = h.store.get_workspace(&h.ws).await.expect("reload");
        assert_eq!(
            ws.attention,
            intent_core::WorkspaceAttention::ReviewRequired,
            "turn-end unread raise must not overwrite review_required"
        );

        // markSeen only clears `unread`; review_required persists untouched.
        h.services.mark_seen(h.ws.clone()).await.expect("mark seen");
        assert_silent(&mut sub).await;
        let ws = h.store.get_workspace(&h.ws).await.expect("reload");
        assert_eq!(
            ws.attention,
            intent_core::WorkspaceAttention::ReviewRequired
        );
    }

    /// `ReviewRequired` flag triggers (§6.5 step 2): a `workspace.update`
    /// carrying `attention: review_required` promotes the derived rollup to
    /// `needs_attention` and emits; `workspace.dismissAttention` retires it
    /// and emits the demotion.
    #[tokio::test]
    async fn review_required_flag_transitions_emit() {
        let h = harness().await;
        // Seed: idle baseline.
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .update_workspace(
                h.ws.clone(),
                intent_core::WorkspaceUpdate {
                    attention: Some(intent_core::WorkspaceAttention::ReviewRequired),
                    ..Default::default()
                },
            )
            .await
            .expect("set review_required");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "workspace:displayStatus-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "needs_attention" })
        );

        h.services
            .dismiss_attention(h.ws.clone())
            .await
            .expect("dismiss");
        let ev = recv_one(&mut sub).await;
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "displayStatus": "idle" })
        );
    }
}
