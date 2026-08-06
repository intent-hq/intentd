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

use intent_core::events::WORKSPACE_DISPLAY_STATUS_CHANGED;
use intent_core::{
    now_iso, PullRequestInfo, PullRequestStatus, Workspace, WorkspaceActivity,
    WorkspaceDisplayStatus, WorkspaceId, WorkspaceTaskStats,
};
use intent_store::NewEvent;

use crate::{compute_task_stats, publish_event, system_actor, Services};

/// Last-observed derived `displayStatus` per workspace (PROTOCOL §6.5): the
/// recompute-and-compare seam behind
/// [`Services::maybe_emit_display_status_changed`]. A mutation that can move
/// the derivation recomputes it and publishes
/// `workspace:displayStatus-changed` only on an actual transition, so no-op
/// recomputes never spam the bus. Seeded lazily (first recompute after a
/// mutation, or an emit-path enrichment) — a first observation records
/// without emitting. In-memory only; a daemon restart re-seeds on first
/// touch. Shared across clones (behind `Arc`) so every service handle
/// compares against the same last-emitted value. The map is private to this
/// module: outside code can neither read nor write the baseline.
#[derive(Default)]
pub(crate) struct DisplayStatusCache(Mutex<HashMap<WorkspaceId, WorkspaceDisplayStatus>>);

impl DisplayStatusCache {
    /// Seed the baseline when absent (read paths): records the first
    /// observation without reporting a transition, so the first post-read
    /// mutation compares against it. Best-effort — a poisoned lock is
    /// ignored.
    fn seed(&self, workspace_id: &WorkspaceId, status: WorkspaceDisplayStatus) {
        if let Ok(mut map) = self.0.lock() {
            map.entry(workspace_id.clone()).or_insert(status);
        }
    }

    /// Record `status` and report whether it transitioned since the last
    /// observation: `Some(false)` on a first observation (a seed has no
    /// baseline to transition from), `None` on a poisoned lock (the caller
    /// skips emission).
    fn record(&self, workspace_id: &WorkspaceId, status: WorkspaceDisplayStatus) -> Option<bool> {
        match self.0.lock() {
            Ok(mut map) => Some(match map.insert(workspace_id.clone(), status) {
                Some(previous) => previous != status,
                None => false,
            }),
            Err(_) => None,
        }
    }

    /// Test-only visibility into whether a baseline exists for `workspace_id`.
    #[cfg(test)]
    pub(crate) fn contains(&self, workspace_id: &WorkspaceId) -> bool {
        self.0
            .lock()
            .expect("lock cache")
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
    pub(crate) async fn enrich_display_status(&self, ws: &mut Workspace) {
        if ws.task_stats.is_none() {
            return;
        }
        // Derive from the row's own `activity` (set by every caller just
        // before enrichment) so a single response can never pair
        // `activity: "agent_running"` with `displayStatus: "idle"`.
        // Active background hooks fold into the promotion (§6.5): an
        // idle agent still watching via a hook reads as active work.
        let display_status = compute_display_status(
            self.workspace_needs_attention(&ws.id).await,
            ws.activity == WorkspaceActivity::AgentRunning
                || self.workspace_has_active_hooks(&ws.id).await
                || self.workspace_has_waiting_agent_subscriptions(&ws.id).await,
            ws.active_pull_request.as_ref(),
            ws.pull_requests.as_deref().unwrap_or_default(),
            ws.pr_status,
            ws.task_stats.as_ref(),
        );
        self.last_display_statuses.seed(&ws.id, display_status);
        ws.display_status = Some(display_status);
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
        let Ok(ws) = self.store.get_workspace(workspace_id).await else {
            return;
        };
        let Ok(notes) = self.store.list_notes(workspace_id).await else {
            return;
        };
        let task_stats = compute_task_stats(&notes);
        let needs_attention = self.workspace_needs_attention(workspace_id).await;
        let status = compute_display_status(
            needs_attention,
            self.workspace_activity(workspace_id) == WorkspaceActivity::AgentRunning
                || self.workspace_has_active_hooks(workspace_id).await
                || self
                    .workspace_has_waiting_agent_subscriptions(workspace_id)
                    .await,
            ws.active_pull_request.as_ref(),
            ws.pull_requests.as_deref().unwrap_or_default(),
            ws.pr_status,
            Some(&task_stats),
        );
        let Some(transitioned) = self.last_display_statuses.record(workspace_id, status) else {
            return;
        };
        if transitioned {
            publish_event(
                &self.event_bus,
                display_status_changed_event(workspace_id, status),
            )
            .await;
        }
    }

    /// Whether any **top-level** agent in the workspace is waiting on the
    /// user (PROTOCOL §6.5, `needs_attention`): a session with no
    /// `parent_agent_id`, not background, and not deleted, that either
    /// carries a pending attention request (`attention_request_kind` —
    /// `discussion`/`blocker`) or has pending structured questions
    /// ([`Services::question_hold_active`]). Child/background sessions never
    /// count — their attention surface is the parent/subscriber (attention
    /// -retire taxonomy). The cheap metadata check runs over every candidate
    /// first, so transcript tail reads only happen when no session already
    /// flagged via an attention request. Best-effort: a store read failure
    /// fails open to `false` (and `question_hold_active` fails open itself)
    /// so list/get emission is never wedged.
    pub(crate) async fn workspace_needs_attention(&self, workspace_id: &WorkspaceId) -> bool {
        let Ok(sessions) = self.store.list_agent_session_summaries(workspace_id).await else {
            return false;
        };
        let top_level: Vec<_> = sessions
            .iter()
            .filter(|s| {
                s.parent_agent_id.is_none()
                    && !s.is_background
                    && s.status != intent_core::AgentStatus::Deleted
            })
            .collect();
        if top_level.iter().any(|s| s.attention_request_kind.is_some()) {
            return true;
        }
        for session in top_level {
            if self.question_hold_active(&session.id).await {
                return true;
            }
        }
        false
    }
}

/// Derive a workspace's `displayStatus` ("current cycle" precedence, spec
/// "Proposed representation" / "Decision: BE-owned displayStatus"), folding
/// in live agent activity (previously a client-side overlay) and the
/// per-workspace needs-attention signal:
/// 0. `needs_attention` → `needs_attention` unconditionally: a top-level
///    agent waiting on the user outranks everything, including a running
///    agent ([`Services::workspace_needs_attention`]).
/// 1. `agent_running` → `in_progress`: a live agent always reads as active
///    work, whatever the PR/task rollup says. Callers fold active-hook
///    state into this flag ([`Services::workspace_has_active_hooks`]) so an
///    idle agent still watching via a background hook reads the same.
/// 2. Active PR — the linked `activePullRequest` when open/draft, else the
///    most recently updated open/draft entry in `pullRequests` — yields
///    `pr_ready` (`mergeable == Some(true)` and not draft) or `pr_open`.
///    When neither carries an open/draft entry but the workspace `prStatus`
///    column is `Open`/`Draft`, that column is the fallback PR-stage signal
///    and yields `pr_open` (never `pr_ready`: the column carries no
///    mergeable info).
/// 3. Open tasks remain (`completed < total`) → `in_progress` when any task
///    has started, else `not_started`.
/// 4. Latest PR (linked, else most recently updated entry) merged — or
///    `prStatus == Merged` — → `pr_merged`.
/// 5. All tasks complete → `complete`; else `not_started`.
/// 6. Without a running agent, a task-stage rollup (`in_progress` /
///    `not_started` from steps 3/5) demotes to `idle`; the PR stages and
///    `complete` pass through unchanged.
///
/// A merged PR in history never masks an open PR (step 2 scans `pullRequests`
/// for open/draft entries) or open tasks (step 3 precedes the merged check).
fn compute_display_status(
    needs_attention: bool,
    agent_running: bool,
    active_pr: Option<&PullRequestInfo>,
    pull_requests: &[PullRequestInfo],
    pr_status: Option<PullRequestStatus>,
    task_stats: Option<&WorkspaceTaskStats>,
) -> WorkspaceDisplayStatus {
    if needs_attention {
        return WorkspaceDisplayStatus::NeedsAttention;
    }
    if agent_running {
        return WorkspaceDisplayStatus::InProgress;
    }
    match compute_base_display_status(active_pr, pull_requests, pr_status, task_stats) {
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
        return if pr.mergeable == Some(true) && !draft {
            WorkspaceDisplayStatus::PrReady
        } else {
            WorkspaceDisplayStatus::PrOpen
        };
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

/// Unit tests for the pure `compute_display_status` derivation ("current
/// cycle" precedence): active/latest open PR → open tasks → merged PR →
/// complete/not_started.
#[cfg(test)]
mod display_status {
    use intent_core::{
        PullRequestInfo, PullRequestStatus, WorkspaceDisplayStatus, WorkspaceTaskStats,
    };

    use super::compute_display_status;

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
            compute_display_status(false, false, None, &[], None, None),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(false, false, None, &[], None, Some(&stats(0, 0, 0))),
            WorkspaceDisplayStatus::Idle
        );
    }

    #[test]
    fn no_prs_task_stage_demotes_to_idle_without_agent() {
        // The base rollup is in_progress / not_started, but without a
        // running agent the task-stage statuses demote to idle.
        assert_eq!(
            compute_display_status(false, false, None, &[], None, Some(&stats(3, 0, 0))),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(false, false, None, &[], None, Some(&stats(3, 0, 1))),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(false, false, None, &[], None, Some(&stats(3, 1, 0))),
            WorkspaceDisplayStatus::Idle
        );
        assert_eq!(
            compute_display_status(false, false, None, &[], None, Some(&stats(3, 3, 0))),
            WorkspaceDisplayStatus::Complete
        );
    }

    #[test]
    fn running_agent_promotes_to_in_progress_unconditionally() {
        // A live agent wins over every PR/task rollup.
        assert_eq!(
            compute_display_status(false, true, None, &[], None, None),
            WorkspaceDisplayStatus::InProgress
        );
        assert_eq!(
            compute_display_status(false, true, None, &[], None, Some(&stats(3, 3, 0))),
            WorkspaceDisplayStatus::InProgress
        );
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        assert_eq!(
            compute_display_status(false, true, Some(&ready), &[], None, None),
            WorkspaceDisplayStatus::InProgress
        );
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(false, true, Some(&merged), &[], None, None),
            WorkspaceDisplayStatus::InProgress
        );
    }

    #[test]
    fn needs_attention_wins_over_everything() {
        // Step 0: the needs-attention signal outranks a running agent, every
        // PR stage, and every task rollup.
        assert_eq!(
            compute_display_status(true, false, None, &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            compute_display_status(true, true, None, &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        assert_eq!(
            compute_display_status(true, false, Some(&ready), &[], None, None),
            WorkspaceDisplayStatus::NeedsAttention
        );
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(true, true, Some(&merged), &[], None, Some(&stats(3, 3, 0))),
            WorkspaceDisplayStatus::NeedsAttention
        );
        assert_eq!(
            compute_display_status(
                true,
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
    fn pr_stages_and_complete_pass_through_without_agent() {
        // The idle demotion only applies to the task-stage rollups; PR
        // stages and complete are untouched.
        let mut ready = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        ready.mergeable = Some(true);
        assert_eq!(
            compute_display_status(false, false, Some(&ready), &[], None, None),
            WorkspaceDisplayStatus::PrReady
        );
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(false, false, Some(&merged), &[], None, None),
            WorkspaceDisplayStatus::PrMerged
        );
        assert_eq!(
            compute_display_status(false, false, None, &[], None, Some(&stats(2, 2, 0))),
            WorkspaceDisplayStatus::Complete
        );
    }

    #[test]
    fn open_active_pr_mergeable_is_pr_ready() {
        let mut open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        open.mergeable = Some(true);
        assert_eq!(
            compute_display_status(false, false, Some(&open), &[], None, Some(&stats(2, 0, 1))),
            WorkspaceDisplayStatus::PrReady
        );
    }

    #[test]
    fn open_active_pr_not_mergeable_or_draft_is_pr_open() {
        let open = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(false, false, Some(&open), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
        let mut draft = pr(PullRequestStatus::Draft, "2026-01-02T00:00:00Z");
        draft.mergeable = Some(true);
        assert_eq!(
            compute_display_status(false, false, Some(&draft), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
        let mut flagged = pr(PullRequestStatus::Open, "2026-01-02T00:00:00Z");
        flagged.mergeable = Some(true);
        flagged.is_draft = Some(true);
        assert_eq!(
            compute_display_status(false, false, Some(&flagged), &[], None, None),
            WorkspaceDisplayStatus::PrOpen
        );
    }

    #[test]
    fn merged_pr_never_masks_open_tasks() {
        // Open tasks keep the rollup off pr_merged; without a running agent
        // the resulting task-stage status reads as idle.
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(
                false,
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
                false,
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
                false,
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
        let list = vec![merged.clone(), ready];
        assert_eq!(
            compute_display_status(
                false,
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
        let list = vec![stale, fresh];
        assert_eq!(
            compute_display_status(false, false, None, &list, None, None),
            WorkspaceDisplayStatus::PrReady
        );
    }

    #[test]
    fn merged_with_all_tasks_done_is_pr_merged() {
        let merged = pr(PullRequestStatus::Merged, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(
                false,
                false,
                Some(&merged),
                &[],
                None,
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::PrMerged
        );
        assert_eq!(
            compute_display_status(false, false, Some(&merged), &[], None, None),
            WorkspaceDisplayStatus::PrMerged
        );
    }

    #[test]
    fn merged_latest_from_list_without_active_pr() {
        let closed = pr(PullRequestStatus::Closed, "2026-01-01T00:00:00Z");
        let merged = pr(PullRequestStatus::Merged, "2026-01-04T00:00:00Z");
        let list = vec![closed, merged];
        assert_eq!(
            compute_display_status(false, false, None, &list, None, None),
            WorkspaceDisplayStatus::PrMerged
        );
    }

    #[test]
    fn closed_pr_falls_through_to_task_logic() {
        let closed = pr(PullRequestStatus::Closed, "2026-01-02T00:00:00Z");
        assert_eq!(
            compute_display_status(
                false,
                false,
                Some(&closed),
                &[],
                None,
                Some(&stats(2, 2, 0))
            ),
            WorkspaceDisplayStatus::Complete
        );
        assert_eq!(
            compute_display_status(false, false, Some(&closed), &[], None, None),
            WorkspaceDisplayStatus::Idle
        );
    }

    #[test]
    fn pr_status_open_or_draft_without_pr_objects_is_pr_open() {
        assert_eq!(
            compute_display_status(false, false, None, &[], Some(PullRequestStatus::Open), None),
            WorkspaceDisplayStatus::PrOpen
        );
        assert_eq!(
            compute_display_status(
                false,
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
                false,
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
                false,
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
                false,
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
                false,
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
                false,
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
                false,
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
        assert_eq!(
            compute_display_status(
                false,
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
                false,
                false,
                None,
                &list,
                Some(PullRequestStatus::Merged),
                None
            ),
            WorkspaceDisplayStatus::PrReady
        );
    }
}

/// Per-workspace needs-attention signal (`Services::workspace_needs_attention`,
/// PROTOCOL §6.5): true iff a **top-level** session (no parent, not
/// background, not deleted) carries a pending attention request or pending
/// structured questions; child/background/deleted sessions never count.
#[cfg(test)]
mod workspace_needs_attention {
    use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
    use intent_store::Store;
    use serde_json::json;

    use crate::tests::{workspace, TempDb};
    use crate::Services;

    pub(super) fn mk_session(ws: &WorkspaceId, id: &str) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: id.to_string(),
            name_explicitly_set: false,
            model: None,
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
        }
    }

    /// Assistant content carrying one structured-question resource block
    /// (the shape `has_question_blocks` matches).
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
    async fn no_sessions_is_false() {
        let (svc, ws, _tmp) = setup().await;
        assert!(!svc.workspace_needs_attention(&ws).await);
    }

    #[tokio::test]
    async fn plain_top_level_session_is_false() {
        let (svc, ws, _tmp) = setup().await;
        svc.store
            .insert_agent_session(&mk_session(&ws, "agent-plain"))
            .await
            .unwrap();
        assert!(!svc.workspace_needs_attention(&ws).await);
    }

    #[tokio::test]
    async fn top_level_attention_request_is_true() {
        let (svc, ws, _tmp) = setup().await;
        for kind in ["discussion", "blocker"] {
            let mut s = mk_session(&ws, &format!("agent-{kind}"));
            s.attention_request_kind = Some(kind.to_string());
            svc.store.insert_agent_session(&s).await.unwrap();
        }
        assert!(svc.workspace_needs_attention(&ws).await);
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

        assert!(!svc.workspace_needs_attention(&ws).await);
    }

    #[tokio::test]
    async fn pending_questions_on_top_level_session_is_true() {
        let (svc, ws, _tmp) = setup().await;
        let session = mk_session(&ws, "agent-q");
        svc.store.insert_agent_session(&session).await.unwrap();
        svc.store
            .append_agent_message(&session.id, "assistant", &question_content(), &now_iso())
            .await
            .unwrap();
        assert!(svc.workspace_needs_attention(&ws).await);
    }

    #[tokio::test]
    async fn superseded_or_dismissed_questions_are_false() {
        let (svc, ws, _tmp) = setup().await;

        // A user reply after the question row supersedes the hold.
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

        assert!(!svc.workspace_needs_attention(&ws).await);
    }

    /// A store read failure fails open to `false` (list/get emission must
    /// never be wedged by the attention probe).
    #[tokio::test]
    async fn store_read_failure_fails_open_to_false() {
        let (svc, ws, _tmp) = setup().await;
        let mut s = mk_session(&ws, "agent-attn");
        s.attention_request_kind = Some("blocker".to_string());
        svc.store.insert_agent_session(&s).await.unwrap();
        assert!(svc.workspace_needs_attention(&ws).await);

        // Force list_agent_session_summaries to fail.
        sqlx::query("DROP TABLE agent_session")
            .execute(svc.store.write_pool())
            .await
            .expect("drop agent_session table");
        assert!(!svc.workspace_needs_attention(&ws).await);
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
        WorkspaceApi, WorkspaceId,
    };
    use intent_store::Store;
    use serde_json::{json, Value};

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

    /// A task-completion transition (in_progress → complete over
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
    /// task flipping not_started → in_progress while the rollup is already
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
        h.services.agent_activity_end(&h.ws).await;
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

    /// Question-resolution trigger via `agent.dismissQuestions` (§6.5 step 0):
    /// persisting the dismissal marker retires the question hold and emits the
    /// needs_attention → idle demotion.
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

    /// Question-resolution trigger via a user-origin delivery (§6.5 step 0):
    /// the persisted user row supersedes the pending question tail
    /// (store-only `agent.sendMessage` path) and emits the demotion.
    #[tokio::test]
    async fn user_answer_retires_question_hold_and_emits() {
        let h = harness().await;
        let session = super::workspace_needs_attention::mk_session(&h.ws, "agent-q2");
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");
        h.store
            .append_agent_message(
                &session.id,
                "assistant",
                &super::workspace_needs_attention::question_content(),
                &now_iso(),
            )
            .await
            .expect("append question");
        h.services.maybe_emit_display_status_changed(&h.ws).await;

        let mut sub = subscribe(&h);
        h.services
            .agent_send_message_op(
                session.id.clone(),
                "here is my answer".to_string(),
                None,
                None,
                None,
                None,
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
}
