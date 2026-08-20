//! Ready-set delta attributable to a batch of task completions
//! (intent-hq/monorepo#2044).
//!
//! Pure helper behind the delivery-time "tasks now unblocked" hint in
//! completion wakes: given the CURRENT workspace task snapshot (fetched fresh
//! at delivery/render time, never cached from enqueue time) and the trigger
//! task-note ids whose completions are being reported in a drained batch,
//! compute which tasks are ready to start NOW but were not ready before those
//! completions — the readiness delta attributable to the batch. Readiness
//! reuses [`classify_batch_tasks`] so the semantics stay identical to the
//! batch-delegate classification; nothing is written, emitted, or scheduled
//! here.

use std::collections::{HashMap, HashSet};

use intent_core::TaskStatus;
use serde_json::json;

pub(crate) use super::batch::BatchTaskSnap;
use super::batch::{classify_batch_tasks, BatchDisposition};

/// `messageMetadata` key on a completion wake naming the trigger tasks —
/// an array of `{ "workspaceId", "taskNoteId" }` objects recorded at ENQUEUE
/// time. It carries only the triggering fact (which linked task notes the
/// settled children had); the unblocked enumeration itself is never computed
/// or stored at enqueue — delivery/render time resolves it fresh.
pub(crate) const UNBLOCKED_TRIGGER_TASKS_KEY: &str = "unblockedTriggerTasks";

/// Stable prefix of the rendered unblocked section, used by the delivery
/// paths as an idempotency guard (a requeued entry whose content already
/// carries a section is never re-annotated — same contract as the
/// dequeue-wait note). Owned by the harness (H6) alongside the section
/// wording.
pub(crate) use crate::harness::v1::UNBLOCKED_SECTION_PREFIX;

/// Stamp `triggers` (`(workspace_id, task_note_id)` pairs) onto a wake's
/// `event_notification` metadata under [`UNBLOCKED_TRIGGER_TASKS_KEY`].
/// No-op for an empty trigger set or non-object metadata.
pub(crate) fn stamp_trigger_tasks(metadata: &mut serde_json::Value, triggers: &[(String, String)]) {
    if triggers.is_empty() {
        return;
    }
    if let Some(obj) = metadata.as_object_mut() {
        let arr: Vec<serde_json::Value> = triggers
            .iter()
            .map(|(ws, id)| json!({ "workspaceId": ws, "taskNoteId": id }))
            .collect();
        obj.insert(UNBLOCKED_TRIGGER_TASKS_KEY.to_string(), json!(arr));
    }
}

/// Event-data key naming a settled group member's trigger tasks — an array
/// of `{ "workspaceId", "taskNoteId" }` objects (the child's linked task
/// plus any flipped completions it recorded) stamped at group RECORD time
/// (when the child settles), so the aggregated wake's trigger set does not
/// depend on the child session still existing when the group fires: a
/// task-linked child deleted between its settlement and group settlement
/// keeps its triggers. Persisted with the group's `raw_events`, so it also
/// survives daemon restarts. Events stamped before the plural upgrade carry
/// a single object under the same key; the reader accepts both shapes.
pub(crate) const EVENT_TRIGGER_TASK_KEY: &str = "unblockedTriggerTask";

/// Stamp a settled child's trigger `(workspace_id, task_note_id)` pairs
/// onto its recorded group event data under [`EVENT_TRIGGER_TASK_KEY`].
/// No-op for an empty pair set or non-object data.
pub(crate) fn stamp_event_trigger_tasks(data: &mut serde_json::Value, pairs: &[(String, String)]) {
    if pairs.is_empty() {
        return;
    }
    if let Some(obj) = data.as_object_mut() {
        let arr: Vec<serde_json::Value> = pairs
            .iter()
            .map(|(ws, id)| json!({ "workspaceId": ws, "taskNoteId": id }))
            .collect();
        obj.insert(EVENT_TRIGGER_TASK_KEY.to_string(), json!(arr));
    }
}

/// Read a recorded group event's [`EVENT_TRIGGER_TASK_KEY`] stamp back as
/// `(workspace_id, task_note_id)` pairs. Accepts both the array shape and
/// the legacy single-object shape (pre-upgrade persisted events); malformed
/// entries are skipped. Empty means no stamp.
pub(crate) fn event_trigger_tasks(data: &serde_json::Value) -> Vec<(String, String)> {
    let pair_of = |t: &serde_json::Value| -> Option<(String, String)> {
        Some((
            t.get("workspaceId")?.as_str()?.to_string(),
            t.get("taskNoteId")?.as_str()?.to_string(),
        ))
    };
    match data.get(EVENT_TRIGGER_TASK_KEY) {
        Some(serde_json::Value::Array(arr)) => arr.iter().filter_map(pair_of).collect(),
        Some(t) => pair_of(t).into_iter().collect(),
        None => Vec::new(),
    }
}

/// Whether a queued message's metadata carries any stamped trigger tasks.
pub(crate) fn metadata_has_triggers(metadata: Option<&serde_json::Value>) -> bool {
    metadata
        .and_then(|md| md.get(UNBLOCKED_TRIGGER_TASKS_KEY))
        .and_then(|v| v.as_array())
        .is_some_and(|a| !a.is_empty())
}

/// Collect the trigger `(workspace_id, task_note_id)` pairs stamped on a
/// batch of message metadatas, deduplicated in first-seen order — the
/// coalescing half of the delivery-time contract: several completion wakes
/// draining in one batch contribute ONE merged trigger set (and thus one
/// delta computation) instead of stitching per-event snapshots.
pub(crate) fn collect_trigger_tasks<'a>(
    metadatas: impl Iterator<Item = Option<&'a serde_json::Value>>,
) -> Vec<(String, String)> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut out = Vec::new();
    for md in metadatas.flatten() {
        let Some(arr) = md
            .get(UNBLOCKED_TRIGGER_TASKS_KEY)
            .and_then(|v| v.as_array())
        else {
            continue;
        };
        for t in arr {
            let (Some(ws), Some(id)) = (
                t.get("workspaceId").and_then(|v| v.as_str()),
                t.get("taskNoteId").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            let pair = (ws.to_string(), id.to_string());
            if seen.insert(pair.clone()) {
                out.push(pair);
            }
        }
    }
    out
}

/// Render the advisory "tasks now unblocked" section for a non-empty delta.
/// Each row names the task as an `intent://local/task/{id}` link plus the
/// reason it became ready; a task sitting in an attention status is annotated
/// rather than dropped (`; currently blocked — needs attention`), since the
/// delegator may want to resolve the attention state precisely because the
/// task is otherwise unblocked. `multiple_triggers` flips the singular/plural
/// framing when a coalesced batch covered more than one completion.
pub(crate) fn render_unblocked_section(delta: &[UnblockedTask], multiple_triggers: bool) -> String {
    crate::harness::latest().unblocked_section(delta, multiple_triggers)
}

/// Why a task appears in the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnblockedReason {
    /// Every `dependsOn` edge is satisfied now, and at least one was unmet
    /// before the trigger completions.
    DepsSatisfied,
    /// The task's `conflictsWith` overlap with running work cleared when a
    /// trigger task completed.
    ConflictCleared,
}

/// One row of the delta, ready for message rendering. `title` falls back to
/// the note id when the caller has no title for it. `attention` carries the
/// task's attention status (`waiting` / `discussion_needed` / `blocked` /
/// `review_required`) when it sits in one — such tasks stay in the delta by
/// design (see [`ready_set_delta`]) and the renderer annotates them instead
/// of dropping them; `None` for ordinary `not_started` candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnblockedTask {
    pub(crate) note_id: String,
    pub(crate) title: String,
    pub(crate) reason: UnblockedReason,
    pub(crate) attention: Option<TaskStatus>,
}

/// Single-task readiness disposition against `snaps`: `conflictsWith`
/// overlap with running work reports as held, and classifying one id at a
/// time keeps candidates from holding each other.
///
/// Cost: each call rebuilds the conflict adjacency and runs the
/// critical-path pass over the whole snapshot, so [`ready_set_delta`] is
/// O(candidates x (V+E)) with up to two calls per candidate. Acceptable at
/// workspace task counts and off the hot RPC paths (it runs once per
/// completion-wake delivery); revisit with a single-id fast path in
/// [`classify_batch_tasks`] if that ever changes.
fn classify_one(id: &str, snaps: &HashMap<String, BatchTaskSnap>) -> Option<BatchDisposition> {
    classify_batch_tasks(&[id.to_string()], snaps)
        .into_iter()
        .next()
        .map(|(_, disposition)| disposition)
}

/// Compute the attributable unblocked delta for `trigger_completions`.
///
/// A task appears iff it is ready now (dep-satisfied and conflict-free per
/// [`classify_batch_tasks`]) but was NOT ready in the counterfactual where
/// the trigger tasks are still in flight — so tasks already ready
/// beforehand, tasks still blocked, and readiness changes not explained by
/// the triggers never show up. Terminal (`complete`/`cancelled`),
/// `in_progress`, live-agent-assigned tasks and the triggers themselves are
/// excluded. Only triggers that are `complete` in `snaps` count: ids that
/// were deleted or reopened between enqueue and delivery are skipped
/// gracefully. A task held before on both a trigger dep and a trigger
/// conflict reports [`UnblockedReason::DepsSatisfied`] (the dep check runs
/// first). Attention statuses (`waiting`, `discussion_needed`, `blocked`,
/// `review_required`) remain candidates ON PURPOSE — batch-delegate
/// classification treats them as delegable, and this helper keeps those
/// semantics; their status is surfaced on [`UnblockedTask::attention`] so the
/// renderer annotates them rather than silently dropping them. Output is
/// sorted by title then note id for reproducible message rendering.
pub(crate) fn ready_set_delta(
    trigger_completions: &[String],
    snaps: &HashMap<String, BatchTaskSnap>,
    titles: &HashMap<String, String>,
) -> Vec<UnblockedTask> {
    let triggers: HashSet<&str> = trigger_completions
        .iter()
        .map(|id| id.as_str())
        .filter(|id| snaps.get(*id).map(|s| s.status) == Some(TaskStatus::Complete))
        .collect();
    if triggers.is_empty() {
        return Vec::new();
    }
    // Counterfactual snapshot: the trigger tasks still in flight (incomplete
    // and running), so deps on them are unmet and their conflict edges are
    // active against the running set.
    let mut before = snaps.clone();
    for id in &triggers {
        let snap = before.get_mut(*id).expect("trigger filtered on presence");
        snap.status = TaskStatus::InProgress;
        snap.live_agent = Some(("trigger".to_string(), "trigger".to_string()));
    }
    let mut out = Vec::new();
    for (id, snap) in snaps {
        if triggers.contains(id.as_str()) {
            continue;
        }
        // Only tasks someone could start now: not terminal, not already
        // being worked (in_progress or a live assigned agent).
        if matches!(
            snap.status,
            TaskStatus::Complete | TaskStatus::Cancelled | TaskStatus::InProgress
        ) || snap.live_agent.is_some()
        {
            continue;
        }
        if !matches!(classify_one(id, snaps), Some(BatchDisposition::Start)) {
            continue;
        }
        let reason = match classify_one(id, &before) {
            Some(BatchDisposition::HeldOnDeps { .. }) => UnblockedReason::DepsSatisfied,
            Some(BatchDisposition::HeldOnConflict { .. }) => UnblockedReason::ConflictCleared,
            // Ready before the triggers too — not attributable to them.
            _ => continue,
        };
        let attention = match snap.status {
            TaskStatus::Waiting
            | TaskStatus::DiscussionNeeded
            | TaskStatus::Blocked
            | TaskStatus::ReviewRequired => Some(snap.status),
            _ => None,
        };
        out.push(UnblockedTask {
            note_id: id.clone(),
            title: titles.get(id).cloned().unwrap_or_else(|| id.clone()),
            reason,
            attention,
        });
    }
    out.sort_by(|a, b| {
        a.title
            .cmp(&b.title)
            .then_with(|| a.note_id.cmp(&b.note_id))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(status: TaskStatus, deps: &[&str], conflicts: &[&str]) -> BatchTaskSnap {
        BatchTaskSnap {
            status,
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            conflicts_with: conflicts.iter().map(|s| s.to_string()).collect(),
            live_agent: None,
            effort_minutes: None,
        }
    }

    fn running(mut s: BatchTaskSnap) -> BatchTaskSnap {
        s.live_agent = Some(("agent-1".into(), "Worker".into()));
        s
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn titles(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(id, t)| (id.to_string(), t.to_string()))
            .collect()
    }

    fn rows(delta: &[UnblockedTask]) -> Vec<(&str, UnblockedReason)> {
        delta
            .iter()
            .map(|u| (u.note_id.as_str(), u.reason))
            .collect()
    }

    #[test]
    fn single_completion_unblocks_sole_dependent() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &titles(&[("t", "T")]));
        assert_eq!(rows(&delta), vec![("t", UnblockedReason::DepsSatisfied)]);
        assert_eq!(delta[0].title, "T");
    }

    #[test]
    fn partially_satisfied_deps_stay_out() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("other".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert(
            "t".into(),
            snap(TaskStatus::NotStarted, &["done", "other"], &[]),
        );
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &HashMap::new());
        assert!(delta.is_empty(), "{delta:?}");
    }

    #[test]
    fn batch_with_both_deps_coalesces_to_one_row() {
        let mut snaps = HashMap::new();
        snaps.insert("d1".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("d2".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &["d1", "d2"], &[]));
        let delta = ready_set_delta(&ids(&["d1", "d2"]), &snaps, &HashMap::new());
        assert_eq!(rows(&delta), vec![("t", UnblockedReason::DepsSatisfied)]);
    }

    #[test]
    fn task_already_ready_before_the_triggers_is_excluded() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("old".into(), snap(TaskStatus::Complete, &[], &[]));
        // No deps at all, and deps satisfied long before the trigger.
        snaps.insert("free".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert(
            "settled".into(),
            snap(TaskStatus::NotStarted, &["old"], &[]),
        );
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &HashMap::new());
        assert!(delta.is_empty(), "{delta:?}");
    }

    #[test]
    fn non_startable_dependents_are_excluded() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("busy".into(), snap(TaskStatus::InProgress, &["done"], &[]));
        snaps.insert("won".into(), snap(TaskStatus::Complete, &["done"], &[]));
        snaps.insert("gone".into(), snap(TaskStatus::Cancelled, &["done"], &[]));
        snaps.insert(
            "live".into(),
            running(snap(TaskStatus::NotStarted, &["done"], &[])),
        );
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &HashMap::new());
        assert!(delta.is_empty(), "{delta:?}");
    }

    #[test]
    fn conflict_cleared_is_reported_separately() {
        let mut snaps = HashMap::new();
        // The trigger declares the edge; symmetric closure covers `t`.
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &["t"]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &[], &[]));
        // Otherwise-blocked peers never surface: unmet non-trigger dep, or a
        // remaining conflict with running work.
        snaps.insert("other".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert(
            "held".into(),
            snap(TaskStatus::NotStarted, &["other"], &["done"]),
        );
        snaps.insert(
            "busy".into(),
            running(snap(TaskStatus::InProgress, &[], &[])),
        );
        snaps.insert(
            "still".into(),
            snap(TaskStatus::NotStarted, &[], &["done", "busy"]),
        );
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &HashMap::new());
        assert_eq!(rows(&delta), vec![("t", UnblockedReason::ConflictCleared)]);
    }

    #[test]
    fn candidate_declared_conflict_edge_also_reports_conflict_cleared() {
        // Reverse direction of the edge: the CANDIDATE declares
        // `conflictsWith: ["done"]`; symmetric closure (inherited from
        // `classify_batch_tasks`) must still surface it.
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &[], &["done"]));
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &HashMap::new());
        assert_eq!(rows(&delta), vec![("t", UnblockedReason::ConflictCleared)]);
    }

    #[test]
    fn attention_statuses_remain_candidates() {
        // Pinned semantics: waiting/discussion_needed/blocked/review_required
        // tasks are delegable per batch classification, so they appear in the
        // delta once their trigger dep completes — with their attention
        // status surfaced for the renderer to annotate.
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("w".into(), snap(TaskStatus::Waiting, &["done"], &[]));
        snaps.insert(
            "d".into(),
            snap(TaskStatus::DiscussionNeeded, &["done"], &[]),
        );
        snaps.insert("b".into(), snap(TaskStatus::Blocked, &["done"], &[]));
        snaps.insert("r".into(), snap(TaskStatus::ReviewRequired, &["done"], &[]));
        snaps.insert("n".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &HashMap::new());
        assert_eq!(
            rows(&delta),
            vec![
                ("b", UnblockedReason::DepsSatisfied),
                ("d", UnblockedReason::DepsSatisfied),
                ("n", UnblockedReason::DepsSatisfied),
                ("r", UnblockedReason::DepsSatisfied),
                ("w", UnblockedReason::DepsSatisfied),
            ]
        );
        let attention: Vec<(&str, Option<TaskStatus>)> = delta
            .iter()
            .map(|u| (u.note_id.as_str(), u.attention))
            .collect();
        assert_eq!(
            attention,
            vec![
                ("b", Some(TaskStatus::Blocked)),
                ("d", Some(TaskStatus::DiscussionNeeded)),
                ("n", None),
                ("r", Some(TaskStatus::ReviewRequired)),
                ("w", Some(TaskStatus::Waiting)),
            ]
        );
    }

    #[test]
    fn dep_and_conflict_on_the_same_trigger_reports_deps_satisfied() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &["t"]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &HashMap::new());
        assert_eq!(rows(&delta), vec![("t", UnblockedReason::DepsSatisfied)]);
    }

    #[test]
    fn missing_or_reopened_triggers_are_tolerated() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        // A reopened trigger neither unlocks its dependents nor counts as a
        // cleared conflict.
        snaps.insert("reopened".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert(
            "peer".into(),
            snap(TaskStatus::NotStarted, &[], &["reopened"]),
        );
        let delta = ready_set_delta(
            &ids(&["ghost", "reopened", "done"]),
            &snaps,
            &HashMap::new(),
        );
        assert_eq!(rows(&delta), vec![("t", UnblockedReason::DepsSatisfied)]);
        // All triggers gone → empty, no panic.
        assert!(ready_set_delta(&ids(&["ghost"]), &snaps, &HashMap::new()).is_empty());
    }

    #[test]
    fn empty_trigger_set_yields_empty_delta() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        assert!(ready_set_delta(&[], &snaps, &HashMap::new()).is_empty());
    }

    #[test]
    fn triggers_themselves_never_appear() {
        let mut snaps = HashMap::new();
        // `d2` is itself a trigger and depends on the other trigger.
        snaps.insert("d1".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("d2".into(), snap(TaskStatus::Complete, &["d1"], &[]));
        let delta = ready_set_delta(&ids(&["d1", "d2"]), &snaps, &HashMap::new());
        assert!(delta.is_empty(), "{delta:?}");
    }

    #[test]
    fn stamp_and_collect_round_trip_with_dedup_in_first_seen_order() {
        let mut md1 = json!({ "type": "event_notification" });
        stamp_trigger_tasks(&mut md1, &[("ws-1".into(), "t-a".into())]);
        let mut md2 = json!({ "type": "event_notification" });
        stamp_trigger_tasks(
            &mut md2,
            &[("ws-1".into(), "t-a".into()), ("ws-1".into(), "t-b".into())],
        );
        assert!(metadata_has_triggers(Some(&md1)));
        assert!(!metadata_has_triggers(Some(&json!({}))));
        assert!(!metadata_has_triggers(None));
        let collected = collect_trigger_tasks([Some(&md1), None, Some(&md2)].into_iter());
        assert_eq!(
            collected,
            vec![
                ("ws-1".to_string(), "t-a".to_string()),
                ("ws-1".to_string(), "t-b".to_string()),
            ]
        );
    }

    #[test]
    fn event_trigger_task_stamp_round_trips_and_fails_soft() {
        // Multi-pair array shape round-trips in order.
        let mut data = json!({ "agentId": "agent-1" });
        stamp_event_trigger_tasks(
            &mut data,
            &[("ws-1".into(), "t-a".into()), ("ws-1".into(), "t-b".into())],
        );
        assert_eq!(
            event_trigger_tasks(&data),
            vec![
                ("ws-1".to_string(), "t-a".to_string()),
                ("ws-1".to_string(), "t-b".to_string()),
            ]
        );
        // Legacy single-object shape (pre-upgrade persisted events) reads back.
        let legacy = json!({
            EVENT_TRIGGER_TASK_KEY: { "workspaceId": "ws-1", "taskNoteId": "t-old" }
        });
        assert_eq!(
            event_trigger_tasks(&legacy),
            vec![("ws-1".to_string(), "t-old".to_string())]
        );
        assert!(event_trigger_tasks(&json!({})).is_empty());
        assert!(
            event_trigger_tasks(&json!({ EVENT_TRIGGER_TASK_KEY: "not-an-object" })).is_empty()
        );
        // Malformed array entries are skipped.
        let malformed = json!({
            EVENT_TRIGGER_TASK_KEY: [
                { "workspaceId": "ws-1" },
                "scalar",
                { "workspaceId": "ws-1", "taskNoteId": "t-ok" },
            ]
        });
        assert_eq!(
            event_trigger_tasks(&malformed),
            vec![("ws-1".to_string(), "t-ok".to_string())]
        );
        // Empty pair set and non-object data are no-ops.
        let mut untouched = json!({ "agentId": "agent-1" });
        stamp_event_trigger_tasks(&mut untouched, &[]);
        assert!(untouched.get(EVENT_TRIGGER_TASK_KEY).is_none());
        let mut non_object = json!("scalar");
        stamp_event_trigger_tasks(&mut non_object, &[("ws-1".into(), "t-a".into())]);
        assert_eq!(non_object, json!("scalar"));
    }

    #[test]
    fn stamp_is_a_noop_for_empty_triggers_and_malformed_entries_are_skipped() {
        let mut md = json!({ "type": "event_notification" });
        stamp_trigger_tasks(&mut md, &[]);
        assert!(md.get(UNBLOCKED_TRIGGER_TASKS_KEY).is_none());
        let malformed = json!({
            UNBLOCKED_TRIGGER_TASKS_KEY: [
                { "workspaceId": "ws-1" },
                { "taskNoteId": "t-a" },
                "not-an-object",
                { "workspaceId": "ws-1", "taskNoteId": "t-ok" },
            ]
        });
        assert_eq!(
            collect_trigger_tasks(std::iter::once(Some(&malformed))),
            vec![("ws-1".to_string(), "t-ok".to_string())]
        );
    }

    #[test]
    fn render_section_links_tasks_and_flips_plural_framing() {
        let delta = vec![
            UnblockedTask {
                note_id: "t3".into(),
                title: "T3: Return a map".into(),
                reason: UnblockedReason::DepsSatisfied,
                attention: None,
            },
            UnblockedTask {
                note_id: "t4".into(),
                title: "T4: FE parity".into(),
                reason: UnblockedReason::ConflictCleared,
                attention: None,
            },
        ];
        let single = render_unblocked_section(&delta, false);
        assert_eq!(
            single,
            "Tasks now unblocked by this completion: \
             [T3: Return a map](intent://local/task/t3) (deps satisfied), \
             [T4: FE parity](intent://local/task/t4) (conflict cleared)."
        );
        let multi = render_unblocked_section(&delta, true);
        assert!(multi.starts_with("Tasks now unblocked by these completions:"));
        assert!(single.starts_with(UNBLOCKED_SECTION_PREFIX));
    }

    #[test]
    fn render_section_annotates_attention_statuses_instead_of_dropping() {
        let delta = vec![
            UnblockedTask {
                note_id: "tb".into(),
                title: "Blocked one".into(),
                reason: UnblockedReason::DepsSatisfied,
                attention: Some(TaskStatus::Blocked),
            },
            UnblockedTask {
                note_id: "tw".into(),
                title: "Waiting one".into(),
                reason: UnblockedReason::ConflictCleared,
                attention: Some(TaskStatus::Waiting),
            },
            UnblockedTask {
                note_id: "td".into(),
                title: "Discussion one".into(),
                reason: UnblockedReason::DepsSatisfied,
                attention: Some(TaskStatus::DiscussionNeeded),
            },
            UnblockedTask {
                note_id: "tr".into(),
                title: "Review one".into(),
                reason: UnblockedReason::DepsSatisfied,
                attention: Some(TaskStatus::ReviewRequired),
            },
        ];
        let section = render_unblocked_section(&delta, false);
        assert_eq!(
            section,
            "Tasks now unblocked by this completion: \
             [Blocked one](intent://local/task/tb) \
             (deps satisfied; currently blocked — needs attention), \
             [Waiting one](intent://local/task/tw) \
             (conflict cleared; currently waiting — needs attention), \
             [Discussion one](intent://local/task/td) \
             (deps satisfied; currently discussion_needed — needs attention), \
             [Review one](intent://local/task/tr) \
             (deps satisfied; currently review_required — needs attention)."
        );
    }

    #[test]
    fn output_is_sorted_by_title_then_id_and_falls_back_to_id() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("z1".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        snaps.insert("a2".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        snaps.insert("m3".into(), snap(TaskStatus::NotStarted, &["done"], &[]));
        // Titles order z1 first; a2/m3 share a title and tie-break by id;
        // m3 has no title and falls back to its note id.
        let titles = titles(&[("z1", "Alpha"), ("a2", "Beta")]);
        let delta = ready_set_delta(&ids(&["done"]), &snaps, &titles);
        let ordered: Vec<(&str, &str)> = delta
            .iter()
            .map(|u| (u.title.as_str(), u.note_id.as_str()))
            .collect();
        assert_eq!(ordered, vec![("Alpha", "z1"), ("Beta", "a2"), ("m3", "m3")]);
    }
}
