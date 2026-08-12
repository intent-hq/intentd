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

pub(crate) use super::batch::BatchTaskSnap;
use super::batch::{classify_batch_tasks, BatchDisposition};

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
/// the note id when the caller has no title for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnblockedTask {
    pub(crate) note_id: String,
    pub(crate) title: String,
    pub(crate) reason: UnblockedReason,
}

/// Single-task readiness disposition against `snaps`: greedy-off, so
/// `conflictsWith` overlap with running work reports as held, and classifying
/// one id at a time keeps candidates from holding each other.
fn classify_one(id: &str, snaps: &HashMap<String, BatchTaskSnap>) -> Option<BatchDisposition> {
    classify_batch_tasks(&[id.to_string()], snaps, false)
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
/// first). Output is sorted by title then note id for reproducible message
/// rendering.
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
        if !matches!(
            classify_one(id, snaps),
            Some(BatchDisposition::Start { .. })
        ) {
            continue;
        }
        let reason = match classify_one(id, &before) {
            Some(BatchDisposition::HeldOnDeps { .. }) => UnblockedReason::DepsSatisfied,
            Some(BatchDisposition::HeldOnConflict { .. }) => UnblockedReason::ConflictCleared,
            // Ready before the triggers too — not attributable to them.
            _ => continue,
        };
        out.push(UnblockedTask {
            note_id: id.clone(),
            title: titles.get(id).cloned().unwrap_or_else(|| id.clone()),
            reason,
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
