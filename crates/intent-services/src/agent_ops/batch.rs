//! Batch `agent.delegate` classification (PROTOCOL §5.5).
//!
//! Pure, stateless helpers behind the `tasks: [taskNoteId]` + `greedy` batch
//! form: given a snapshot of the workspace's task notes (status, `dependsOn`,
//! `conflictsWith`, live assigned agent) they classify every requested task as
//! start / held / skipped and project the unlock plan. The functions write no
//! scheduler state — the delegate op re-runs them on every call, which is what
//! makes re-supplying the same list idempotent.

use std::collections::{HashMap, HashSet};

use intent_core::TaskStatus;

/// Per-task snapshot the classification runs over. Built by the delegate op
/// from the workspace's notes plus a live-agent scan; `live_agent` is
/// `Some((id, name))` when the task's newest assigned agent is live (the same
/// live/resumable predicate as the single-task occupancy gate).
#[derive(Debug, Clone, Default)]
pub(crate) struct BatchTaskSnap {
    pub(crate) status: TaskStatus,
    pub(crate) depends_on: Vec<String>,
    pub(crate) conflicts_with: Vec<String>,
    pub(crate) live_agent: Option<(String, String)>,
}

/// Classified disposition for one requested task.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BatchDisposition {
    /// Eligible to start now. `conflicts_with` is non-empty only under
    /// `greedy: true`, naming the running/starting tasks it overlaps with.
    Start {
        conflicts_with: Vec<String>,
    },
    /// Unmet `dependsOn` edges. `decision_needed` is the subset that can
    /// never complete on its own (cancelled or missing task notes).
    HeldOnDeps {
        unmet: Vec<String>,
        decision_needed: Vec<String>,
    },
    /// `conflictsWith` (symmetric closure) intersects the running/starting
    /// set and `greedy` is false.
    HeldOnConflict {
        conflicts_with: Vec<String>,
    },
    /// Already has a live assigned agent — the idempotency case.
    SkippedAlreadyRunning {
        agent_id: String,
        agent_name: String,
    },
    /// Terminal statuses need no agent.
    SkippedComplete,
    SkippedCancelled,
}

/// Classify `requested` (deduped, order-preserving) against the snapshot.
/// Pure function of current state: task statuses, `dependsOn` /
/// `conflictsWith` edges, and which tasks have live assigned agents. The
/// conflict relation is treated as symmetric (undirected closure over the
/// whole snapshot), and tasks started earlier in this same batch count toward
/// the running set for later entries.
pub(crate) fn classify_batch_tasks(
    requested: &[String],
    snaps: &HashMap<String, BatchTaskSnap>,
    greedy: bool,
) -> Vec<(String, BatchDisposition)> {
    // Symmetric conflict adjacency over the full snapshot: A conflictsWith B
    // holds in both directions regardless of which note declares the edge.
    let mut conflicts: HashMap<&str, HashSet<&str>> = HashMap::new();
    for (id, snap) in snaps {
        for other in &snap.conflicts_with {
            conflicts.entry(id.as_str()).or_default().insert(other);
            conflicts.entry(other.as_str()).or_default().insert(id);
        }
    }
    // Running set: tasks with a live assigned agent that are still workable.
    let mut active: HashSet<&str> = snaps
        .iter()
        .filter(|(_, s)| {
            s.live_agent.is_some()
                && !matches!(s.status, TaskStatus::Complete | TaskStatus::Cancelled)
        })
        .map(|(id, _)| id.as_str())
        .collect();

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for id in requested {
        if !seen.insert(id.as_str()) {
            continue;
        }
        let Some(snap) = snaps.get(id) else {
            continue; // caller validated existence; defensive skip
        };
        let disposition = match snap.status {
            TaskStatus::Complete => BatchDisposition::SkippedComplete,
            TaskStatus::Cancelled => BatchDisposition::SkippedCancelled,
            _ => {
                if let Some((agent_id, agent_name)) = &snap.live_agent {
                    BatchDisposition::SkippedAlreadyRunning {
                        agent_id: agent_id.clone(),
                        agent_name: agent_name.clone(),
                    }
                } else {
                    let unmet: Vec<String> = snap
                        .depends_on
                        .iter()
                        .filter(|dep| {
                            snaps.get(dep.as_str()).map(|d| d.status) != Some(TaskStatus::Complete)
                        })
                        .cloned()
                        .collect();
                    if !unmet.is_empty() {
                        let decision_needed = unmet
                            .iter()
                            .filter(|dep| {
                                match snaps.get(dep.as_str()) {
                                    // Cancelled can never complete; missing
                                    // task notes can never complete either.
                                    Some(d) => d.status == TaskStatus::Cancelled,
                                    None => true,
                                }
                            })
                            .cloned()
                            .collect();
                        BatchDisposition::HeldOnDeps {
                            unmet,
                            decision_needed,
                        }
                    } else {
                        let mut overlapping: Vec<String> = conflicts
                            .get(id.as_str())
                            .map(|neighbors| {
                                neighbors
                                    .iter()
                                    .filter(|n| active.contains(**n))
                                    .map(|n| n.to_string())
                                    .collect()
                            })
                            .unwrap_or_default();
                        overlapping.sort();
                        if overlapping.is_empty() {
                            active.insert(id.as_str());
                            BatchDisposition::Start {
                                conflicts_with: Vec::new(),
                            }
                        } else if greedy {
                            active.insert(id.as_str());
                            BatchDisposition::Start {
                                conflicts_with: overlapping,
                            }
                        } else {
                            BatchDisposition::HeldOnConflict {
                                conflicts_with: overlapping,
                            }
                        }
                    }
                }
            }
        };
        out.push((id.clone(), disposition));
    }
    out
}

/// Project the unlock plan: which of the held tasks become startable once the
/// currently started/running set settles (their statuses flip to `complete`).
/// Pure re-classification over the simulated snapshot — no state is written;
/// the caller is expected to re-call `agent.delegate` at settlement, which
/// recomputes this for real. Held-on-deps tasks whose unmet deps include a
/// decision-needed (cancelled/missing) dependency never appear: settlement
/// alone cannot unlock them.
pub(crate) fn project_unlock_plan(
    classified: &[(String, BatchDisposition)],
    snaps: &HashMap<String, BatchTaskSnap>,
) -> Vec<String> {
    let mut simulated = snaps.clone();
    for (id, disposition) in classified {
        let settles = matches!(
            disposition,
            BatchDisposition::Start { .. } | BatchDisposition::SkippedAlreadyRunning { .. }
        );
        if settles {
            if let Some(snap) = simulated.get_mut(id) {
                snap.status = TaskStatus::Complete;
                snap.live_agent = None;
            }
        }
    }
    let held: Vec<String> = classified
        .iter()
        .filter(|(_, d)| {
            matches!(
                d,
                BatchDisposition::HeldOnDeps { .. } | BatchDisposition::HeldOnConflict { .. }
            )
        })
        .map(|(id, _)| id.clone())
        .collect();
    classify_batch_tasks(&held, &simulated, false)
        .into_iter()
        .filter(|(_, d)| matches!(d, BatchDisposition::Start { .. }))
        .map(|(id, _)| id)
        .collect()
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
        }
    }

    fn running(mut s: BatchTaskSnap) -> BatchTaskSnap {
        s.live_agent = Some(("agent-1".into(), "Worker".into()));
        s
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn ready_tasks_start_and_dep_blocked_tasks_hold() {
        let mut snaps = HashMap::new();
        snaps.insert("t1".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("t2".into(), snap(TaskStatus::NotStarted, &["t1"], &[]));
        let out = classify_batch_tasks(&ids(&["t1", "t2"]), &snaps, false);
        assert_eq!(
            out[0].1,
            BatchDisposition::Start {
                conflicts_with: vec![]
            }
        );
        assert_eq!(
            out[1].1,
            BatchDisposition::HeldOnDeps {
                unmet: vec!["t1".into()],
                decision_needed: vec![],
            }
        );
    }

    #[test]
    fn complete_dep_satisfies_and_cancelled_or_missing_dep_needs_decision() {
        let mut snaps = HashMap::new();
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("gone".into(), snap(TaskStatus::Cancelled, &[], &[]));
        snaps.insert(
            "t".into(),
            snap(TaskStatus::NotStarted, &["done", "gone", "missing"], &[]),
        );
        let out = classify_batch_tasks(&ids(&["t"]), &snaps, false);
        assert_eq!(
            out[0].1,
            BatchDisposition::HeldOnDeps {
                unmet: vec!["gone".into(), "missing".into()],
                decision_needed: vec!["gone".into(), "missing".into()],
            }
        );
    }

    #[test]
    fn conflict_with_running_task_holds_unless_greedy() {
        let mut snaps = HashMap::new();
        // The RUNNING task declares the edge; the requested task does not —
        // symmetric closure must still hold it.
        snaps.insert(
            "busy".into(),
            running(snap(TaskStatus::InProgress, &[], &["t"])),
        );
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let held = classify_batch_tasks(&ids(&["t"]), &snaps, false);
        assert_eq!(
            held[0].1,
            BatchDisposition::HeldOnConflict {
                conflicts_with: vec!["busy".into()]
            }
        );
        let greedy = classify_batch_tasks(&ids(&["t"]), &snaps, true);
        assert_eq!(
            greedy[0].1,
            BatchDisposition::Start {
                conflicts_with: vec!["busy".into()]
            }
        );
    }

    #[test]
    fn conflict_within_the_same_batch_holds_the_later_entry() {
        let mut snaps = HashMap::new();
        snaps.insert("a".into(), snap(TaskStatus::NotStarted, &[], &["b"]));
        snaps.insert("b".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let out = classify_batch_tasks(&ids(&["a", "b"]), &snaps, false);
        assert_eq!(
            out[0].1,
            BatchDisposition::Start {
                conflicts_with: vec![]
            }
        );
        assert_eq!(
            out[1].1,
            BatchDisposition::HeldOnConflict {
                conflicts_with: vec!["a".into()]
            }
        );
    }

    #[test]
    fn running_and_terminal_tasks_skip_idempotently() {
        let mut snaps = HashMap::new();
        snaps.insert(
            "busy".into(),
            running(snap(TaskStatus::InProgress, &[], &[])),
        );
        snaps.insert("done".into(), snap(TaskStatus::Complete, &[], &[]));
        snaps.insert("gone".into(), snap(TaskStatus::Cancelled, &[], &[]));
        let out = classify_batch_tasks(&ids(&["busy", "done", "gone"]), &snaps, false);
        assert_eq!(
            out[0].1,
            BatchDisposition::SkippedAlreadyRunning {
                agent_id: "agent-1".into(),
                agent_name: "Worker".into(),
            }
        );
        assert_eq!(out[1].1, BatchDisposition::SkippedComplete);
        assert_eq!(out[2].1, BatchDisposition::SkippedCancelled);
    }

    #[test]
    fn duplicate_requested_ids_classify_once() {
        let mut snaps = HashMap::new();
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let out = classify_batch_tasks(&ids(&["t", "t"]), &snaps, false);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn unlock_plan_projects_dep_and_conflict_release_at_settlement() {
        let mut snaps = HashMap::new();
        snaps.insert("t1".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("t2".into(), snap(TaskStatus::NotStarted, &["t1"], &[]));
        snaps.insert("t3".into(), snap(TaskStatus::NotStarted, &[], &["t1"]));
        // t4 depends on a cancelled task: never unlocked by settlement.
        snaps.insert("dead".into(), snap(TaskStatus::Cancelled, &[], &[]));
        snaps.insert("t4".into(), snap(TaskStatus::NotStarted, &["dead"], &[]));
        let classified = classify_batch_tasks(&ids(&["t1", "t2", "t3", "t4"]), &snaps, false);
        let unlocked = project_unlock_plan(&classified, &snaps);
        assert_eq!(unlocked, vec!["t2".to_string(), "t3".to_string()]);
    }

    #[test]
    fn unlock_plan_counts_already_running_tasks_as_settling() {
        let mut snaps = HashMap::new();
        snaps.insert(
            "busy".into(),
            running(snap(TaskStatus::InProgress, &[], &[])),
        );
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &["busy"], &[]));
        let classified = classify_batch_tasks(&ids(&["busy", "t"]), &snaps, false);
        let unlocked = project_unlock_plan(&classified, &snaps);
        assert_eq!(unlocked, vec!["t".to_string()]);
    }
}
