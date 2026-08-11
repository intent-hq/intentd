//! Batch `agent.delegate` classification (PROTOCOL §5.5).
//!
//! Pure, stateless helpers behind the `tasks: [taskNoteId]` + `greedy` batch
//! form: given a snapshot of the workspace's task notes (status, `dependsOn`,
//! `conflictsWith`, live assigned agent) they classify every requested task as
//! start / held / skipped and project the unlock plan. The functions write no
//! scheduler state — the delegate op re-runs them on every call, which is what
//! makes re-supplying the same list idempotent.
//!
//! Greedy-off admission is a HEURISTIC, deliberately not an exact solver:
//! makespan minimization under `dependsOn` + `conflictsWith` is NP-complete,
//! and the spec records the decision to use deterministic effort-weighted
//! critical-path list scheduling instead of an external solver crate.
//! Priority = the longest effort-weighted chain of dependents downstream of
//! the task (one topological pass — cheap and exact since `dependsOn` is
//! validated as a DAG at write time); startable tasks are admitted in
//! descending priority, holding any whose `conflictsWith` intersects the
//! admitted/running set. Ties break by more-dependents-unlocked, then
//! shortest-processing-time, then task id — fully deterministic. Classic list
//! scheduling: O(V+E), no search or backtracking, within 2x of optimal for
//! this constraint family.

use std::collections::{HashMap, HashSet};

use intent_core::TaskStatus;

use crate::task_effort::DEFAULT_EFFORT_MINUTES;

/// Per-task snapshot the classification runs over. Built by the delegate op
/// from the workspace's notes plus a live-agent scan; `live_agent` is
/// `Some((id, name))` when the task's newest assigned agent is live (the same
/// live/resumable predicate as the single-task occupancy gate).
/// `effort_minutes` is the parsed `estimatedEffort` (None when absent or
/// unparseable — scheduling math then uses [`DEFAULT_EFFORT_MINUTES`]).
#[derive(Debug, Clone, Default)]
pub(crate) struct BatchTaskSnap {
    pub(crate) status: TaskStatus,
    pub(crate) depends_on: Vec<String>,
    pub(crate) conflicts_with: Vec<String>,
    pub(crate) live_agent: Option<(String, String)>,
    pub(crate) effort_minutes: Option<u64>,
}

fn is_workable(status: TaskStatus) -> bool {
    !matches!(status, TaskStatus::Complete | TaskStatus::Cancelled)
}

fn effort_of(snap: &BatchTaskSnap) -> u64 {
    snap.effort_minutes.unwrap_or(DEFAULT_EFFORT_MINUTES)
}

/// Reverse `dependsOn` adjacency over workable tasks: dep id → workable
/// tasks that depend on it.
fn dependent_edges(snaps: &HashMap<String, BatchTaskSnap>) -> HashMap<&str, Vec<&str>> {
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for (id, snap) in snaps {
        if !is_workable(snap.status) {
            continue;
        }
        for dep in &snap.depends_on {
            dependents.entry(dep.as_str()).or_default().push(id);
        }
    }
    dependents
}

/// Effort-weighted critical-path priority for every workable task: own
/// effort plus the longest chain of workable dependents downstream, computed
/// in one topological (Kahn) pass over the reverse-`dependsOn` graph.
/// `dependsOn` is cycle-checked at write time; leftover nodes from a
/// defensive cycle fall back to their own effort.
pub(crate) fn critical_path_priorities(
    snaps: &HashMap<String, BatchTaskSnap>,
) -> HashMap<String, u64> {
    let workable: HashSet<&str> = snaps
        .iter()
        .filter(|(_, s)| is_workable(s.status))
        .map(|(id, _)| id.as_str())
        .collect();
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut indegree: HashMap<&str, usize> = workable.iter().map(|id| (*id, 0)).collect();
    for (id, snap) in snaps {
        if !workable.contains(id.as_str()) {
            continue;
        }
        for dep in &snap.depends_on {
            if workable.contains(dep.as_str()) {
                dependents.entry(dep.as_str()).or_default().push(id);
                *indegree.get_mut(id.as_str()).unwrap() += 1;
            }
        }
    }
    let mut queue: Vec<&str> = indegree
        .iter()
        .filter(|(_, deg)| **deg == 0)
        .map(|(id, _)| *id)
        .collect();
    let mut order: Vec<&str> = Vec::new();
    while let Some(id) = queue.pop() {
        order.push(id);
        for dependent in dependents.get(id).into_iter().flatten() {
            let deg = indegree.get_mut(*dependent).unwrap();
            *deg -= 1;
            if *deg == 0 {
                queue.push(dependent);
            }
        }
    }
    // Sinks-first: every dependent's priority exists before its dep's.
    let mut priorities: HashMap<String, u64> = HashMap::new();
    for id in order.iter().rev() {
        let downstream = dependents
            .get(id)
            .into_iter()
            .flatten()
            .filter_map(|d| priorities.get(*d))
            .max()
            .copied()
            .unwrap_or(0);
        priorities.insert((*id).to_string(), effort_of(&snaps[*id]) + downstream);
    }
    for id in &workable {
        priorities
            .entry((*id).to_string())
            .or_insert_with(|| effort_of(&snaps[*id]));
    }
    priorities
}

/// Remaining serial work: the longest effort-weighted `dependsOn` chain
/// through the requested tasks (their critical-path priority already spans
/// all downstream dependents). `Some` only when at least one workable task in
/// the requested set or its downstream closure carries a parsed estimate —
/// otherwise the number would be pure 30-min defaults and is suppressed.
pub(crate) fn serial_remaining_minutes(
    requested: &[String],
    snaps: &HashMap<String, BatchTaskSnap>,
) -> Option<u64> {
    let dependents = dependent_edges(snaps);
    let mut stack: Vec<&str> = requested
        .iter()
        .map(|s| s.as_str())
        .filter(|id| snaps.get(*id).is_some_and(|s| is_workable(s.status)))
        .collect();
    let mut closure: HashSet<&str> = stack.iter().copied().collect();
    while let Some(id) = stack.pop() {
        for dependent in dependents.get(id).into_iter().flatten() {
            if closure.insert(dependent) {
                stack.push(dependent);
            }
        }
    }
    if !closure
        .iter()
        .any(|id| snaps.get(*id).is_some_and(|s| s.effort_minutes.is_some()))
    {
        return None;
    }
    let priorities = critical_path_priorities(snaps);
    let minutes = requested
        .iter()
        .filter_map(|id| priorities.get(id))
        .max()
        .copied()?;
    (minutes > 0).then_some(minutes)
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
/// the running set for later entries. Under `greedy: false`, startable tasks
/// are admitted in critical-path-priority order (see the module docs), not
/// request order — the output order still follows the request.
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
        .filter(|(_, s)| s.live_agent.is_some() && is_workable(s.status))
        .map(|(id, _)| id.as_str())
        .collect();

    // First pass: fixed dispositions (terminal / running / dep-blocked);
    // conflict-free dep-satisfied tasks are candidates for admission.
    let mut seen = HashSet::new();
    let mut out: Vec<(String, Option<BatchDisposition>)> = Vec::new();
    let mut startable: Vec<&str> = Vec::new();
    for id in requested {
        if !seen.insert(id.as_str()) {
            continue;
        }
        let Some(snap) = snaps.get(id) else {
            continue; // caller validated existence; defensive skip
        };
        let disposition = match snap.status {
            TaskStatus::Complete => Some(BatchDisposition::SkippedComplete),
            TaskStatus::Cancelled => Some(BatchDisposition::SkippedCancelled),
            _ => {
                if let Some((agent_id, agent_name)) = &snap.live_agent {
                    Some(BatchDisposition::SkippedAlreadyRunning {
                        agent_id: agent_id.clone(),
                        agent_name: agent_name.clone(),
                    })
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
                        Some(BatchDisposition::HeldOnDeps {
                            unmet,
                            decision_needed,
                        })
                    } else {
                        startable.push(id.as_str());
                        None
                    }
                }
            }
        };
        out.push((id.clone(), disposition));
    }

    // Admission. Greedy admits everything in request order (conflicts named
    // against the active-so-far set); greedy-off is the deterministic
    // critical-path list scheduling documented on the module: descending
    // priority, ties by more-dependents-unlocked, then shortest effort, then
    // task id.
    if !greedy {
        let priorities = critical_path_priorities(snaps);
        let dependents = dependent_edges(snaps);
        startable.sort_by(|a, b| {
            let priority = |id: &str| priorities.get(id).copied().unwrap_or(0);
            let unlocked = |id: &str| dependents.get(id).map_or(0, |d| d.len());
            priority(b)
                .cmp(&priority(a))
                .then(unlocked(b).cmp(&unlocked(a)))
                .then(effort_of(&snaps[*a]).cmp(&effort_of(&snaps[*b])))
                .then(a.cmp(b))
        });
    }
    let mut admitted_overlaps: HashMap<&str, Vec<String>> = HashMap::new();
    let mut held_on_conflict: HashSet<&str> = HashSet::new();
    for id in startable {
        let mut overlapping: Vec<String> = conflicts
            .get(id)
            .map(|neighbors| {
                neighbors
                    .iter()
                    .filter(|n| active.contains(**n))
                    .map(|n| n.to_string())
                    .collect()
            })
            .unwrap_or_default();
        overlapping.sort();
        if overlapping.is_empty() || greedy {
            active.insert(id);
            admitted_overlaps.insert(id, overlapping);
        } else {
            held_on_conflict.insert(id);
        }
    }

    out.into_iter()
        .map(|(id, disposition)| {
            let disposition = disposition.unwrap_or_else(|| {
                if held_on_conflict.contains(id.as_str()) {
                    // Report overlap against the FINAL admitted/running set:
                    // an admission decided after the hold can add overlap.
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
                    BatchDisposition::HeldOnConflict {
                        conflicts_with: overlapping,
                    }
                } else {
                    BatchDisposition::Start {
                        conflicts_with: admitted_overlaps
                            .get(id.as_str())
                            .cloned()
                            .unwrap_or_default(),
                    }
                }
            });
            (id, disposition)
        })
        .collect()
}

/// Project the unlock plan: which of the held tasks become startable once the
/// currently started/running set settles (their statuses flip to `complete`).
/// `started` is the set of tasks that ACTUALLY started (post-delegation, so a
/// start whose delegation errored never counts as settling); on top of it,
/// EVERY workable task with a live assigned agent settles in the simulation —
/// requested or not — because a running non-requested task holding a
/// requested one via `conflictsWith` releases it at exactly that settlement.
/// Pure re-classification over the simulated snapshot — no state is written;
/// the caller is expected to re-call `agent.delegate` at settlement, which
/// recomputes this for real. Held-on-deps tasks whose unmet deps include a
/// decision-needed (cancelled/missing) dependency never appear: settlement
/// alone cannot unlock them.
pub(crate) fn project_unlock_plan(
    classified: &[(String, BatchDisposition)],
    snaps: &HashMap<String, BatchTaskSnap>,
    started: &[String],
) -> Vec<String> {
    let started_set: HashSet<&str> = started.iter().map(|s| s.as_str()).collect();
    let mut simulated = snaps.clone();
    for (id, snap) in simulated.iter_mut() {
        let running = snap.live_agent.is_some()
            && !matches!(snap.status, TaskStatus::Complete | TaskStatus::Cancelled);
        if started_set.contains(id.as_str()) || running {
            snap.status = TaskStatus::Complete;
            snap.live_agent = None;
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
            effort_minutes: None,
        }
    }

    fn with_effort(mut s: BatchTaskSnap, minutes: u64) -> BatchTaskSnap {
        s.effort_minutes = Some(minutes);
        s
    }

    fn running(mut s: BatchTaskSnap) -> BatchTaskSnap {
        s.live_agent = Some(("agent-1".into(), "Worker".into()));
        s
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn started_ids(out: &[(String, BatchDisposition)]) -> Vec<&str> {
        out.iter()
            .filter(|(_, d)| matches!(d, BatchDisposition::Start { .. }))
            .map(|(id, _)| id.as_str())
            .collect()
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
        let unlocked = project_unlock_plan(&classified, &snaps, &ids(&["t1"]));
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
        let unlocked = project_unlock_plan(&classified, &snaps, &[]);
        assert_eq!(unlocked, vec!["t".to_string()]);
    }

    #[test]
    fn unlock_plan_settles_running_tasks_outside_the_batch() {
        // A non-requested running task holds a requested one via conflict;
        // its settlement is exactly what unlocks the held task, so the plan
        // must name it even though `busy` never appears in `classified`.
        let mut snaps = HashMap::new();
        snaps.insert(
            "busy".into(),
            running(snap(TaskStatus::InProgress, &[], &["t"])),
        );
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let classified = classify_batch_tasks(&ids(&["t"]), &snaps, false);
        assert_eq!(
            classified[0].1,
            BatchDisposition::HeldOnConflict {
                conflicts_with: vec!["busy".into()]
            }
        );
        let unlocked = project_unlock_plan(&classified, &snaps, &[]);
        assert_eq!(unlocked, vec!["t".to_string()]);
    }

    #[test]
    fn unlock_plan_ignores_starts_that_failed_to_delegate() {
        // t2 depends on t1; t1 classified Start but its delegation errored
        // (t1 absent from `started`), so t2 is NOT advertised as unlocked.
        let mut snaps = HashMap::new();
        snaps.insert("t1".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("t2".into(), snap(TaskStatus::NotStarted, &["t1"], &[]));
        let classified = classify_batch_tasks(&ids(&["t1", "t2"]), &snaps, false);
        let unlocked = project_unlock_plan(&classified, &snaps, &[]);
        assert_eq!(unlocked, Vec::<String>::new());
    }

    #[test]
    fn no_two_conflicting_tasks_ever_co_admitted_greedy_off() {
        // Conflict edges a—b, b—c, c—d (declared on one side each; the
        // symmetric closure covers both directions). No greedy-off start set
        // may contain both endpoints of any edge.
        let mut snaps = HashMap::new();
        snaps.insert("a".into(), snap(TaskStatus::NotStarted, &[], &["b"]));
        snaps.insert("b".into(), snap(TaskStatus::NotStarted, &[], &["c"]));
        snaps.insert("c".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("d".into(), snap(TaskStatus::NotStarted, &[], &["c"]));
        let out = classify_batch_tasks(&ids(&["a", "b", "c", "d"]), &snaps, false);
        let started = started_ids(&out);
        for (id, s) in snaps.iter() {
            for other in &s.conflicts_with {
                assert!(
                    !(started.contains(&id.as_str()) && started.contains(&other.as_str())),
                    "conflicting pair {id}—{other} co-admitted: {out:?}"
                );
            }
        }
        assert!(!started.is_empty(), "at least one task admitted: {out:?}");
    }

    #[test]
    fn longer_dependency_chain_admitted_before_equal_conflict_shorter_one() {
        // `short` and `long` conflict; `long` heads a 3-task dependsOn chain
        // while `short` heads a 1-task chain. Request order puts `short`
        // first — greedy-off must still admit `long` (higher critical-path
        // priority) and hold `short`.
        let mut snaps = HashMap::new();
        snaps.insert("long".into(), snap(TaskStatus::NotStarted, &[], &["short"]));
        snaps.insert("mid".into(), snap(TaskStatus::NotStarted, &["long"], &[]));
        snaps.insert("leaf".into(), snap(TaskStatus::NotStarted, &["mid"], &[]));
        snaps.insert("short".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let out = classify_batch_tasks(&ids(&["short", "long"]), &snaps, false);
        // Output order still follows the request.
        assert_eq!(out[0].0, "short");
        assert_eq!(
            out[0].1,
            BatchDisposition::HeldOnConflict {
                conflicts_with: vec!["long".into()]
            }
        );
        assert_eq!(
            out[1].1,
            BatchDisposition::Start {
                conflicts_with: vec![]
            }
        );
    }

    #[test]
    fn effort_weighted_priority_beats_hop_count() {
        // `heavy` unlocks one 300-min dependent; `chatty` heads a 2-hop chain
        // of 10-min tasks (30 total). Effort-weighted critical path admits
        // `heavy` (330 > 30) despite the shorter hop count.
        let mut snaps = HashMap::new();
        snaps.insert(
            "heavy".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &["chatty"]), 30),
        );
        snaps.insert(
            "big".into(),
            with_effort(snap(TaskStatus::NotStarted, &["heavy"], &[]), 300),
        );
        snaps.insert(
            "chatty".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &[]), 10),
        );
        snaps.insert(
            "c2".into(),
            with_effort(snap(TaskStatus::NotStarted, &["chatty"], &[]), 10),
        );
        snaps.insert(
            "c3".into(),
            with_effort(snap(TaskStatus::NotStarted, &["c2"], &[]), 10),
        );
        let out = classify_batch_tasks(&ids(&["chatty", "heavy"]), &snaps, false);
        assert_eq!(started_ids(&out), vec!["heavy"]);
    }

    #[test]
    fn ties_break_by_unlock_count_then_effort_then_id() {
        // Equal critical-path priority (60 each): `u` unlocks two dependents,
        // `v` one — `u` wins the unlock-count tie and `v` is held.
        let mut snaps = HashMap::new();
        snaps.insert(
            "u".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &["v"]), 30),
        );
        snaps.insert(
            "ud1".into(),
            with_effort(snap(TaskStatus::NotStarted, &["u"], &[]), 30),
        );
        snaps.insert(
            "ud2".into(),
            with_effort(snap(TaskStatus::NotStarted, &["u"], &[]), 30),
        );
        snaps.insert(
            "v".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &[]), 30),
        );
        snaps.insert(
            "vd1".into(),
            with_effort(snap(TaskStatus::NotStarted, &["v"], &[]), 30),
        );
        let out = classify_batch_tasks(&ids(&["v", "u"]), &snaps, false);
        assert_eq!(started_ids(&out), vec!["u"]);

        // Equal priority (50) and unlock count (1); own effort 10 vs 20 —
        // shortest-processing-time admits `zquick` even though its id sorts
        // after `slow`.
        let mut snaps = HashMap::new();
        snaps.insert(
            "zquick".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &["slow"]), 10),
        );
        snaps.insert(
            "slow".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &[]), 20),
        );
        snaps.insert(
            "qd".into(),
            with_effort(snap(TaskStatus::NotStarted, &["zquick"], &[]), 40),
        );
        snaps.insert(
            "sd".into(),
            with_effort(snap(TaskStatus::NotStarted, &["slow"], &[]), 30),
        );
        let out = classify_batch_tasks(&ids(&["slow", "zquick"]), &snaps, false);
        assert_eq!(started_ids(&out), vec!["zquick"]);

        // Everything ties → lexicographically smaller id wins.
        let mut snaps = HashMap::new();
        snaps.insert("x".into(), snap(TaskStatus::NotStarted, &[], &["y"]));
        snaps.insert("y".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let out = classify_batch_tasks(&ids(&["y", "x"]), &snaps, false);
        assert_eq!(started_ids(&out), vec!["x"]);
    }

    #[test]
    fn critical_path_priorities_span_chains_and_skip_terminal_tasks() {
        let mut snaps = HashMap::new();
        snaps.insert(
            "a".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &[]), 60),
        );
        snaps.insert(
            "b".into(),
            with_effort(snap(TaskStatus::NotStarted, &["a"], &[]), 120),
        );
        // Unestimated → 30-min default in the chain math.
        snaps.insert("c".into(), snap(TaskStatus::NotStarted, &["b"], &[]));
        // Terminal dependents contribute nothing.
        snaps.insert(
            "done".into(),
            with_effort(snap(TaskStatus::Complete, &["a"], &[]), 999),
        );
        let priorities = critical_path_priorities(&snaps);
        assert_eq!(priorities.get("c"), Some(&30));
        assert_eq!(priorities.get("b"), Some(&150));
        assert_eq!(priorities.get("a"), Some(&210));
        assert!(!priorities.contains_key("done"));
    }

    #[test]
    fn serial_remaining_minutes_requires_estimates_and_spans_downstream() {
        // No estimates anywhere → suppressed.
        let mut snaps = HashMap::new();
        snaps.insert("a".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("b".into(), snap(TaskStatus::NotStarted, &["a"], &[]));
        assert_eq!(serial_remaining_minutes(&ids(&["a"]), &snaps), None);

        // A downstream (non-requested) estimate is enough to surface the
        // number; the unestimated requested task uses the 30-min default.
        snaps.get_mut("b").unwrap().effort_minutes = Some(90);
        assert_eq!(serial_remaining_minutes(&ids(&["a"]), &snaps), Some(120));

        // Terminal requested tasks contribute nothing.
        snaps.get_mut("a").unwrap().status = TaskStatus::Complete;
        assert_eq!(serial_remaining_minutes(&ids(&["a"]), &snaps), None);
        // ...but a workable requested task still reports its own chain.
        assert_eq!(serial_remaining_minutes(&ids(&["b"]), &snaps), Some(90));
    }
}
