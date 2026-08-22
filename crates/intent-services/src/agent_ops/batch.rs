//! Batch `agent.delegate` classification (PROTOCOL §5.5).
//!
//! Pure, stateless helpers behind the `tasks: [entry]` batch form: given a
//! snapshot of the workspace's task notes (status, `dependsOn`,
//! `conflictsWith`, live assigned agent) they classify every requested task as
//! start / held / skipped and project the unlock plan. The functions write no
//! scheduler state — the delegate op re-runs them on every call, which is what
//! makes re-supplying the same list idempotent.
//!
//! Admission is a HEURISTIC, deliberately not an exact solver: makespan
//! minimization under `dependsOn` + `conflictsWith` is NP-complete, and the
//! spec records the decision to use deterministic effort-weighted
//! critical-path list scheduling instead of an external solver crate.
//! Priority = the longest effort-weighted chain of dependents downstream of
//! the task (one topological pass — cheap and exact since `dependsOn` is
//! validated as a DAG at write time); startable tasks are admitted in
//! descending priority, holding any whose `conflictsWith` intersects the
//! admitted/running set. Ties break by more-dependents-unlocked, then
//! shortest-processing-time, then task id — fully deterministic. Classic list
//! scheduling: O(V+E), no search or backtracking, within 2x of optimal for
//! this constraint family. There is no batch-level conflict override (the
//! former `greedy` param was removed): forcing past a conflict hold is the
//! single-task form's job — delegate the held task individually.

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
/// tasks that depend on it. Duplicate `dependsOn` entries on a note dedup to
/// one edge so consumers (the unlocked tie-break) count dependents, not edge
/// occurrences.
fn dependent_edges(snaps: &HashMap<String, BatchTaskSnap>) -> HashMap<&str, Vec<&str>> {
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for (id, snap) in snaps {
        if !is_workable(snap.status) {
            continue;
        }
        let deps: HashSet<&str> = snap
            .depends_on
            .iter()
            .map(std::string::String::as_str)
            .collect();
        for dep in deps {
            dependents.entry(dep).or_default().push(id);
        }
    }
    dependents
}

/// Per-task critical-path detail: effort-weighted priority plus whether the
/// chain attaining it carries at least one explicit (parsed) estimate.
type PathDetail = (u64, bool);

/// Effort-weighted critical-path detail for every workable task: own effort
/// plus the longest chain of workable dependents downstream, computed in one
/// topological (Kahn) pass over the reverse-`dependsOn` graph, alongside
/// whether that chain contains any explicit estimate (vs pure
/// [`DEFAULT_EFFORT_MINUTES`] defaults). `dependsOn` is cycle-checked at
/// write time; leftover nodes from a defensive cycle fall back to their own
/// effort.
fn critical_path_details(snaps: &HashMap<String, BatchTaskSnap>) -> HashMap<String, PathDetail> {
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
        let deps: HashSet<&str> = snap
            .depends_on
            .iter()
            .map(std::string::String::as_str)
            .collect();
        for dep in deps {
            if workable.contains(dep) {
                dependents.entry(dep).or_default().push(id);
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
    // Sinks-first: every dependent's detail exists before its dep's. The
    // estimate flag propagates along max-attaining chains only (any of them
    // on a tie), so it says something about the reported number itself.
    let mut details: HashMap<String, PathDetail> = HashMap::new();
    for id in order.iter().rev() {
        let downstream: Vec<&PathDetail> = dependents
            .get(id)
            .into_iter()
            .flatten()
            .filter_map(|d| details.get(*d))
            .collect();
        let max = downstream.iter().map(|(p, _)| *p).max().unwrap_or(0);
        let chain_estimated = downstream
            .iter()
            .any(|(p, estimated)| *p == max && *estimated);
        let own = &snaps[*id];
        details.insert(
            (*id).to_string(),
            (
                effort_of(own).saturating_add(max),
                own.effort_minutes.is_some() || chain_estimated,
            ),
        );
    }
    for id in &workable {
        details
            .entry((*id).to_string())
            .or_insert_with(|| (effort_of(&snaps[*id]), snaps[*id].effort_minutes.is_some()));
    }
    details
}

/// Effort-weighted critical-path priority for every workable task (see
/// [`critical_path_details`]).
pub(crate) fn critical_path_priorities(
    snaps: &HashMap<String, BatchTaskSnap>,
) -> HashMap<String, u64> {
    critical_path_details(snaps)
        .into_iter()
        .map(|(id, (priority, _))| (id, priority))
        .collect()
}

/// Remaining serial work: the longest effort-weighted `dependsOn` chain
/// through the requested tasks whose max-attaining chain carries at least
/// one parsed estimate (their critical-path priority already spans all
/// downstream dependents). Chains that are pure 30-min defaults never
/// contribute — a graph with no estimates anywhere returns `None`, and a
/// longer unestimated chain does not suppress (or inflate) the estimated
/// one, so the reported number can understate when an unestimated chain is
/// longer. Deliberately downstream-only: an incomplete upstream dep
/// outside the requested set does NOT count toward the estimate, so partial
/// batches can understate total remaining serial time.
pub(crate) fn serial_remaining_minutes(
    requested: &[String],
    snaps: &HashMap<String, BatchTaskSnap>,
) -> Option<u64> {
    let details = critical_path_details(snaps);
    let minutes = requested
        .iter()
        .filter_map(|id| details.get(id))
        .filter(|(_, estimated)| *estimated)
        .map(|(p, _)| *p)
        .max()?;
    (minutes > 0).then_some(minutes)
}

/// Requested tasks the relation graph does not cover (monorepo#2457 part 3):
/// own `dependsOn` and `conflictsWith` both empty, AND not referenced by any
/// other requested task's relations. Annotation only — classification
/// (start/hold/skip) is untouched; the delegate op stamps
/// `relationsUnknown: true` on these rows and counts the started ones in the
/// human-readable result summary, so a coordinator can tell "ready by the
/// graph" apart from "the graph says nothing about this task".
pub(crate) fn relations_unknown_ids(
    requested: &[String],
    snaps: &HashMap<String, BatchTaskSnap>,
) -> HashSet<String> {
    let mut referenced: HashSet<&str> = HashSet::new();
    for id in requested {
        if let Some(snap) = snaps.get(id) {
            referenced.extend(snap.depends_on.iter().map(String::as_str));
            referenced.extend(snap.conflicts_with.iter().map(String::as_str));
        }
    }
    requested
        .iter()
        .filter(|id| {
            snaps.get(id.as_str()).is_some_and(|s| {
                s.depends_on.is_empty()
                    && s.conflicts_with.is_empty()
                    && !referenced.contains(id.as_str())
            })
        })
        .cloned()
        .collect()
}

/// Classified disposition for one requested task.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BatchDisposition {
    /// Eligible to start now.
    Start,
    /// Unmet `dependsOn` edges. `decision_needed` is the subset that can
    /// never complete on its own (cancelled or missing task notes).
    HeldOnDeps {
        unmet: Vec<String>,
        decision_needed: Vec<String>,
    },
    /// `conflictsWith` (symmetric closure) intersects the running/starting
    /// set.
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
/// the running set for later entries. Startable tasks are admitted in
/// critical-path-priority order (see the module docs), not request order —
/// the output order still follows the request.
pub(crate) fn classify_batch_tasks(
    requested: &[String],
    snaps: &HashMap<String, BatchTaskSnap>,
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
                    if unmet.is_empty() {
                        startable.push(id.as_str());
                        None
                    } else {
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
                    }
                }
            }
        };
        out.push((id.clone(), disposition));
    }

    // Admission: the deterministic critical-path list scheduling documented
    // on the module — descending priority, ties by more-dependents-unlocked,
    // then shortest effort, then task id.
    {
        let priorities = critical_path_priorities(snaps);
        let dependents = dependent_edges(snaps);
        startable.sort_by(|a, b| {
            let priority = |id: &str| priorities.get(id).copied().unwrap_or(0);
            let unlocked = |id: &str| dependents.get(id).map_or(0, std::vec::Vec::len);
            priority(b)
                .cmp(&priority(a))
                .then(unlocked(b).cmp(&unlocked(a)))
                .then(effort_of(&snaps[*a]).cmp(&effort_of(&snaps[*b])))
                .then(a.cmp(b))
        });
    }
    let mut held_on_conflict: HashSet<&str> = HashSet::new();
    for id in startable {
        let overlaps = conflicts
            .get(id)
            .is_some_and(|neighbors| neighbors.iter().any(|n| active.contains(*n)));
        if overlaps {
            held_on_conflict.insert(id);
        } else {
            active.insert(id);
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
                                .map(std::string::ToString::to_string)
                                .collect()
                        })
                        .unwrap_or_default();
                    overlapping.sort();
                    BatchDisposition::HeldOnConflict {
                        conflicts_with: overlapping,
                    }
                } else {
                    BatchDisposition::Start
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
    let started_set: HashSet<&str> = started.iter().map(std::string::String::as_str).collect();
    let mut simulated = snaps.clone();
    for (id, snap) in &mut simulated {
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
    classify_batch_tasks(&held, &simulated)
        .into_iter()
        .filter(|(_, d)| matches!(d, BatchDisposition::Start))
        .map(|(id, _)| id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(status: TaskStatus, deps: &[&str], conflicts: &[&str]) -> BatchTaskSnap {
        BatchTaskSnap {
            status,
            depends_on: deps.iter().map(std::string::ToString::to_string).collect(),
            conflicts_with: conflicts
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
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
        v.iter().map(std::string::ToString::to_string).collect()
    }

    fn started_ids(out: &[(String, BatchDisposition)]) -> Vec<&str> {
        out.iter()
            .filter(|(_, d)| matches!(d, BatchDisposition::Start))
            .map(|(id, _)| id.as_str())
            .collect()
    }

    // Relation-less annotation (monorepo#2457 part 3): mixed request — the
    // task with no relations flags, the relation-bearing pair does not.
    #[test]
    fn relations_unknown_flags_only_uncovered_tasks_in_a_mixed_request() {
        let mut snaps = HashMap::new();
        snaps.insert("t1".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("t2".into(), snap(TaskStatus::NotStarted, &["t1"], &[]));
        snaps.insert("lone".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let unknown = relations_unknown_ids(&ids(&["t1", "t2", "lone"]), &snaps);
        assert_eq!(unknown, HashSet::from(["lone".to_string()]));
    }

    // All-relation-less request: every task flags.
    #[test]
    fn relations_unknown_flags_every_task_when_none_carry_relations() {
        let mut snaps = HashMap::new();
        snaps.insert("a".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("b".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let unknown = relations_unknown_ids(&ids(&["a", "b"]), &snaps);
        assert_eq!(unknown, HashSet::from(["a".to_string(), "b".to_string()]));
    }

    // A task declaring no relations but referenced by another requested
    // task's `dependsOn` is covered by the graph — NOT flagged. Same for a
    // `conflictsWith` reference. References only count from REQUESTED tasks:
    // an edge from an unrequested note does not cover a requested one.
    #[test]
    fn relations_unknown_spares_tasks_referenced_by_other_requested_tasks() {
        let mut snaps = HashMap::new();
        snaps.insert("dep".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &["dep"], &[]));
        snaps.insert("rival".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("c".into(), snap(TaskStatus::NotStarted, &[], &["rival"]));
        let unknown = relations_unknown_ids(&ids(&["dep", "t", "rival", "c"]), &snaps);
        assert!(unknown.is_empty(), "{unknown:?}");

        // Drop the referencing tasks from the request: the same notes now
        // flag — the graph visible to THIS request does not cover them.
        let unknown = relations_unknown_ids(&ids(&["dep", "rival"]), &snaps);
        assert_eq!(
            unknown,
            HashSet::from(["dep".to_string(), "rival".to_string()])
        );
    }

    #[test]
    fn ready_tasks_start_and_dep_blocked_tasks_hold() {
        let mut snaps = HashMap::new();
        snaps.insert("t1".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("t2".into(), snap(TaskStatus::NotStarted, &["t1"], &[]));
        let out = classify_batch_tasks(&ids(&["t1", "t2"]), &snaps);
        assert_eq!(out[0].1, BatchDisposition::Start);
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
        let out = classify_batch_tasks(&ids(&["t"]), &snaps);
        assert_eq!(
            out[0].1,
            BatchDisposition::HeldOnDeps {
                unmet: vec!["gone".into(), "missing".into()],
                decision_needed: vec!["gone".into(), "missing".into()],
            }
        );
    }

    #[test]
    fn conflict_with_running_task_holds() {
        let mut snaps = HashMap::new();
        // The RUNNING task declares the edge; the requested task does not —
        // symmetric closure must still hold it.
        snaps.insert(
            "busy".into(),
            running(snap(TaskStatus::InProgress, &[], &["t"])),
        );
        snaps.insert("t".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let held = classify_batch_tasks(&ids(&["t"]), &snaps);
        assert_eq!(
            held[0].1,
            BatchDisposition::HeldOnConflict {
                conflicts_with: vec!["busy".into()]
            }
        );
    }

    #[test]
    fn conflict_within_the_same_batch_holds_the_later_entry() {
        let mut snaps = HashMap::new();
        snaps.insert("a".into(), snap(TaskStatus::NotStarted, &[], &["b"]));
        snaps.insert("b".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let out = classify_batch_tasks(&ids(&["a", "b"]), &snaps);
        assert_eq!(out[0].1, BatchDisposition::Start);
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
        let out = classify_batch_tasks(&ids(&["busy", "done", "gone"]), &snaps);
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
        let out = classify_batch_tasks(&ids(&["t", "t"]), &snaps);
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
        let classified = classify_batch_tasks(&ids(&["t1", "t2", "t3", "t4"]), &snaps);
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
        let classified = classify_batch_tasks(&ids(&["busy", "t"]), &snaps);
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
        let classified = classify_batch_tasks(&ids(&["t"]), &snaps);
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
        let classified = classify_batch_tasks(&ids(&["t1", "t2"]), &snaps);
        let unlocked = project_unlock_plan(&classified, &snaps, &[]);
        assert_eq!(unlocked, Vec::<String>::new());
    }

    #[test]
    fn no_two_conflicting_tasks_ever_co_admitted() {
        // Conflict edges a—b, b—c, c—d (declared on one side each; the
        // symmetric closure covers both directions). No start set may
        // contain both endpoints of any edge.
        let mut snaps = HashMap::new();
        snaps.insert("a".into(), snap(TaskStatus::NotStarted, &[], &["b"]));
        snaps.insert("b".into(), snap(TaskStatus::NotStarted, &[], &["c"]));
        snaps.insert("c".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("d".into(), snap(TaskStatus::NotStarted, &[], &["c"]));
        let out = classify_batch_tasks(&ids(&["a", "b", "c", "d"]), &snaps);
        let started = started_ids(&out);
        for (id, s) in &snaps {
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
        // first — admission must still admit `long` (higher critical-path
        // priority) and hold `short`.
        let mut snaps = HashMap::new();
        snaps.insert("long".into(), snap(TaskStatus::NotStarted, &[], &["short"]));
        snaps.insert("mid".into(), snap(TaskStatus::NotStarted, &["long"], &[]));
        snaps.insert("leaf".into(), snap(TaskStatus::NotStarted, &["mid"], &[]));
        snaps.insert("short".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let out = classify_batch_tasks(&ids(&["short", "long"]), &snaps);
        // Output order still follows the request.
        assert_eq!(out[0].0, "short");
        assert_eq!(
            out[0].1,
            BatchDisposition::HeldOnConflict {
                conflicts_with: vec!["long".into()]
            }
        );
        assert_eq!(out[1].1, BatchDisposition::Start);
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
        let out = classify_batch_tasks(&ids(&["chatty", "heavy"]), &snaps);
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
        let out = classify_batch_tasks(&ids(&["v", "u"]), &snaps);
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
        let out = classify_batch_tasks(&ids(&["slow", "zquick"]), &snaps);
        assert_eq!(started_ids(&out), vec!["zquick"]);

        // Everything ties → lexicographically smaller id wins.
        let mut snaps = HashMap::new();
        snaps.insert("x".into(), snap(TaskStatus::NotStarted, &[], &["y"]));
        snaps.insert("y".into(), snap(TaskStatus::NotStarted, &[], &[]));
        let out = classify_batch_tasks(&ids(&["y", "x"]), &snaps);
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
        // No estimates anywhere (pure-defaults graph) → suppressed.
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

    #[test]
    fn serial_remaining_reports_estimated_chain_despite_longer_defaults_chain() {
        // Regression for monorepo#2128 (dogfood shape): an estimated chain
        // b(10) → d(15) → e(5) = 30 min alongside a longer pure-defaults
        // chain g → h = 60 min. The unestimated chain must not suppress the
        // estimated one — report the estimated chain's 30, understating the
        // true remaining serial work.
        let mut snaps = HashMap::new();
        snaps.insert(
            "b".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &[]), 10),
        );
        snaps.insert(
            "d".into(),
            with_effort(snap(TaskStatus::NotStarted, &["b"], &[]), 15),
        );
        snaps.insert(
            "e".into(),
            with_effort(snap(TaskStatus::NotStarted, &["d"], &[]), 5),
        );
        snaps.insert("g".into(), snap(TaskStatus::NotStarted, &[], &[]));
        snaps.insert("h".into(), snap(TaskStatus::NotStarted, &["g"], &[]));
        assert_eq!(
            serial_remaining_minutes(&ids(&["b", "g"]), &snaps),
            Some(30)
        );
        // Estimating anywhere on the longer chain makes it eligible and it
        // takes over as the max.
        snaps.get_mut("h").unwrap().effort_minutes = Some(45);
        assert_eq!(
            serial_remaining_minutes(&ids(&["b", "g"]), &snaps),
            Some(75)
        );
    }

    #[test]
    fn duplicate_depends_on_entries_count_once_in_the_unlock_tie_break() {
        // `u` and `v` conflict with equal critical-path priority (60 each)
        // and one real dependent each — but `vd` duplicates its dep on `v`.
        // Double-counting edges would hand `v` the unlock tie (2 > 1);
        // deduped, the tie falls through to the id tie-break and `u` wins.
        let mut snaps = HashMap::new();
        snaps.insert(
            "u".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &["v"]), 30),
        );
        snaps.insert(
            "ud".into(),
            with_effort(snap(TaskStatus::NotStarted, &["u"], &[]), 30),
        );
        snaps.insert(
            "v".into(),
            with_effort(snap(TaskStatus::NotStarted, &[], &[]), 30),
        );
        snaps.insert(
            "vd".into(),
            with_effort(snap(TaskStatus::NotStarted, &["v", "v"], &[]), 30),
        );
        let out = classify_batch_tasks(&ids(&["v", "u"]), &snaps);
        assert_eq!(started_ids(&out), vec!["u"]);
    }
}
