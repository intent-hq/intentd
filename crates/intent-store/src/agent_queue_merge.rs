//! Three-way merge of one agent's send queue, so a process whose view of the
//! queue is out of date can still apply its own change without overwriting
//! another process's (stateless replicas groundwork).
//!
//! The inputs are plain [`AgentQueueRow`]s; payloads stay opaque:
//! - `base`: what this process last read or wrote (ids and row digests),
//! - `local`: this process's queue now (base plus its own mutations),
//! - `stored`: the queue as the database holds it now.
//!
//! The result keeps every change of `local` relative to `base`, and every
//! change another process made to the stored queue since:
//! 1. An entry `local` removed is removed; one another process removed stays
//!    removed, even if `local` still has it.
//! 2. An entry both sides still have takes the local row when `local`
//!    changed it since `base`, the stored row otherwise.
//! 3. Entries both sides know keep the local order. An entry only the stored
//!    queue has (another process added it) stays right after its stored
//!    predecessor among the kept entries, ahead of entries `local` added
//!    there: entries added at one point land in commit order.
//!
//! With `stored` equal to `base` the result is `local` itself.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use intent_core::AgentId;

use crate::AgentQueueRow;

/// A queue as one process last saw it: entry id to row digest.
pub type AgentQueueBase = HashMap<String, u64>;

/// Digest of everything in a row but its position (and agent, the key).
#[must_use]
pub fn agent_queue_row_digest(row: &AgentQueueRow) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.id.hash(&mut hasher);
    row.payload.to_string().hash(&mut hasher);
    row.created_at.hash(&mut hasher);
    row.turn_id.hash(&mut hasher);
    hasher.finish()
}

/// The base of `rows`: what a process saw after reading or writing them.
#[must_use]
pub fn agent_queue_base(rows: &[AgentQueueRow]) -> AgentQueueBase {
    rows.iter()
        .map(|row| (row.id.clone(), agent_queue_row_digest(row)))
        .collect()
}

/// Merges `local`'s change since `base` onto `stored` (see the module docs).
/// Positions are renumbered from 0 and every row is owned by `agent_id`.
#[must_use]
pub fn merge_agent_queue(
    agent_id: &AgentId,
    base: &AgentQueueBase,
    local: &[AgentQueueRow],
    stored: &[AgentQueueRow],
) -> Vec<AgentQueueRow> {
    let stored_by_id: HashMap<&str, &AgentQueueRow> =
        stored.iter().map(|row| (row.id.as_str(), row)).collect();
    let local_ids: HashSet<&str> = local.iter().map(|row| row.id.as_str()).collect();
    // A local entry another process removed since `base` stays removed.
    let kept: Vec<&AgentQueueRow> = local
        .iter()
        .filter(|row| !base.contains_key(&row.id) || stored_by_id.contains_key(row.id.as_str()))
        .collect();
    let kept_ids: HashSet<&str> = kept.iter().map(|row| row.id.as_str()).collect();
    // Entries another process added: in neither `base` nor `local`.
    let mut foreign_after: HashMap<Option<&str>, Vec<&AgentQueueRow>> = HashMap::new();
    let mut anchor: Option<&str> = None;
    // Foreign entries after the last kept stored entry: the stored tail.
    let mut tail: Vec<&AgentQueueRow> = Vec::new();
    for row in stored {
        let id = row.id.as_str();
        if kept_ids.contains(id) {
            foreign_after.entry(anchor).or_default().append(&mut tail);
            anchor = Some(id);
        } else if !base.contains_key(id) && !local_ids.contains(id) {
            tail.push(row);
        }
    }
    // The tail stays a tail even when `local` reordered: it follows the
    // last entry of `local` that the stored queue also holds.
    let tail_anchor = kept
        .iter()
        .rev()
        .map(|row| row.id.as_str())
        .find(|id| stored_by_id.contains_key(id));
    foreign_after
        .entry(tail_anchor)
        .or_default()
        .append(&mut tail);
    let pick = |row: &AgentQueueRow| -> AgentQueueRow {
        let unchanged_locally = base
            .get(&row.id)
            .is_some_and(|digest| *digest == agent_queue_row_digest(row));
        match stored_by_id.get(row.id.as_str()) {
            Some(stored) if unchanged_locally => (*stored).clone(),
            _ => row.clone(),
        }
    };
    let mut merged: Vec<AgentQueueRow> = Vec::with_capacity(kept.len() + stored.len());
    merged.extend(foreign_after.remove(&None).into_iter().flatten().cloned());
    for row in kept {
        merged.push(pick(row));
        merged.extend(
            foreign_after
                .remove(&Some(row.id.as_str()))
                .into_iter()
                .flatten()
                .cloned(),
        );
    }
    for (position, row) in merged.iter_mut().enumerate() {
        row.position = i64::try_from(position).unwrap_or(i64::MAX);
        row.agent_id = agent_id.clone();
    }
    merged
}

/// The ids among `claims` this process saw stored (`base`) that the stored
/// queue no longer holds: another process removed them (drained or
/// cancelled) first. An id this process never saw stored was never visible
/// to another process, so it is never lost.
#[must_use]
pub fn lost_agent_queue_claims(
    base: &AgentQueueBase,
    stored: &[AgentQueueRow],
    claims: &[String],
) -> Vec<String> {
    let stored_ids: HashSet<&str> = stored.iter().map(|row| row.id.as_str()).collect();
    claims
        .iter()
        .filter(|id| base.contains_key(*id) && !stored_ids.contains(id.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn agent() -> AgentId {
        AgentId("agent".into())
    }

    fn row(id: &str, text: &str) -> AgentQueueRow {
        AgentQueueRow {
            id: id.into(),
            agent_id: agent(),
            position: 0,
            payload: json!({ "content": text }),
            created_at: "2026-11-25T00:00:00Z".into(),
            turn_id: format!("turn-{id}"),
        }
    }

    fn rows(ids: &[&str]) -> Vec<AgentQueueRow> {
        ids.iter().map(|id| row(id, id)).collect()
    }

    fn ids(rows: &[AgentQueueRow]) -> Vec<&str> {
        rows.iter().map(|row| row.id.as_str()).collect()
    }

    fn merge(base: &[&str], local: &[AgentQueueRow], stored: &[&str]) -> Vec<AgentQueueRow> {
        merge_agent_queue(
            &agent(),
            &agent_queue_base(&rows(base)),
            local,
            &rows(stored),
        )
    }

    #[test]
    fn an_unchanged_stored_queue_yields_local() {
        let local = rows(&["c", "a", "x"]);
        let merged = merge(&["a", "b", "c"], &local, &["a", "b", "c"]);
        assert_eq!(ids(&merged), ["c", "a", "x"]);
        assert_eq!(
            merged.iter().map(|r| r.position).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }

    #[test]
    fn appends_from_two_processes_land_in_commit_order() {
        // The other process appended y first; this one appends x.
        let merged = merge(&["a"], &rows(&["a", "x"]), &["a", "y"]);
        assert_eq!(ids(&merged), ["a", "y", "x"]);
    }

    #[test]
    fn front_inserts_from_two_processes_land_in_commit_order() {
        let merged = merge(&["a"], &rows(&["x", "a"]), &["y", "a"]);
        assert_eq!(ids(&merged), ["y", "x", "a"]);
    }

    #[test]
    fn a_removal_on_either_side_stays_removed() {
        // This process removed a; the other removed b and added y.
        let merged = merge(&["a", "b", "c"], &rows(&["b", "c"]), &["a", "c", "y"]);
        assert_eq!(ids(&merged), ["c", "y"]);
    }

    #[test]
    fn a_foreign_entry_after_a_removed_entry_moves_to_the_previous_kept_one() {
        let merged = merge(&["a", "b"], &rows(&["a"]), &["a", "b", "y"]);
        assert_eq!(ids(&merged), ["a", "y"]);
        let merged = merge(&["a", "b"], &rows(&["b"]), &["a", "y", "b"]);
        assert_eq!(ids(&merged), ["y", "b"]);
    }

    #[test]
    fn a_local_reorder_keeps_its_order_around_foreign_entries() {
        let merged = merge(&["a", "b"], &rows(&["b", "a"]), &["a", "b", "y"]);
        assert_eq!(ids(&merged), ["b", "a", "y"]);
    }

    #[test]
    fn edits_take_the_side_that_changed() {
        let base = ["a", "b"];
        let local = vec![row("a", "edited here"), row("b", "b")];
        let stored = vec![row("a", "a"), row("b", "edited there")];
        let merged = merge_agent_queue(&agent(), &agent_queue_base(&rows(&base)), &local, &stored);
        assert_eq!(merged[0].payload, json!({ "content": "edited here" }));
        assert_eq!(merged[1].payload, json!({ "content": "edited there" }));
    }

    #[test]
    fn an_entry_new_to_this_process_but_already_stored_is_not_duplicated() {
        let merged = merge(&[], &rows(&["a"]), &["a"]);
        assert_eq!(ids(&merged), ["a"]);
    }

    #[test]
    fn lost_claims_are_only_stored_entries_another_process_removed() {
        let base = agent_queue_base(&rows(&["a", "b"]));
        let claims = ["a".to_owned(), "b".to_owned(), "new".to_owned()];
        assert_eq!(
            lost_agent_queue_claims(&base, &rows(&["b"]), &claims),
            ["a"]
        );
        assert!(lost_agent_queue_claims(&base, &rows(&["a", "b"]), &claims).is_empty());
    }
}
