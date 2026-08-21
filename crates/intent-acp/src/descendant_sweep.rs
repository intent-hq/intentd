//! Shared descendant-sweep backstop for process-tree kill paths (unix only).
//!
//! Group signalling (`killpg` on a child spawned with `process_group(0)`) is
//! not enough on its own: children can start MCP servers or helpers that move
//! into their OWN process groups (observed live: codex-acp's auggie MCP
//! server ran as its own group leader, so `killpg` on the spawn group never
//! reached it and it survived orphaned). Every kill path therefore snapshots
//! the child's descendant pids *before* killing ([`descendant_pids`] /
//! [`descendant_pids_many`]) and sweeps any survivors afterwards, regardless
//! of process group ([`sweep_escaped_descendants`]).
//!
//! Snapshot-before-kill is the only viable ordering: post-kill, escaped
//! descendants reparent to init and are invisible to a ppid walk. Anything
//! spawned between the snapshot and the kill is missed — accepted for a
//! best-effort backstop. The happy path pays one short-lived `ps` per
//! teardown; every stage is bounded and non-fatal on failure.

use std::process::Stdio;
use std::time::Duration;

/// Per-root budget for the pre-kill snapshot. The walk stops after
/// `DESCENDANT_SWEEP_CAP × roots` pids total (a shared pool scaled by root
/// count, not enforced per individual root) — keeps the backstop sweep cheap
/// even against a pathological fork storm.
const DESCENDANT_SWEEP_CAP: usize = 64;
/// Timeout on the `ps` snapshot; a hung `ps` must not stall reaping.
const PS_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(2);
/// Grace window between the sweep's SIGTERM and SIGKILL passes —
/// intentionally short and fixed, independent of whatever grace the calling
/// group-kill path used, since escapees already ignored the group signal.
const SWEEP_TERM_GRACE: Duration = Duration::from_millis(500);

/// Snapshot one child's descendant pids. See [`descendant_pids_many`].
pub async fn descendant_pids(root: u32) -> Vec<i32> {
    descendant_pids_many(&[root]).await
}

#[allow(clippy::similar_names)] // pid/pgid/ppid are the POSIX terms
/// Snapshot the descendant pids of every `root` by walking the system
/// `pid → ppid` table (`ps -axo pid=,ppid=`, portable across macOS and
/// Linux) — ONE `ps` invocation covers the whole batch. Best-effort: any
/// failure (spawn error, timeout, unparseable output) yields an empty
/// snapshot rather than an error — the sweep is a backstop, never a kill
/// failure.
pub async fn descendant_pids_many(roots: &[u32]) -> Vec<i32> {
    let mut roots: Vec<i32> = roots
        .iter()
        .filter_map(|&r| i32::try_from(r).ok())
        .collect();
    // Dedupe so repeated roots neither inflate the snapshot cap (which scales
    // with root count) nor add redundant BFS work.
    roots.sort_unstable();
    roots.dedup();
    if roots.is_empty() {
        return Vec::new();
    }
    let mut ps = tokio::process::Command::new("ps");
    ps.args(["-axo", "pid=,ppid="])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let output = match tokio::time::timeout(PS_SNAPSHOT_TIMEOUT, async {
        ps.spawn()?.wait_with_output().await
    })
    .await
    {
        Ok(Ok(out)) if out.status.success() => out.stdout,
        _ => return Vec::new(),
    };
    let table: Vec<(i32, i32)> = String::from_utf8_lossy(&output)
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let pid = parts.next()?.parse().ok()?;
            let ppid = parts.next()?.parse().ok()?;
            Some((pid, ppid))
        })
        .collect();
    descendants_in_table(&table, &roots)
}

/// Breadth-first walk of a `(pid, ppid)` table from every `root`, returning
/// descendant pids (children, grandchildren, …) de-duplicated across roots
/// and capped at [`DESCENDANT_SWEEP_CAP`] × root count overall (a shared
/// pool, so one prolific root may use more than its share). Cycle-safe via a
/// visited set even though real ppid tables are acyclic.
fn descendants_in_table(table: &[(i32, i32)], roots: &[i32]) -> Vec<i32> {
    use std::collections::{HashSet, VecDeque};
    let cap = DESCENDANT_SWEEP_CAP.saturating_mul(roots.len().max(1));
    let mut out = Vec::new();
    let mut seen: HashSet<i32> = roots.iter().copied().collect();
    let mut queue: VecDeque<i32> = roots.iter().copied().collect();
    while let Some(parent) = queue.pop_front() {
        for &(pid, ppid) in table {
            if ppid == parent && seen.insert(pid) {
                out.push(pid);
                queue.push_back(pid);
                if out.len() >= cap {
                    return out;
                }
            }
        }
    }
    out
}

#[allow(clippy::similar_names)] // pid/pgid/ppid are the POSIX terms
/// SIGTERM → grace → SIGKILL any snapshotted descendants that survived the
/// group kill (i.e. escaped into a foreign process group). Pids that already
/// died — or whose pid was recycled into the daemon's own process group — are
/// skipped, and the SIGKILL pass only revisits pids that were alive at the
/// SIGTERM pass. This bounds (but, like any pid-based sweep, cannot fully
/// eliminate) the window in which a recycled pid could be signalled. Returns
/// immediately (no grace wait) when nothing survived — the common case —
/// though a group-killed descendant that init has not reaped yet still
/// passes the liveness check (`getpgid` succeeds on zombies), in which case
/// the sweep harmlessly signals it and pays one grace window.
pub async fn sweep_escaped_descendants(pids: &[i32]) {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::{getpgid, Pid};
    let own_pgid = getpgid(None).ok();
    let survivor = |pid: i32| -> Option<Pid> {
        if pid <= 1 {
            return None;
        }
        let pid = Pid::from_raw(pid);
        let pgid = getpgid(Some(pid)).ok()?;
        if Some(pgid) == own_pgid {
            return None;
        }
        Some(pid)
    };
    let survivors: Vec<Pid> = pids.iter().filter_map(|&p| survivor(p)).collect();
    if survivors.is_empty() {
        return;
    }
    for pid in &survivors {
        let _ = kill(*pid, Signal::SIGTERM);
    }
    tokio::time::sleep(SWEEP_TERM_GRACE).await;
    for pid in survivors.iter().filter_map(|p| survivor(p.as_raw())) {
        let _ = kill(pid, Signal::SIGKILL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descendants_in_table_walks_transitively() {
        let table = [(10, 1), (11, 10), (12, 10), (13, 11), (99, 98)];
        let mut got = descendants_in_table(&table, &[10]);
        got.sort_unstable();
        assert_eq!(got, vec![11, 12, 13]);
    }

    #[test]
    fn descendants_in_table_is_cycle_safe_and_capped() {
        let cycle = [(11, 10), (10, 11)];
        assert_eq!(descendants_in_table(&cycle, &[10]), vec![11]);

        let wide: Vec<(i32, i32)> = (0..200).map(|i| (100 + i, 10)).collect();
        assert_eq!(
            descendants_in_table(&wide, &[10]).len(),
            DESCENDANT_SWEEP_CAP,
            "snapshot must stay bounded"
        );
    }

    #[test]
    fn descendants_in_table_walks_multiple_roots_deduplicated() {
        // 10 and 20 are separate roots; 30 is reachable from both (via 11 and
        // 21) and must be reported once. 99 is unrelated.
        let table = [(11, 10), (21, 20), (30, 11), (30, 21), (99, 98)];
        let mut got = descendants_in_table(&table, &[10, 20]);
        got.sort_unstable();
        assert_eq!(got, vec![11, 21, 30]);
    }

    #[test]
    fn descendants_in_table_cap_scales_with_root_count() {
        let mut wide: Vec<(i32, i32)> = (0..200).map(|i| (1000 + i, 10)).collect();
        wide.extend((0..200).map(|i| (2000 + i, 20)));
        assert_eq!(
            descendants_in_table(&wide, &[10, 20]).len(),
            DESCENDANT_SWEEP_CAP * 2,
            "per-root cap must scale with the batch size"
        );
    }
}
