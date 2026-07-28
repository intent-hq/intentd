//! `services::file_tracking` — the BE-internal attribution writer (§17.4, §9.11).
//!
//! Ports the `track-change` half of the TS file-tracking pipeline + attribution
//! engine (`attribution-engine.ts`'s `recordAgentWrite`): it records *which*
//! agent/session/turn changed *which* file as the file moves through git stages
//! (`unstaged → staged → committed → pushed → pr → merged`), upserting one
//! `tracked_changes` row per file per stage. This is **not** a wire method — the
//! FE learns of changes via events and re-reads via the §17.4 reads (M4.8). Raw
//! content stays lazy via the blob SHAs on the row.
//!
//! Per §3.2 this depends only on `intent-store`; it never imports a sibling
//! service module (the diff stats it records are computed by the wiring layer via
//! [`crate::diffs`] and passed in on the [`NewTrackedChange`]).

use intent_core::Result;
use intent_store::{NewTrackedChange, Store};

/// Record (upsert) an attribution row for a file change. The `path` is
/// normalized to a repo-relative, forward-slash form before storage, mirroring
/// the TS attribution engine's `normalizePath` so lookups stay consistent across
/// the git/filesystem/tool path sources.
///
/// Returns the `(lines_added, lines_deleted)` **delta** this recording
/// represents: the row's new cumulative per-file counters minus the replaced
/// row's, clamped ≥ 0 per counter so a shrinking diff (e.g. an agent reverting
/// its own lines) never yields a negative delta. A fresh row baselines against
/// the max counters any sibling row (same workspace/path/stage, other agent)
/// already recorded — each row carries the file's **full** diff, so a second
/// agent's first row must not replay lines a sibling already fed into the
/// usage stats (monorepo#1009); 0 when the path is new. The upsert and the
/// sibling read are not transactional: if two agents' *first* rows for the
/// same brand-new path raced, each would baseline against the other and the
/// initial lines would go unrecorded — acceptable for this best-effort
/// recording, and the pipeline effectively serializes per workspace. Callers
/// feed this growth into the global usage-stats recording (D5).
pub async fn track_change(store: &Store, mut change: NewTrackedChange) -> Result<(u64, u64)> {
    change.path = normalize_path(&change.path);
    let prev = store.upsert_tracked_change(&change).await?;
    let (prev_additions, prev_deletions) = match prev {
        Some(prev) => prev,
        None => store
            .max_sibling_tracked_change_counters(
                &change.workspace_id,
                &change.path,
                &change.stage,
                change.agent_id.as_deref(),
            )
            .await?
            .unwrap_or((0, 0)),
    };
    Ok((
        (change.additions - prev_additions).max(0) as u64,
        (change.deletions - prev_deletions).max(0) as u64,
    ))
}

/// Normalize a file path for consistent attribution lookups (parity with
/// `attribution-engine.ts` `normalizePath`): backslashes → `/`, strip leading
/// `/` and `./`, drop trailing `/`, and collapse repeated slashes.
pub fn normalize_path(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    let unified = path.replace('\\', "/");
    let mut out = String::with_capacity(unified.len());
    let mut prev_slash = false;
    for ch in unified.chars() {
        if ch == '/' {
            if !prev_slash {
                out.push('/');
            }
            prev_slash = true;
        } else {
            out.push(ch);
            prev_slash = false;
        }
    }
    let trimmed = out.trim_start_matches('/').trim_end_matches('/');
    trimmed.strip_prefix("./").unwrap_or(trimmed).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_paths() {
        assert_eq!(normalize_path("./src/main.rs"), "src/main.rs");
        assert_eq!(
            normalize_path("/abs//nested/file.txt"),
            "abs/nested/file.txt"
        );
        assert_eq!(normalize_path("a\\b\\c.rs"), "a/b/c.rs");
        assert_eq!(normalize_path("dir/"), "dir");
        assert_eq!(normalize_path(""), "");
    }
}
