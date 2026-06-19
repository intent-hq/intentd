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
pub async fn track_change(store: &Store, mut change: NewTrackedChange) -> Result<()> {
    change.path = normalize_path(&change.path);
    store.upsert_tracked_change(&change).await
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
