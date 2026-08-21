//! Workspace vocabulary derivation + cache for dictation biasing.
//!
//! For a workspace, gathers markdown sources — root `README.md` / `AGENTS.md`,
//! the workspace spec note, and the `README.md` / `AGENTS.md` of top-level
//! directories and their direct children (e.g. `packages/intentd/AGENTS.md`;
//! the walk never descends further) — runs
//! [`intent_voice::extract_vocabulary`], and caches the ranked terms per
//! workspace keyed on a content hash of the sources plus `maxTerms`.
//! Staleness is probed cheaply (candidate path/size/mtime stats plus the spec
//! note `rev` — no file content reads), so repeated calls with unchanged
//! sources hit the cache without re-reading or re-extracting. Derivation is
//! best-effort: missing files, workspaces, or notes are skipped silently —
//! never an error. `maxTerms = 0` disables derivation entirely (returns empty
//! without walking anything).
//!
//! Not on a hot list RPC: consumers are seconds-scale calls such as
//! `voice.transcribe` (wired separately); the cache bounds repeat cost.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::SystemTime;

use intent_core::{NoteId, WorkspaceId};
use intent_store::Store;
use intent_voice::{extract_vocabulary, SourceKind};
use tokio::sync::Mutex;

/// Settings path of the workspace-vocabulary size cap (a TOML-backed catalog
/// entry — number, default 50, min 0, max 100; PROTOCOL §5.12, v4.6).
pub(crate) const MAX_TERMS_SETTING_PATH: &str = "voice.workspaceVocabulary.maxTerms";

/// Default `voice.workspaceVocabulary.maxTerms` when unset or malformed.
pub(crate) const DEFAULT_MAX_TERMS: usize = 50;

/// Per-file size cap; larger files are skipped outright.
const MAX_SOURCE_FILE_BYTES: u64 = 1024 * 1024;

/// Total content budget across all file sources; gathering stops once spent.
const MAX_TOTAL_SOURCE_BYTES: u64 = 4 * 1024 * 1024;

/// Directory-listing cap per level (defensive bound for huge roots).
const MAX_DIRS_PER_LEVEL: usize = 256;

/// Source file names gathered at each level.
const SOURCE_FILE_NAMES: [&str; 2] = ["README.md", "AGENTS.md"];

/// Directory names never descended into (besides dot-directories).
const SKIP_DIRS: [&str; 2] = ["node_modules", "target"];

/// Parse the stored `voice.workspaceVocabulary.maxTerms` value (raw JSON
/// string from the settings table): a non-negative JSON integer is honored
/// (`0` disables derivation); absent or malformed degrades to
/// [`DEFAULT_MAX_TERMS`] — never an error.
#[cfg(test)]
pub(crate) fn parse_max_terms_setting(raw: Option<&str>) -> usize {
    match raw {
        None => DEFAULT_MAX_TERMS,
        Some(s) => serde_json::from_str::<serde_json::Value>(s)
            .ok()
            .and_then(|v| v.as_u64())
            .map_or(DEFAULT_MAX_TERMS, |n| n as usize),
    }
}

/// Tolerant raw-table fallback for the max-terms cap. The production wire-up
/// reads the TOML-backed catalog entry through the settings service and
/// passes it to [`WorkspaceVocabularyCache::vocabulary_with_max_terms`]; this
/// raw read only serves callers without a settings service (tests). Read
/// failures degrade to the default.
#[cfg(test)]
pub(crate) async fn resolve_max_terms(store: &Store) -> usize {
    let raw = store
        .get_setting(MAX_TERMS_SETTING_PATH)
        .await
        .ok()
        .flatten();
    parse_max_terms_setting(raw.as_deref())
}

/// Parse a catalog-served `voice.workspaceVocabulary.maxTerms` JSON value
/// (the settings service returns numbers as JSON floats): a non-negative
/// finite number is truncated and honored (`0` disables derivation); absent
/// or malformed degrades to [`DEFAULT_MAX_TERMS`] — never an error.
pub(crate) fn parse_max_terms_value(value: Option<&serde_json::Value>) -> usize {
    value
        .and_then(serde_json::Value::as_f64)
        .filter(|n| n.is_finite() && *n >= 0.0)
        .map_or(DEFAULT_MAX_TERMS, |n| n as usize)
}

/// One stat-probed candidate source file (exists, regular file, within the
/// per-file size cap).
#[derive(Debug, Clone)]
struct ProbeEntry {
    path: PathBuf,
    size: u64,
    mtime: Option<SystemTime>,
}

/// Cached derivation for one workspace.
struct CacheEntry {
    /// Hash of the cheap probe: candidate paths + size/mtime, spec note
    /// `rev`/`updatedAt`, and `maxTerms`.
    probe_hash: u64,
    /// Hash of the actual source contents + `maxTerms`; lets a probe change
    /// that did not alter content (e.g. `touch`) skip re-extraction.
    content_hash: u64,
    terms: Vec<String>,
}

/// Per-workspace vocabulary cache. Cheaply shared behind the service layer;
/// all entries live in memory (derivation is deterministic and re-runs on
/// daemon restart).
#[derive(Default)]
pub(crate) struct WorkspaceVocabularyCache {
    entries: Mutex<HashMap<String, CacheEntry>>,
    /// Number of full extractor runs (cache misses); test observability.
    derivations: AtomicUsize,
}

impl WorkspaceVocabularyCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of full derivations (extractor runs) performed so far.
    #[cfg(test)]
    pub(crate) fn derivation_count(&self) -> usize {
        self.derivations.load(Ordering::Relaxed)
    }

    /// Ranked vocabulary for `workspace_id`, reading the
    /// `voice.workspaceVocabulary.maxTerms` setting at derivation time.
    #[cfg(test)]
    pub async fn vocabulary(&self, store: &Store, workspace_id: &WorkspaceId) -> Vec<String> {
        let max_terms = resolve_max_terms(store).await;
        self.vocabulary_with_max_terms(store, workspace_id, max_terms)
            .await
    }

    /// Ranked vocabulary with an explicit cap. `0` returns empty immediately
    /// — no store reads, no filesystem walk.
    pub(crate) async fn vocabulary_with_max_terms(
        &self,
        store: &Store,
        workspace_id: &WorkspaceId,
        max_terms: usize,
    ) -> Vec<String> {
        if max_terms == 0 {
            return Vec::new();
        }
        let root = match store.get_workspace(workspace_id).await {
            Ok(ws) => crate::git_ops::worktree_path(&ws),
            Err(_) => None,
        };
        // Single-row read; `rev`/`updatedAt` double as the staleness probe
        // and the content is reused on recompute (no second fetch).
        let spec = store
            .get_note(workspace_id, &NoteId::from("spec"))
            .await
            .ok();
        let probe = match root.clone() {
            Some(root) => tokio::task::spawn_blocking(move || probe_sources(&root))
                .await
                .unwrap_or_default(),
            None => Vec::new(),
        };
        let probe_hash = hash_probe(&probe, spec.as_ref(), max_terms);
        {
            let cache = self.entries.lock().await;
            if let Some(entry) = cache.get(workspace_id.as_str()) {
                if entry.probe_hash == probe_hash {
                    return entry.terms.clone();
                }
            }
        }
        // Probe miss: read the (bounded) source contents off the runtime.
        let files = tokio::task::spawn_blocking(move || read_sources(&probe))
            .await
            .unwrap_or_default();
        let content_hash =
            hash_content(&files, spec.as_ref().map(|n| n.content.as_str()), max_terms);
        let mut cache = self.entries.lock().await;
        if let Some(entry) = cache.get_mut(workspace_id.as_str()) {
            if entry.content_hash == content_hash {
                // Metadata-only change (e.g. touch): refresh the probe, keep
                // the derived terms without re-running the extractor.
                entry.probe_hash = probe_hash;
                return entry.terms.clone();
            }
        }
        let mut sources: Vec<(SourceKind, &str)> = files
            .iter()
            .map(|(_, text)| (SourceKind::Markdown, text.as_str()))
            .collect();
        if let Some(note) = &spec {
            sources.push((SourceKind::Markdown, note.content.as_str()));
        }
        let terms = extract_vocabulary(&sources, max_terms);
        self.derivations.fetch_add(1, Ordering::Relaxed);
        cache.insert(
            workspace_id.as_str().to_string(),
            CacheEntry {
                probe_hash,
                content_hash,
                terms: terms.clone(),
            },
        );
        terms
    }
}

/// Sorted, bounded listing of a directory's non-hidden child directories,
/// skipping [`SKIP_DIRS`]. Unreadable directories yield an empty list.
fn child_dirs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| !n.starts_with('.') && !SKIP_DIRS.contains(&n.as_str()))
        .collect();
    names.sort();
    names.truncate(MAX_DIRS_PER_LEVEL);
    names.into_iter().map(|n| dir.join(n)).collect()
}

/// Enumerate + stat the candidate source files (no content reads): the root
/// `README.md` / `AGENTS.md`, then the same names in each top-level directory
/// and each of its direct children. Deterministic order (sorted directory
/// names). Non-files and files over the per-file cap are dropped.
fn probe_sources(root: &Path) -> Vec<ProbeEntry> {
    let mut candidates: Vec<PathBuf> = SOURCE_FILE_NAMES.iter().map(|n| root.join(n)).collect();
    for top in child_dirs(root) {
        for name in SOURCE_FILE_NAMES {
            candidates.push(top.join(name));
        }
        for child in child_dirs(&top) {
            for name in SOURCE_FILE_NAMES {
                candidates.push(child.join(name));
            }
        }
    }
    candidates
        .into_iter()
        .filter_map(|path| {
            let meta = std::fs::metadata(&path).ok()?;
            if !meta.is_file() || meta.len() > MAX_SOURCE_FILE_BYTES {
                return None;
            }
            Some(ProbeEntry {
                size: meta.len(),
                mtime: meta.modified().ok(),
                path,
            })
        })
        .collect()
}

/// Read the probed sources as UTF-8, stopping once the total budget is spent.
/// Unreadable / non-UTF-8 files are skipped silently.
fn read_sources(probe: &[ProbeEntry]) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(probe.len());
    let mut total: u64 = 0;
    for entry in probe {
        if total + entry.size > MAX_TOTAL_SOURCE_BYTES {
            break;
        }
        let Ok(text) = std::fs::read_to_string(&entry.path) else {
            continue;
        };
        total += text.len() as u64;
        out.push((entry.path.to_string_lossy().into_owned(), text));
    }
    out
}

/// Hash the cheap staleness probe: file paths + size/mtime, the spec note
/// `rev`/`updatedAt`, and `maxTerms`.
fn hash_probe(probe: &[ProbeEntry], spec: Option<&intent_core::Note>, max_terms: usize) -> u64 {
    let mut h = DefaultHasher::new();
    max_terms.hash(&mut h);
    match spec {
        Some(note) => {
            1u8.hash(&mut h);
            note.rev.hash(&mut h);
            note.updated_at.hash(&mut h);
        }
        None => 0u8.hash(&mut h),
    }
    probe.len().hash(&mut h);
    for entry in probe {
        entry.path.hash(&mut h);
        entry.size.hash(&mut h);
        match entry
            .mtime
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        {
            Some(d) => {
                1u8.hash(&mut h);
                d.as_nanos().hash(&mut h);
            }
            None => 0u8.hash(&mut h),
        }
    }
    h.finish()
}

/// Hash the actual source contents (+ paths, spec content, `maxTerms`) — the
/// cache key proper.
fn hash_content(files: &[(String, String)], spec_content: Option<&str>, max_terms: usize) -> u64 {
    let mut h = DefaultHasher::new();
    max_terms.hash(&mut h);
    spec_content.hash(&mut h);
    files.len().hash(&mut h);
    for (path, text) in files {
        path.hash(&mut h);
        text.hash(&mut h);
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use intent_core::{
        now_iso, ContentType, Note, NoteMetadata, NoteVisibility, Workspace, WorkspaceActivity,
        WorkspaceAttention, WorkspaceStatus,
    };

    use super::*;

    fn workspace(id: &WorkspaceId, worktree: Option<&Path>) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: worktree.map(|p| p.to_string_lossy().into_owned()),
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    fn spec_note(ws: &WorkspaceId, content: &str) -> Note {
        let ts = now_iso();
        Note {
            id: NoteId::from("spec"),
            workspace_id: ws.clone(),
            title: "Spec".to_string(),
            content: content.to_string(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        }
    }

    /// Temp DB + workspace whose worktree points at `root`.
    async fn setup(root: Option<&Path>) -> (tempfile::TempDir, Store, WorkspaceId) {
        let db_dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(&db_dir.path().join("db.sqlite"))
            .await
            .expect("open store");
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&workspace(&ws, root))
            .await
            .expect("ws");
        (db_dir, store, ws)
    }

    fn write(root: &Path, rel: &str, content: &str) {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).expect("mkdir");
        std::fs::write(path, content).expect("write");
    }

    #[test]
    fn parse_max_terms_setting_is_tolerant() {
        assert_eq!(parse_max_terms_setting(None), DEFAULT_MAX_TERMS);
        assert_eq!(parse_max_terms_setting(Some("25")), 25);
        assert_eq!(parse_max_terms_setting(Some("0")), 0, "0 disables");
        for malformed in ["\"abc\"", "-3", "2.5", "null", "{", "[1]"] {
            assert_eq!(
                parse_max_terms_setting(Some(malformed)),
                DEFAULT_MAX_TERMS,
                "expected default for {malformed}"
            );
        }
    }

    #[tokio::test]
    async fn resolve_max_terms_reads_raw_setting_with_default() {
        let (_db, store, _ws) = setup(None).await;
        assert_eq!(resolve_max_terms(&store).await, DEFAULT_MAX_TERMS);
        store
            .set_setting(MAX_TERMS_SETTING_PATH, "7")
            .await
            .expect("set");
        assert_eq!(resolve_max_terms(&store).await, 7);
    }

    #[tokio::test]
    async fn gathers_root_per_package_and_spec_sources() {
        let root = tempfile::tempdir().expect("root");
        write(root.path(), "README.md", "# Root\nZorblatt everywhere.");
        write(root.path(), "AGENTS.md", "Use Grimwold conventions.");
        write(root.path(), "docs/README.md", "The Snarfle pipeline.");
        write(
            root.path(),
            "packages/foo/AGENTS.md",
            "Run Quuxify before commits.",
        );
        let (_db, store, ws) = setup(Some(root.path())).await;
        store
            .insert_note(&spec_note(&ws, "Ship the Flibberity feature."))
            .await
            .expect("spec");

        let cache = WorkspaceVocabularyCache::new();
        let terms = cache.vocabulary(&store, &ws).await;
        for expected in ["Zorblatt", "Grimwold", "Snarfle", "Quuxify", "Flibberity"] {
            assert!(terms.contains(&expected.to_string()), "missing {expected}");
        }
        assert!(terms.len() <= DEFAULT_MAX_TERMS);
    }

    #[tokio::test]
    async fn missing_files_and_spec_are_skipped_silently() {
        let root = tempfile::tempdir().expect("root");
        let (_db, store, ws) = setup(Some(root.path())).await;
        let cache = WorkspaceVocabularyCache::new();
        assert!(cache.vocabulary(&store, &ws).await.is_empty());

        // No worktree at all: still empty, never an error.
        let (_db2, store2, ws2) = setup(None).await;
        assert!(cache.vocabulary(&store2, &ws2).await.is_empty());

        // Unknown workspace: empty.
        let unknown = WorkspaceId::new();
        assert!(cache.vocabulary(&store, &unknown).await.is_empty());
    }

    #[tokio::test]
    async fn max_terms_zero_disables_derivation() {
        let root = tempfile::tempdir().expect("root");
        write(root.path(), "README.md", "Zorblatt.");
        let (_db, store, ws) = setup(Some(root.path())).await;
        store
            .set_setting(MAX_TERMS_SETTING_PATH, "0")
            .await
            .expect("set");
        let cache = WorkspaceVocabularyCache::new();
        assert!(cache.vocabulary(&store, &ws).await.is_empty());
        assert_eq!(cache.derivation_count(), 0, "no walk / extraction");
    }

    #[tokio::test]
    async fn honors_max_terms_cap() {
        let root = tempfile::tempdir().expect("root");
        write(
            root.path(),
            "README.md",
            "Zorblatt Quuxify Snarfle Grimwold Flibberity",
        );
        let (_db, store, ws) = setup(Some(root.path())).await;
        store
            .set_setting(MAX_TERMS_SETTING_PATH, "2")
            .await
            .expect("set");
        let cache = WorkspaceVocabularyCache::new();
        assert_eq!(cache.vocabulary(&store, &ws).await.len(), 2);
    }

    #[tokio::test]
    async fn repeated_calls_hit_the_cache_and_content_change_invalidates() {
        let root = tempfile::tempdir().expect("root");
        write(root.path(), "README.md", "Zorblatt tooling.");
        let (_db, store, ws) = setup(Some(root.path())).await;
        let cache = WorkspaceVocabularyCache::new();

        let first = cache.vocabulary(&store, &ws).await;
        assert!(first.contains(&"Zorblatt".to_string()));
        assert_eq!(cache.derivation_count(), 1);

        let second = cache.vocabulary(&store, &ws).await;
        assert_eq!(second, first, "unchanged sources return cached terms");
        assert_eq!(cache.derivation_count(), 1, "no re-extraction");

        write(root.path(), "README.md", "Zorblatt tooling.\nNow Quuxify.");
        let third = cache.vocabulary(&store, &ws).await;
        assert!(third.contains(&"Quuxify".to_string()));
        assert_eq!(cache.derivation_count(), 2, "content change re-derives");
    }

    #[tokio::test]
    async fn spec_note_change_invalidates() {
        let root = tempfile::tempdir().expect("root");
        let (_db, store, ws) = setup(Some(root.path())).await;
        store
            .insert_note(&spec_note(&ws, "Plan the Zorblatt rollout."))
            .await
            .expect("spec");
        let cache = WorkspaceVocabularyCache::new();
        assert!(cache
            .vocabulary(&store, &ws)
            .await
            .contains(&"Zorblatt".to_string()));

        store
            .update_note(&spec_note(&ws, "Plan the Quuxify rollout."))
            .await
            .expect("update spec");
        let terms = cache.vocabulary(&store, &ws).await;
        assert!(terms.contains(&"Quuxify".to_string()));
        assert!(!terms.contains(&"Zorblatt".to_string()));
        assert_eq!(cache.derivation_count(), 2);
    }

    #[tokio::test]
    async fn distinct_max_terms_values_are_distinct_cache_keys() {
        let root = tempfile::tempdir().expect("root");
        write(root.path(), "README.md", "Zorblatt Quuxify Snarfle");
        let (_db, store, ws) = setup(Some(root.path())).await;
        let cache = WorkspaceVocabularyCache::new();
        assert_eq!(
            cache.vocabulary_with_max_terms(&store, &ws, 3).await.len(),
            3
        );
        assert_eq!(
            cache.vocabulary_with_max_terms(&store, &ws, 1).await.len(),
            1
        );
        assert_eq!(cache.derivation_count(), 2, "maxTerms is part of the key");
    }
}
