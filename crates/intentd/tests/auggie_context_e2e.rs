//! Real-`auggie` context-engine integration test (§8, parity item #12; M10 CE-3).
//!
//! CI exercises only the mock/fake engine (see `intent-services` unit tests),
//! so this test drives the **real** [`AuggieContextEngine`] end-to-end over this
//! repository under Option A: auggie exposes no structured codebase-retrieval
//! CLI, so `retrieve()` degrades instantly and `search.codebase` is served by the
//! ripgrep/symbol path. It asserts the engine reports `Available` (binary
//! present, for `intentd doctor`) yet `search.codebase` returns ripgrep-backed
//! results **quickly** — never the old 30s interactive-mode hang. It is
//! **env-gated** so CI without `auggie`/credentials stays hermetic and green.
//!
//! ## Running locally
//!
//! ```sh
//! # from packages/intentd
//! INTENTD_AUGGIE_E2E=1 cargo test -p intentd --test auggie_context_e2e -- --nocapture
//! ```
//!
//! Requirements for the test to actually exercise the path (rather than skip):
//! - `INTENTD_AUGGIE_E2E=1` in the environment, **and**
//! - a real `auggie` binary discoverable via the CE-1 discovery (on `PATH` or
//!   `~/.augment/bin`), **and**
//! - that `auggie` is authenticated (`auggie login`).
//!
//! When the gate is unset, or `auggie` is absent/unauthenticated, the test
//! **skips cleanly** (prints a `SKIP …` note to stderr and returns) — it never
//! fails. This is the realistic "best-effort/local" form: CI does not provision
//! auggie credentials, so the gated path simply does not run there.

mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use intent_context::{discovery, AuggieContextEngine, ContextEngine, EngineAvailability};
use intent_core::{
    now_iso, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_services::Services;
use intent_store::Store;

#[tokio::test]
async fn auggie_context_engine_real_retrieve_e2e() {
    // Gate: opt-in only. Anything other than an explicit "1" skips.
    let gate = std::env::var("INTENTD_AUGGIE_E2E").unwrap_or_default();
    if gate.trim() != "1" {
        eprintln!(
            "SKIP auggie_context_e2e: INTENTD_AUGGIE_E2E != \"1\" \
             (set INTENTD_AUGGIE_E2E=1 with auggie on PATH + authed to run)"
        );
        return;
    }

    // Discovery (CE-1): no binary → skip, not fail.
    let Some(bin) = discovery::find_auggie() else {
        eprintln!(
            "SKIP auggie_context_e2e: no auggie binary discoverable on PATH / ~/.augment/bin"
        );
        return;
    };
    eprintln!("auggie_context_e2e: using auggie at {}", bin.display());

    // Availability is a non-error probe (§8.3); under Option A it still reports
    // binary presence for `intentd doctor`. Unavailable (incl. "needs login") →
    // skip cleanly.
    let engine = AuggieContextEngine::new();
    match engine.availability().await {
        EngineAvailability::Available { name, version } => {
            eprintln!("auggie_context_e2e: engine available: {name} {version:?}");
        }
        EngineAvailability::Unavailable { reason } => {
            eprintln!(
                "SKIP auggie_context_e2e: engine unavailable ({reason}) — \
                 likely not installed/authed"
            );
            return;
        }
    }

    // Search over this cargo workspace (`packages/intentd`), which is full of
    // Rust source, using an obvious query that must match real code here.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_path = manifest_dir
        .ancestors()
        .nth(2)
        .map_or_else(|| manifest_dir.clone(), Path::to_path_buf);

    // Wire a Services over a temp store with a workspace pointing at that
    // worktree, backed by the *real* auggie engine. Under Option A `retrieve()`
    // degrades instantly, so `search.codebase` must be served by ripgrep.
    let db_path =
        std::env::temp_dir().join(format!("intentd-auggie-e2e-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, &workspace_path))
        .await
        .expect("insert workspace");
    let ws_root = common::hermetic_workspaces_root();
    let svc = Services::new(store)
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_context_engine(std::sync::Arc::new(engine));

    eprintln!(
        "auggie_context_e2e: search.codebase over {} …",
        workspace_path.display()
    );
    let started = Instant::now();
    let r = svc
        .search_codebase(
            ws,
            "AuggieContextEngine".to_string(),
            Some("auggie-e2e".to_string()),
        )
        .await
        .expect("search.codebase must not error");
    let elapsed = started.elapsed();

    // The crux of M10 CE-3: no interactive-mode 30s hang. The old empty-args
    // spawn timed out at 30s; the degraded path returns near-instantly.
    assert!(
        elapsed < Duration::from_secs(20),
        "search.codebase took {elapsed:?}; expected a fast ripgrep-backed degrade, not a hang"
    );

    assert_eq!(r["requestId"], "auggie-e2e");
    let matches = r["matches"].as_array().expect("matches array");
    // Ripgrep-backed hits over this repo for an obviously-present symbol. We do
    // NOT assert engine-backed hits — Option A produces none.
    assert!(
        !matches.is_empty(),
        "expected ripgrep-backed matches for 'AuggieContextEngine' in {}",
        workspace_path.display()
    );
    assert!(
        matches
            .iter()
            .all(|m| m["file"].as_str().is_some_and(|f| !f.is_empty())),
        "every ripgrep match must carry a workspace-relative file: {matches:?}"
    );
    eprintln!(
        "auggie_context_e2e: PASS — {} ripgrep-backed hit(s) in {elapsed:?}; first = {:?}",
        matches.len(),
        matches.first()
    );

    let _ = std::fs::remove_file(&db_path);
}

/// Build a minimal active workspace whose worktree points at `worktree` so
/// `search.codebase` resolves its search root to that directory.
fn workspace(id: &WorkspaceId, worktree: &Path) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "auggie-e2e".to_string(),
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
        worktree_path: Some(worktree.to_string_lossy().to_string()),
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
