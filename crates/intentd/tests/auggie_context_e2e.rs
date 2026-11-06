//! Real-`auggie` context-engine integration test (§8, parity item #12; M10 CE-3).
//!
//! CI exercises only the mock/fake engine (see `intent-services` unit tests),
//! so this test drives the **real** [`AuggieContextEngine`] end-to-end over this
//! repository under Option A: auggie exposes no structured codebase-retrieval
//! CLI, so `retrieve()` degrades instantly and `search.codebase` is served by the
//! ripgrep/symbol path. It asserts the engine reports `Available` (binary
//! present, for `intentd doctor`) yet `search.codebase` returns ripgrep-backed
//! results **without spawning auggie** — the old interactive-mode spawn hung
//! until its 30s timeout. The real binary runs behind a recording shim, so the
//! test sees every invocation. It is **env-gated** so CI without
//! `auggie`/credentials stays hermetic and green.
//!
//! ## Running locally
//!
//! ```sh
//! # from packages/intentd
//! INTENTD_AUGGIE_E2E=1 cargo test -p intentd --test suite_e2e auggie_context_e2e:: -- --nocapture
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

// The recording shim in front of the real binary is a POSIX shell script.
#![cfg(unix)]

use crate::common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use intent_context::{discovery, AuggieContextEngine, ContextEngine, EngineAvailability};
use intent_core::{now_iso, Workspace, WorkspaceApi, WorkspaceId};
use intent_services::Services;
use intent_store::Store;

#[intent_test_macros::daemon_test]
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

    // Drive the real binary through a shim that logs each invocation's
    // arguments, so the test can tell which spawns the engine made.
    let shim_dir = common::test_tempdir("intentd-auggie-shim-");
    let (shim, invocations) = recording_shim(shim_dir.path(), &bin);

    // Availability is a non-error probe (§8.3); under Option A it still reports
    // binary presence for `intentd doctor`. Unavailable (incl. "needs login") →
    // skip cleanly.
    let engine = AuggieContextEngine::with_path(shim);
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
    let db_dir = common::test_tempdir("intentd-auggie-e2e-");
    let db_path = db_dir.path().join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, &workspace_path))
        .await
        .expect("insert workspace");
    let ws_root = common::hermetic_workspaces_root();
    let svc = Services::new_isolated(store)
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_context_engine(std::sync::Arc::new(engine));

    eprintln!(
        "auggie_context_e2e: search.codebase over {} …",
        workspace_path.display()
    );
    let r = svc
        .search_codebase(
            ws,
            "AuggieContextEngine".to_string(),
            Some("auggie-e2e".to_string()),
        )
        .await
        .expect("search.codebase must not error");

    // The crux of M10 CE-3: search never runs auggie beyond its bounded
    // `--version` availability probe (made once above and again by the
    // service). The old empty-args interactive spawn hung until its 30s
    // timeout; it would show up here as a non-probe invocation.
    let log = std::fs::read_to_string(&invocations).expect("read shim invocation log");
    assert!(
        !log.is_empty() && log.lines().all(|line| line == "argv:--version"),
        "search.codebase must degrade to ripgrep without spawning auggie \
         beyond `--version` probes; shim saw:\n{log}"
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
        "auggie_context_e2e: PASS — {} ripgrep-backed hit(s); first = {:?}",
        matches.len(),
        matches.first()
    );
}

/// Write an `auggie` shim in `dir` that appends `argv:<args>` to a log, then
/// execs the real binary `bin`. Returns the shim path and the log path once
/// the shim is safe to exec.
fn recording_shim(dir: &Path, bin: &Path) -> (PathBuf, PathBuf) {
    let shim = dir.join("auggie");
    let log = dir.join("invocations.log");
    std::fs::write(&log, "").expect("create shim invocation log");
    let quote = |p: &Path| format!("'{}'", p.display().to_string().replace('\'', r"'\''"));
    // The real binary's dir leads PATH so a `#!/usr/bin/env node` launcher
    // finds the node install it came with.
    let bin_dir = bin.parent().unwrap_or_else(|| Path::new("/"));
    let script = format!(
        "#!/bin/sh\n[ \"$1\" = {READY_PROBE} ] && exit 0\n\
         printf 'argv:%s\\n' \"$*\" >> {log}\nPATH={bin_dir}:\"$PATH\" exec {bin} \"$@\"\n",
        log = quote(&log),
        bin_dir = quote(bin_dir),
        bin = quote(bin),
    );
    std::fs::write(&shim, script).expect("write auggie shim");
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755))
        .expect("chmod auggie shim");
    // A child forked while the shim was open for writing holds that fd until
    // its own exec, so exec of the fresh file can fail with ETXTBSY (and the
    // engine would report auggie unavailable, skipping the test). No new
    // copy of the closed fd can appear, so one clean exec settles it.
    for _ in 0..400 {
        match std::process::Command::new(&shim).arg(READY_PROBE).status() {
            Ok(status) => {
                assert!(status.success(), "auggie shim ready probe failed: {status}");
                return (shim, log);
            }
            Err(e) if e.kind() == std::io::ErrorKind::ExecutableFileBusy => {
                // timing-guard: retry backoff; the next exec attempt observes readiness.
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            Err(e) => panic!("exec auggie shim: {e}"),
        }
    }
    panic!("auggie shim kept failing with ETXTBSY");
}

/// Shim argument that exits before logging or running the real binary.
const READY_PROBE: &str = "--intentd-shim-ready";

/// Build a minimal active workspace whose worktree points at `worktree` so
/// `search.codebase` resolves its search root to that directory.
fn workspace(id: &WorkspaceId, worktree: &Path) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "auggie-e2e".to_string(),
        branch: "main".to_string(),
        created_at: ts.clone(),
        updated_at: ts,
        worktree_path: Some(worktree.to_string_lossy().to_string()),
        ..Default::default()
    }
}
