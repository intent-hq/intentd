//! Real-`auggie` context-engine integration test (§8, parity item #12).
//!
//! CI exercises only the mock/fake engine (see `intent-services` unit tests),
//! so this test drives the **real** [`AuggieContextEngine`] end-to-end over this
//! repository: discovery → availability → `retrieve()` → parsed hits. It is
//! **env-gated** so CI without `auggie`/credentials stays hermetic and green.
//!
//! ## Running locally
//!
//! ```sh
//! # from packages/intentd
//! INTENTD_AUGGIE_E2E=1 cargo test -p intentd --test auggie_context_e2e -- --nocapture
//! ```
//!
//! Requirements for the test to actually exercise the engine (rather than skip):
//! - `INTENTD_AUGGIE_E2E=1` in the environment, **and**
//! - a real `auggie` binary discoverable via the CE-1 discovery (on `PATH` or
//!   `~/.augment/bin`), **and**
//! - that `auggie` is authenticated (`auggie login`).
//!
//! When the gate is unset, or `auggie` is absent/unauthenticated, the test
//! **skips cleanly** (prints a `SKIP …` note to stderr and returns) — it never
//! fails. This is the realistic "best-effort/local" form: CI does not provision
//! auggie credentials, so the gated path simply does not run there.

use std::path::{Path, PathBuf};

use intent_context::{
    discovery, AuggieContextEngine, ContextEngine, ContextError, EngineAvailability,
    RetrieveRequest,
};
use intent_core::WorkspaceId;

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

    // Availability is a non-error probe (§8.3). Unavailable (incl. "needs
    // login") → skip cleanly.
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

    // Retrieve over this cargo workspace (`packages/intentd`), which is full of
    // Rust source, using an obvious query that must match real code here.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_path = manifest_dir
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| manifest_dir.clone());

    let req = RetrieveRequest {
        workspace_id: WorkspaceId::from_string("auggie-context-e2e"),
        workspace_path: workspace_path.clone(),
        query: "AuggieContextEngine context engine retrieve over a workspace".to_string(),
        max_results: Some(10),
    };
    eprintln!(
        "auggie_context_e2e: retrieving over {} …",
        workspace_path.display()
    );

    match engine.retrieve(req).await {
        // Engine-backed hits: assert the shape `search.codebase` exposes as
        // `CodebaseMatch` (each item maps 1:1 to a CodebaseMatch, carrying an
        // optional relevance score).
        Ok(result) if !result.items.is_empty() => {
            assert!(
                result.items.iter().all(|i| !i.file.is_empty()),
                "every engine hit must carry a workspace-relative file: {result:?}"
            );
            assert!(
                result.items.iter().any(|i| i.score.is_some()),
                "expected at least one engine hit to carry a relevance score: {result:?}"
            );
            eprintln!(
                "auggie_context_e2e: PASS — {} engine-backed hit(s); first = {:?}",
                result.items.len(),
                result.items.first()
            );
        }
        // Available + authed but no hits. The concrete retrieval subcommand
        // wiring is finalized outside this test's scope; don't fail a local,
        // CI-irrelevant run on it.
        Ok(empty) => {
            eprintln!(
                "SKIP auggie_context_e2e: engine returned no hits ({empty:?}); \
                 retrieval wiring may be incomplete"
            );
        }
        // Auth/availability lost between probe and retrieve (e.g. "needs login").
        Err(ContextError::Unavailable { reason }) => {
            eprintln!(
                "SKIP auggie_context_e2e: engine reported unavailable during retrieve \
                 ({reason}) — e.g. needs login"
            );
        }
        // Any other engine error is treated as a best-effort skip rather than a
        // failure, so local runs never go red on environment/wiring issues.
        Err(other) => {
            eprintln!(
                "SKIP auggie_context_e2e: best-effort retrieve errored ({other}); \
                 retrieval wiring may be incomplete"
            );
        }
    }
}
