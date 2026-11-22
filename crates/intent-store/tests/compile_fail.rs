//! Compile-fail checks for intent-store's guarded APIs.
//!
//! Every trybuild case lives in this one test binary. trybuild shares one
//! project directory per crate; its in-process lock serializes the `#[test]`s
//! below under `cargo test`, but its cross-process lock is only best effort and
//! is broken under load, so separate binaries overwrite each other's cases (CI
//! run 43048039407). nextest runs each `#[test]` in its own process, so
//! `.config/nextest.toml` puts this binary in the one-thread `trybuild-serial`
//! group.

/// The owned pool must not export the `SQLx` configuration/initialization bypasses.
#[test]
fn pool_escape_apis_are_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/store_pool_*.rs");
}

/// Writing a note's text requires that note's `NoteWriteLock`, and the only
/// way to get one is `Store::lock_note_content` (intent-hq/intent#7184).
#[test]
fn note_text_writes_without_the_lock_are_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/note_write_lock_*.rs");
}

/// A metadata-only `AgentSessionSummary` can never reach a full-row store
/// writer: writing one back would clear the stored `system_prompt`.
#[test]
fn summary_write_back_is_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/agent_session_summary_*.rs");
}

/// The Postgres opener's `OrchestratorOnly` token cannot be forged, built
/// from the public `DaemonMode` enum, or defaulted: only the intentd binary
/// mints it (`tests/orchestrator_only_lint.rs` pins the call sites).
#[test]
fn orchestrator_only_token_cannot_be_forged() {
    trybuild::TestCases::new().compile_fail("tests/ui/orchestrator_only_*.rs");
}

/// nextest runs every `#[test]` in its own process, so trybuild's in-process
/// lock never applies there; only a one-thread nextest group keeps these cases
/// from rewriting the shared project together (merge-queue run 44979683017).
#[test]
fn nextest_runs_this_binary_one_test_at_a_time() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.config/nextest.toml");
    let config: toml::Table = std::fs::read_to_string(&path)
        .expect("read .config/nextest.toml")
        .parse()
        .expect("parse .config/nextest.toml");
    let overrides = config["profile"]["default"]["overrides"]
        .as_array()
        .expect("profile.default.overrides");
    let group = overrides
        .iter()
        .find(|o| o.get("filter").and_then(|f| f.as_str()) == Some("binary_id(=intent-store::compile_fail)"))
        .and_then(|o| o.get("test-group"))
        .and_then(|g| g.as_str())
        .expect("an override with filter binary_id(=intent-store::compile_fail) and a test-group");
    assert_eq!(
        config["test-groups"][group]
            .get("max-threads")
            .and_then(|t| t.as_integer()),
        Some(1),
        "test group {group} must run one test at a time"
    );
}

#[test]
fn trybuild_cases_stay_in_this_binary() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut others = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read tests dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().is_some_and(|ext| ext == "rs")
            && path
                .file_name()
                .is_some_and(|name| name != "compile_fail.rs")
            && std::fs::read_to_string(&path)
                .expect("read test file")
                .contains("trybuild::")
        {
            others.push(path);
        }
    }
    assert!(
        others.is_empty(),
        "move these trybuild cases into tests/compile_fail.rs: {others:?}"
    );
}
