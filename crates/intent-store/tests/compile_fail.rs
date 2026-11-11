//! Compile-fail checks for intent-store's guarded APIs.
//!
//! Every trybuild case lives in this one test binary. trybuild shares one
//! project directory per crate; its in-process lock serializes the `#[test]`s
//! below, but its cross-process lock is only best effort and is broken under
//! load, so separate binaries overwrite each other's cases (CI run 43048039407).

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
