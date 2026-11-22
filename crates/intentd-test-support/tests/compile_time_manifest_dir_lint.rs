//! Compile-time manifest-dir lint.
//!
//! `env!("CARGO_MANIFEST_DIR")` bakes the building checkout's absolute path
//! into the compiled crate. In library code that path outlives the checkout:
//! the rlib is linked into other crates' tests, and a build reused from
//! another checkout carries the other checkout's path. A Cargo target
//! directory shared between checkouts does that (its freshness check compares
//! source mtimes, not paths, so a checkout made before another checkout's
//! build reuses that build's rlibs), and so would a compiler cache that keys
//! without the path. `source_lint::workspace_root` used to resolve the
//! workspace that way, and `agent_hidden_field_egress_lint` on
//! intent-hq/intentd#3648 read a deleted `/tmp/…/v3648/mono` checkout.
//!
//! This lint fails on `env!` / `option_env!` of `CARGO_MANIFEST_DIR` in
//! library code: `crates/*/src/**` text that is not test-build-only, as
//! `source_lint::test_code_files_with(root, TestCfgs::TestBuildOnly)` decides
//! (`#[cfg(test)]` / `#[cfg(all(…, test, …))]` items, conventional test files
//! and the out-of-line modules they declare). A `cfg(any(test, feature =
//! "test-hooks"))` item is library code here: it builds into the rlib other
//! crates' tests link. Test-build code is out of scope: the path a test
//! binary bakes comes from the same build as the code that reads it.
//!
//! Read the path at run time instead — `source_lint::workspace_root()`, or
//! `std::env::var_os("CARGO_MANIFEST_DIR")`, which `cargo test` and nextest
//! set to the running test's package — or move the code under `#[cfg(test)]`.
//! There is no opt-out.

use intentd_test_support::source_lint::{
    test_code_files_with, tokenize, workspace_root, TestCfgs, TestCodeText,
};

const VAR: &str = "CARGO_MANIFEST_DIR";
const MACROS: &[&str] = &["env", "option_env"];
const OPENERS: &[&str] = &["(", "[", "{"];

/// 1-based lines of `src` reading `CARGO_MANIFEST_DIR` at compile time
/// outside test-build-only code (`whole` marks a file that is test code
/// throughout).
fn hits(src: &str, whole: bool) -> Vec<usize> {
    let text = TestCodeText::with_cfgs(src, whole, TestCfgs::TestBuildOnly);
    let tokens = tokenize(&text.chars);
    text.lexed
        .literals
        .iter()
        .filter(|literal| literal.cooked() == VAR && !text.in_test(literal.offset))
        .filter(|literal| {
            let before = tokens.partition_point(|token| token.start < literal.offset);
            before >= 3
                && OPENERS.iter().any(|open| tokens[before - 1].is(open))
                && tokens[before - 2].is("!")
                && MACROS.iter().any(|name| tokens[before - 3].is(name))
        })
        .map(|literal| literal.line)
        .collect()
}

#[test]
fn library_code_never_bakes_the_manifest_dir() {
    let root = workspace_root();
    let mut failures = Vec::new();
    let files = test_code_files_with(&root, TestCfgs::TestBuildOnly);
    assert!(
        files
            .iter()
            .any(|file| file.rel.ends_with("/src/source_lint.rs")),
        "the scan found no crates/*/src files under {}",
        root.display()
    );
    for file in files.iter().filter(|file| !file.whole) {
        for line in hits(&file.src, false) {
            failures.push(format!("{}:{line}", file.rel));
        }
    }
    assert!(
        failures.is_empty(),
        "compile-time `env!(\"{VAR}\")` in library code bakes this checkout's path into the \
         rlib, which a build reused by another checkout then reads (intent-hq/intentd#3648); \
         resolve it at run time with `source_lint::workspace_root()` or \
         `std::env::var_os(\"{VAR}\")`, or move the code under `#[cfg(test)]`:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn detector_flags_compile_time_reads_in_library_code() {
    let src = r#"
fn root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
fn joined() -> &'static str {
    concat!(std::env!("CARGO_MANIFEST_DIR"), "/x")
}
fn maybe() -> Option<&'static str> {
    option_env! { "CARGO_MANIFEST_DIR" }
}
#[cfg(any(test, feature = "test-hooks"))]
pub fn hook_fixture() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
"#;
    assert_eq!(hits(src, false), vec![3, 6, 9, 13]);
}

#[test]
fn detector_ignores_runtime_reads_test_code_and_text() {
    let src = r#"
// env!("CARGO_MANIFEST_DIR") in a comment
fn runtime() -> Option<std::ffi::OsString> {
    std::env::var_os("CARGO_MANIFEST_DIR")
}
fn message() -> &'static str {
    "env!(\"CARGO_MANIFEST_DIR\")"
}
fn other() -> &'static str {
    env!("CARGO_PKG_NAME")
}
#[cfg(test)]
fn fixture() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
#[cfg(all(test, unix))]
mod unix_tests {
    const ROOT: &str = env!("CARGO_MANIFEST_DIR");
}
"#;
    assert_eq!(hits(src, false), Vec::<usize>::new());
    assert_eq!(
        hits("const ROOT: &str = env!(\"CARGO_MANIFEST_DIR\");\n", true),
        Vec::<usize>::new(),
        "a whole test file is exempt"
    );
}
