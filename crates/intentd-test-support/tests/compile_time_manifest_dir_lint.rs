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
//! crates' tests link.
//!
//! It also fails on them anywhere in a source lint
//! (`crates/*/tests/*_lint.rs`): a reused test binary is reused whole, so a
//! lint that bakes its checkout scans the building checkout even when the
//! library it links is fixed (`elapsed_assert_lint` read a deleted checkout
//! in the two-checkout reproduction). Other test code is out of scope: it
//! reads fixtures of the build it belongs to, not the tree under review.
//! An `env!` inside `include_str!` / `include_bytes!` arguments is exempt:
//! those embed the file's bytes, never its path.
//!
//! Read the path at run time instead — `source_lint::workspace_root()` or
//! `source_lint::package_dir()`, from the `CARGO_MANIFEST_DIR` that
//! `cargo test` and nextest set for the running test — or, in library code,
//! move the code under `#[cfg(test)]`.
//! There is no opt-out.

use intentd_test_support::source_lint::{
    test_code_files_with, tokenize, workspace_root, TestCfgs, TestCodeText, Token,
};

const VAR: &str = "CARGO_MANIFEST_DIR";
const MACROS: &[&str] = &["env", "option_env"];
const OPENERS: &[&str] = &["(", "[", "{"];
const CLOSERS: &[&str] = &[")", "]", "}"];
/// Macros that read the file at compile time and embed its bytes, so a path
/// built from `env!` inside them never reaches the compiled crate.
const INCLUDE_MACROS: &[&str] = &["include_str", "include_bytes"];

/// Whether the token at `at` sits inside the arguments of an
/// `include_str!` / `include_bytes!` call, at any nesting depth.
fn inside_include(tokens: &[Token], at: usize) -> bool {
    let mut depth = 0usize;
    for j in (0..at).rev() {
        if CLOSERS.iter().any(|close| tokens[j].is(close)) {
            depth += 1;
        } else if OPENERS.iter().any(|open| tokens[j].is(open)) {
            if depth > 0 {
                depth -= 1;
                continue;
            }
            if j >= 2
                && tokens[j - 1].is("!")
                && INCLUDE_MACROS.iter().any(|name| tokens[j - 2].is(name))
            {
                return true;
            }
        }
    }
    false
}

/// Which code of a file the rule covers.
#[derive(Clone, Copy)]
enum Scope {
    /// Library code: everything but test-build-only code (`whole` marks a
    /// file that is test code throughout).
    Library { whole: bool },
    /// A source lint: the whole file.
    Lint,
}

/// 1-based lines of `src` reading `CARGO_MANIFEST_DIR` at compile time in
/// `scope`, outside `include_str!` / `include_bytes!` arguments.
fn hits(src: &str, scope: Scope) -> Vec<usize> {
    let whole = matches!(scope, Scope::Library { whole: true });
    let text = TestCodeText::with_cfgs(src, whole, TestCfgs::TestBuildOnly);
    let tokens = tokenize(&text.chars);
    text.lexed
        .literals
        .iter()
        .filter(|literal| {
            literal.cooked() == VAR
                && (matches!(scope, Scope::Lint) || !text.in_test(literal.offset))
        })
        .filter(|literal| {
            let before = tokens.partition_point(|token| token.start < literal.offset);
            before >= 3
                && OPENERS.iter().any(|open| tokens[before - 1].is(open))
                && tokens[before - 2].is("!")
                && MACROS.iter().any(|name| tokens[before - 3].is(name))
                && !inside_include(&tokens, before - 3)
        })
        .map(|literal| literal.line)
        .collect()
}

/// Whether `rel` is a source lint: `crates/<crate>/tests/<name>_lint.rs`.
fn is_source_lint(rel: &str) -> bool {
    let parts: Vec<&str> = rel.split('/').collect();
    matches!(parts.as_slice(), ["crates", _, "tests", name] if name.ends_with("_lint.rs"))
}

#[test]
fn library_code_and_source_lints_never_bake_the_manifest_dir() {
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
    let lints = files
        .iter()
        .filter(|file| is_source_lint(&file.rel))
        .count();
    assert!(
        lints > 1,
        "the scan found {lints} source lints under {}",
        root.display()
    );
    for file in &files {
        let scope = if is_source_lint(&file.rel) {
            Scope::Lint
        } else if file.whole {
            continue;
        } else {
            Scope::Library { whole: false }
        };
        for line in hits(&file.src, scope) {
            failures.push(format!("{}:{line}", file.rel));
        }
    }
    assert!(
        failures.is_empty(),
        "compile-time `env!(\"{VAR}\")` in library code or a source lint bakes this checkout's \
         path into the build, which another checkout reusing it then reads \
         (intent-hq/intentd#3648); resolve it at run time with `source_lint::workspace_root()` / \
         `source_lint::package_dir()`, or (library code only) move it under `#[cfg(test)]` (inside \
         `include_str!` / `include_bytes!` it is fine: only the file's bytes are embedded):\n  {}",
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
static AFTER_INCLUDE: (&str, &str) = (include_str!("x.txt"), env!("CARGO_MANIFEST_DIR"));
"#;
    assert_eq!(
        hits(src, Scope::Library { whole: false }),
        vec![3, 6, 9, 13, 15]
    );
}

#[test]
fn source_lints_are_scanned_whole() {
    let src = "#[cfg(test)]\nconst ROOT: &str = env!(\"CARGO_MANIFEST_DIR\");\n";
    assert_eq!(hits(src, Scope::Lint), vec![2]);
    assert_eq!(
        hits(src, Scope::Library { whole: false }),
        Vec::<usize>::new()
    );
    assert!(is_source_lint(
        "crates/intent-core/tests/fixed_sleep_lint.rs"
    ));
    assert!(!is_source_lint(
        "crates/intent-core/tests/common/fixed_sleep_lint.rs"
    ));
    assert!(!is_source_lint("crates/intent-core/tests/e2e.rs"));
    assert!(!is_source_lint("crates/intent-core/src/x_lint.rs"));
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
static EMBEDDED: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/data.txt"));
static BYTES: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/data.bin"));
#[cfg(test)]
fn fixture() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
#[cfg(all(test, unix))]
mod unix_tests {
    const ROOT: &str = env!("CARGO_MANIFEST_DIR");
}
"#;
    assert_eq!(
        hits(src, Scope::Library { whole: false }),
        Vec::<usize>::new()
    );
    assert_eq!(
        hits(
            "const ROOT: &str = env!(\"CARGO_MANIFEST_DIR\");\n",
            Scope::Library { whole: true }
        ),
        Vec::<usize>::new(),
        "a whole test file is exempt"
    );
}
