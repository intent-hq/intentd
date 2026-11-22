//! Source lint: who may mint the Postgres opener's `OrchestratorOnly` token.
//!
//! Rust cannot limit a public constructor to one downstream crate, so the
//! compile-fail cases (`tests/compile_fail.rs`) prove only that the token
//! cannot be forged, defaulted or converted from `DaemonMode`. This lint pins
//! the two constructors that remain, in every Rust file of the repository
//! (`target/`, `vendor/` and `.git/` aside):
//!
//! - `mint_for_intentd_serve` is named only by its definition and once by
//!   each binary that runs in orchestrator mode ([`MINT_CALLERS`]: intentd's
//!   store choice, `crates/intentd/src/store_backend.rs`, and the
//!   orchestrator binary, `crates/intentd-orchestrator/src/main.rs`),
//!   outside their `#[cfg(test)]` items and inside a private `fn`, so no
//!   other module can reach the mint through a public wrapper.
//! - In any file that names `OrchestratorOnly`, the identifier `for_tests`
//!   appears only in test code: a crate's `tests/` directory, a `tests.rs`
//!   file, a `#[cfg(test)]` item, or the `intentd-test-support` crate. Matching
//!   the bare identifier covers `Self::for_tests`, `<OrchestratorOnly>::` and
//!   spacing; a `use ... as` alias of the type in a file that never names
//!   `OrchestratorOnly` is outside the contract.
//!
//! Comments and string literals are blanked first.

use std::path::{Component, Path, PathBuf};

use intentd_test_support::source_lint::{blank_cfg_test_items, lex, rust_files, workspace_root};

const MINT: &str = "mint_for_intentd_serve";
const MINT_DEFINITION: &str = "crates/intent-store/src/orchestrator_only.rs";
/// The binaries that may mint the token, each exactly once.
const MINT_CALLERS: &[&str] = &[
    "crates/intentd/src/store_backend.rs",
    "crates/intentd-orchestrator/src/main.rs",
];
const TOKEN: &str = "OrchestratorOnly";
const FOR_TESTS: &str = "for_tests";
const THIS_FILE: &str = "crates/intent-store/tests/orchestrator_only_lint.rs";
/// Top-level directories that hold no first-party Rust.
const SKIPPED_TOP_LEVEL: &[&str] = &["target", "vendor", ".git", "node_modules"];

/// Byte offsets of `needle` in `text` that are whole identifiers.
fn word_hits(text: &str, needle: &str) -> Vec<usize> {
    let is_ident = |c: char| c.is_alphanumeric() || c == '_';
    text.match_indices(needle)
        .map(|(at, _)| at)
        .filter(|&at| {
            let before = text[..at].chars().next_back();
            let after = text[at + needle.len()..].chars().next();
            !before.is_some_and(is_ident) && !after.is_some_and(is_ident)
        })
        .collect()
}

fn line_of(text: &str, offset: usize) -> usize {
    text[..offset].matches('\n').count() + 1
}

/// Test code by path: `crates/<crate>/tests/**`, any `tests.rs`, or the
/// `intentd-test-support` crate. A `src/**/tests/` directory is not exempt.
fn is_test_path(rel: &Path) -> bool {
    let parts: Vec<_> = rel
        .components()
        .filter_map(|c| match c {
            Component::Normal(name) => name.to_str(),
            _ => None,
        })
        .collect();
    matches!(parts.as_slice(), ["crates", _, "tests", ..])
        || matches!(parts.as_slice(), ["crates", "intentd-test-support", ..])
        || rel.file_name().is_some_and(|name| name == "tests.rs")
}

/// Whether the innermost `fn` declared before `at` is private (no `pub`).
fn enclosing_fn_is_private(code: &str, at: usize) -> bool {
    let before = &code[..at];
    let Some(fn_at) = word_hits(before, "fn").into_iter().next_back() else {
        return false;
    };
    let line_start = before[..fn_at].rfind('\n').map_or(0, |i| i + 1);
    !before[line_start..fn_at]
        .split_whitespace()
        .any(|word| word.starts_with("pub"))
}

/// Every first-party Rust file under `root`.
fn repository_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(root).expect("read the repository root") {
        let path = entry.expect("dir entry").path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if SKIPPED_TOP_LEVEL.contains(&name) {
            continue;
        }
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
    files.sort();
    files
}

/// Violations in one file, plus the count of allowed mint calls in it.
fn check_file(rel: &Path, src: &str) -> (Vec<String>, usize) {
    let rel_str = rel.to_string_lossy().replace('\\', "/");
    let code = lex(src).blanked;
    let non_test = blank_cfg_test_items(&code);
    let mut violations = Vec::new();
    let mut mint_calls = 0;

    let non_test_mints = word_hits(&non_test, MINT);
    for at in word_hits(&code, MINT) {
        let line = line_of(&code, at);
        if rel_str == MINT_DEFINITION && code[..at].trim_end().ends_with("fn") {
            continue;
        }
        if MINT_CALLERS.contains(&rel_str.as_str()) && non_test_mints.contains(&at) {
            if enclosing_fn_is_private(&code, at) {
                mint_calls += 1;
            } else {
                violations.push(format!(
                    "{rel_str}:{line}: `{MINT}` must be called from a private fn, so no public \
                     wrapper turns any mode into a token"
                ));
            }
            continue;
        }
        violations.push(format!(
            "{rel_str}:{line}: `{MINT}` may be called only from {} (non-test code)",
            MINT_CALLERS.join(" or ")
        ));
    }

    if !is_test_path(rel) && !word_hits(&code, TOKEN).is_empty() {
        for at in word_hits(&non_test, FOR_TESTS) {
            // Its definition, gated on `cfg(test)` or `test-support`.
            if rel_str == MINT_DEFINITION && code[..at].trim_end().ends_with("fn") {
                continue;
            }
            violations.push(format!(
                "{rel_str}:{}: `{TOKEN}::{FOR_TESTS}` may be used only in test code",
                line_of(&code, at)
            ));
        }
    }
    (violations, mint_calls)
}

#[test]
fn orchestrator_only_constructors_stay_where_they_belong() {
    let root = workspace_root();
    let mut violations = Vec::new();
    let mut mint_calls = std::collections::BTreeMap::new();
    for path in repository_rust_files(&root) {
        let rel = path.strip_prefix(&root).expect("under the workspace root");
        if rel == Path::new(THIS_FILE) {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read source");
        let (found, calls) = check_file(rel, &src);
        violations.extend(found);
        if calls > 0 {
            mint_calls.insert(rel.to_string_lossy().replace('\\', "/"), calls);
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
    for caller in MINT_CALLERS {
        assert_eq!(
            mint_calls.get(*caller).copied().unwrap_or(0),
            1,
            "{caller} must mint the token exactly once, in orchestrator mode only"
        );
    }
}

#[test]
fn word_hits_match_whole_identifiers_only() {
    assert_eq!(word_hits("a mint_for_intentd_serve()", MINT), vec![2]);
    assert!(word_hits("xmint_for_intentd_serve()", MINT).is_empty());
    assert!(word_hits("mint_for_intentd_serve_2()", MINT).is_empty());
    assert_eq!(word_hits("Self::for_tests()", FOR_TESTS), vec![6]);
}

/// The lint's own negative cases: each source must be flagged.
#[test]
fn the_lint_flags_escapes() {
    let caller = Path::new(MINT_CALLERS[0]);
    let public_wrapper = "pub fn choose() -> T { OrchestratorOnly::mint_for_intentd_serve() }\n";
    assert_eq!(check_file(caller, public_wrapper).0.len(), 1);
    let crate_wrapper =
        "pub(crate) fn choose() -> T { OrchestratorOnly::mint_for_intentd_serve() }\n";
    assert_eq!(check_file(caller, crate_wrapper).0.len(), 1);
    let private = "fn choose() -> T { OrchestratorOnly::mint_for_intentd_serve() }\n";
    assert_eq!(check_file(caller, private), (Vec::new(), 1));

    let elsewhere = Path::new("crates/intent-services/src/lib.rs");
    assert_eq!(check_file(elsewhere, private).0.len(), 1);
    for src in [
        "impl OrchestratorOnly { fn x() -> Self { Self::for_tests() } }\n",
        "fn x() { <OrchestratorOnly>::for_tests(); }\n",
        "fn x() { OrchestratorOnly:: for_tests(); }\n",
    ] {
        assert_eq!(check_file(elsewhere, src).0.len(), 1, "{src}");
    }
    let definition = Path::new(MINT_DEFINITION);
    let defined = "impl OrchestratorOnly {\n    #[cfg(any(test, feature = \"test-support\"))]\n    pub fn for_tests() -> Self { todo!() }\n    fn x() { Self::for_tests(); }\n}\n";
    assert_eq!(
        check_file(definition, defined).0.len(),
        1,
        "only the definition is exempt, not a use in the same file"
    );
    let src_tests_dir = Path::new("crates/intent-store/src/postgres/tests/helper.rs");
    assert_eq!(
        check_file(src_tests_dir, "fn x() { OrchestratorOnly::for_tests(); }\n")
            .0
            .len(),
        1
    );
    let cfg_test = "#[cfg(test)]\nmod tests { fn x() { OrchestratorOnly::for_tests(); } }\n";
    assert!(check_file(elsewhere, cfg_test).0.is_empty());
    let test_dir = Path::new("crates/intent-store/tests/postgres_backend.rs");
    assert!(
        check_file(test_dir, "fn x() { OrchestratorOnly::for_tests(); }\n")
            .0
            .is_empty()
    );
}
