//! Source lint: who may mint the Postgres opener's `OrchestratorOnly` token.
//!
//! Rust cannot limit a public constructor to one downstream crate, so the
//! compile-fail cases (`tests/compile_fail.rs`) prove only that the token
//! cannot be forged, defaulted or converted from `DaemonMode`. This lint pins
//! the two constructors that remain:
//!
//! - `mint_for_intentd_serve` is named only by its definition and by the
//!   intentd binary's store choice (`crates/intentd/src/store_backend.rs`),
//!   outside its `#[cfg(test)]` items, once.
//! - `OrchestratorOnly::for_tests` appears only in test code: a `tests/`
//!   directory, a `tests.rs` file, a `#[cfg(test)]` item, or the
//!   `intentd-test-support` crate.
//!
//! Comments and string literals are blanked first. A `use` alias of
//! `for_tests` is outside the contract; the mint is matched by name, so no
//! alias of it escapes.

use std::path::{Component, Path};

use intentd_test_support::source_lint::{blank_cfg_test_items, lex, rust_files, workspace_root};

const MINT: &str = "mint_for_intentd_serve";
const MINT_DEFINITION: &str = "crates/intent-store/src/orchestrator_only.rs";
const MINT_CALLER: &str = "crates/intentd/src/store_backend.rs";
const FOR_TESTS: &str = "OrchestratorOnly::for_tests";

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

fn is_test_path(rel: &Path) -> bool {
    rel.components().any(|c| match c {
        Component::Normal(name) => name == "tests" || name == "intentd-test-support",
        _ => false,
    }) || rel.file_name().is_some_and(|name| name == "tests.rs")
}

#[test]
fn orchestrator_only_constructors_stay_where_they_belong() {
    let root = workspace_root();
    let mut violations = Vec::new();
    let mut mint_calls = 0;
    for path in rust_files(&root.join("crates")) {
        let rel = path.strip_prefix(&root).expect("under the workspace root");
        let rel_str = rel.to_string_lossy().replace('\\', "/");
        if rel_str == "crates/intent-store/tests/orchestrator_only_lint.rs" {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read source");
        let code = lex(&src).blanked;
        let non_test = blank_cfg_test_items(&code);

        for at in word_hits(&code, MINT) {
            let line = line_of(&code, at);
            let in_non_test = word_hits(&non_test, MINT).contains(&at);
            let is_definition = rel_str == MINT_DEFINITION && code[..at].trim_end().ends_with("fn");
            if is_definition {
                continue;
            }
            if rel_str == MINT_CALLER && in_non_test {
                mint_calls += 1;
                continue;
            }
            violations.push(format!(
                "{rel_str}:{line}: `{MINT}` may be called only from {MINT_CALLER} (non-test code)"
            ));
        }

        if is_test_path(rel) {
            continue;
        }
        let non_test_hits = word_hits(&non_test, FOR_TESTS);
        for at in non_test_hits {
            violations.push(format!(
                "{rel_str}:{}: `{FOR_TESTS}` may be used only in test code",
                line_of(&code, at)
            ));
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
    assert_eq!(
        mint_calls, 1,
        "{MINT_CALLER} must mint the token exactly once, from the resolved daemon mode"
    );
}

#[test]
fn word_hits_match_whole_identifiers_only() {
    assert_eq!(word_hits("a mint_for_intentd_serve()", MINT), vec![2]);
    assert!(word_hits("xmint_for_intentd_serve()", MINT).is_empty());
    assert!(word_hits("mint_for_intentd_serve_2()", MINT).is_empty());
    assert_eq!(
        word_hits("OrchestratorOnly::for_tests()", FOR_TESTS),
        vec![0]
    );
}
