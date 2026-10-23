//! Test-target registration lint.
//!
//! Every integration-test executable links its whole crate graph, and for
//! `intentd` that is the whole daemon (180-360 MiB per executable), so
//! `crates/intentd` sets `autotests = false` and groups its test files as
//! modules of a few `tests/suite_*.rs` targets instead of building one
//! executable per file. Without auto-discovery a new `tests/<file>.rs` that
//! nobody registers is never compiled or run, and nothing else would notice.
//! This lint fails when:
//!
//! - a top-level `crates/*/tests/*.rs` file is neither the `src_path` of a
//!   cargo test target (per `cargo metadata --no-deps`, no build) nor declared
//!   by a bare `mod <file>;` line in one of its crate's test target roots;
//! - a `tests/suite_*.rs` root declares its modules out of sorted order, which
//!   keeps the lists merge-friendly and makes a duplicate entry visible.
//!
//! Files under `tests/<dir>/` are not checked: they are helpers or module
//! directories, reached through `mod` / `#[path]` from a root that the
//! compiler already checks.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SUITE_PREFIX: &str = "suite_";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize workspace root")
}

/// Canonical `src_path` of every cargo test target of the workspace at `root`.
fn test_target_sources(root: &Path) -> BTreeSet<PathBuf> {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .arg("--manifest-path")
        .arg(root.join("Cargo.toml"))
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata failed under {}:\n{}",
        root.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata emits JSON");
    let packages = metadata["packages"].as_array().into_iter().flatten();
    packages
        .flat_map(|p| p["targets"].as_array().into_iter().flatten())
        .filter(|t| {
            t["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|k| k == "test"))
        })
        .filter_map(|t| t["src_path"].as_str())
        .map(|src| fs::canonicalize(src).unwrap_or_else(|_| PathBuf::from(src)))
        .collect()
}

/// One crate's top-level `tests/*.rs` files: (file stem, canonical path, text).
fn top_level_test_files(tests: &Path) -> Vec<(String, PathBuf, String)> {
    let Ok(entries) = fs::read_dir(tests) else {
        return Vec::new();
    };
    let mut files: Vec<_> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "rs"))
        .map(|p| {
            let stem = p.file_stem().unwrap().to_string_lossy().into_owned();
            let text =
                fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
            (stem, fs::canonicalize(&p).unwrap(), text)
        })
        .collect();
    files.sort();
    files
}

/// Module names a root declares with a bare `mod <name>;` line.
fn declared_modules(root_text: &str) -> Vec<&str> {
    root_text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("mod ")?.strip_suffix(';'))
        .filter(|name| name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        .collect()
}

/// Stems of `files` that are neither a target root nor a module of one.
fn unregistered(files: &[(String, PathBuf, String)], targets: &BTreeSet<PathBuf>) -> Vec<String> {
    let declared: BTreeSet<&str> = files
        .iter()
        .filter(|(_, path, _)| targets.contains(path))
        .flat_map(|(_, _, text)| declared_modules(text))
        .collect();
    files
        .iter()
        .filter(|(stem, path, _)| !targets.contains(path) && !declared.contains(stem.as_str()))
        .map(|(stem, _, _)| stem.clone())
        .collect()
}

/// The first out-of-order (or repeated) module of a suite root, if any.
fn first_unsorted(root_text: &str) -> Option<(String, String)> {
    let modules: Vec<&str> = declared_modules(root_text)
        .into_iter()
        .filter(|m| *m != "common")
        .collect();
    modules
        .windows(2)
        .find(|w| w[0] >= w[1])
        .map(|w| (w[0].to_owned(), w[1].to_owned()))
}

fn violations(root: &Path) -> Vec<String> {
    let targets = test_target_sources(root);
    let mut out = Vec::new();
    let mut crates: Vec<PathBuf> = fs::read_dir(root.join("crates"))
        .expect("read crates/")
        .flatten()
        .map(|e| e.path())
        .collect();
    crates.sort();
    for krate in crates {
        let tests = krate.join("tests");
        let files = top_level_test_files(&tests);
        let rel = |stem: &str| {
            tests
                .join(format!("{stem}.rs"))
                .strip_prefix(root)
                .unwrap()
                .display()
                .to_string()
        };
        for stem in unregistered(&files, &targets) {
            out.push(format!(
                "{} is neither a cargo test target nor a `mod {stem};` of one, so it is \
                 never built: add `mod {stem};` to the matching tests/suite_*.rs root \
                 (and `use crate::common;` instead of `mod common;`), or a [[test]] entry",
                rel(&stem)
            ));
        }
        for (stem, _, text) in files.iter().filter(|(s, _, _)| s.starts_with(SUITE_PREFIX)) {
            if let Some((before, after)) = first_unsorted(text) {
                out.push(format!(
                    "{}: `mod {after};` must sort after `mod {before};` (keep the suite list sorted, no duplicates)",
                    rel(stem)
                ));
            }
        }
    }
    out
}

#[test]
fn every_test_file_is_built_by_a_target() {
    let found = violations(&workspace_root());
    assert!(found.is_empty(), "{}", found.join("\n"));
}

#[test]
fn unregistered_flags_only_files_no_root_reaches() {
    let target = PathBuf::from("/t/suite_a.rs");
    let standalone = PathBuf::from("/t/solo.rs");
    let files = vec![
        (
            "member".to_owned(),
            PathBuf::from("/t/member.rs"),
            String::new(),
        ),
        (
            "orphan".to_owned(),
            PathBuf::from("/t/orphan.rs"),
            String::new(),
        ),
        (
            "solo".to_owned(),
            standalone.clone(),
            "#[path = \"x/orphan.rs\"]\nmod helper;".to_owned(),
        ),
        (
            "suite_a".to_owned(),
            target.clone(),
            "//! mod orphan; in a comment\nmod common;\nmod member;\n".to_owned(),
        ),
    ];
    let targets = BTreeSet::from([target, standalone]);
    assert_eq!(unregistered(&files, &targets), ["orphan"]);
}

#[test]
fn suite_lists_must_be_sorted_without_duplicates() {
    assert_eq!(first_unsorted("mod common;\nmod a;\nmod b;\n"), None);
    assert_eq!(
        first_unsorted("mod common;\nmod b;\nmod a;\n"),
        Some(("b".to_owned(), "a".to_owned()))
    );
    assert_eq!(
        first_unsorted("mod a;\nmod a;\n"),
        Some(("a".to_owned(), "a".to_owned()))
    );
}

/// End to end through `cargo metadata`: a crate with `autotests = false`
/// whose suite forgets one file.
#[test]
fn autotests_false_crate_with_a_forgotten_file_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let write = |rel: &str, text: &str| {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    };
    write(
        "Cargo.toml",
        "[workspace]\nmembers = [\"crates/k\"]\nresolver = \"2\"\n",
    );
    write(
        "crates/k/Cargo.toml",
        "[package]\nname = \"k\"\nversion = \"0.0.0\"\nedition = \"2021\"\nautotests = false\n\n\
         [[test]]\nname = \"suite_k\"\n",
    );
    write("crates/k/src/lib.rs", "");
    write("crates/k/tests/suite_k.rs", "mod common;\n\nmod one;\n");
    write("crates/k/tests/one.rs", "");
    write("crates/k/tests/two.rs", "");
    write("crates/k/tests/common/mod.rs", "");
    let found = violations(&root.canonicalize().unwrap());
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(
        found[0].starts_with("crates/k/tests/two.rs is neither"),
        "{found:?}"
    );
}
