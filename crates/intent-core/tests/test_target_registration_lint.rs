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
//!   keeps the lists merge-friendly and makes a duplicate entry visible;
//! - a suite module (`tests/<m>.rs` or anything under `tests/<m>/`) changes
//!   process-global state in-process: `env::set_var(` / `env::remove_var(` /
//!   `set_current_dir(` / `set_child_subreaper(`. nextest runs each test in its
//!   own process, but plain `cargo test` runs a suite's tests as threads of one
//!   process, where that state leaks into every other file's tests (the
//!   reason such files are standalone `[[test]]` targets). A call that only
//!   runs in a separately exec'd child opts out with
//!   `// process-global: allow — <reason>` on the line above;
//! - a `--test <name>` in `.github/`, `scripts/` or the `Makefile` names a
//!   suite module instead of a test target: cargo rejects it with "no test
//!   target named", and jobs that only run in the merge queue would not show
//!   that until a queued run fails (intentd#2635).
//!
//! Files under `tests/<dir>/` are not checked: they are helpers or module
//! directories, reached through `mod` / `#[path]` from a root that the
//! compiler already checks.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SUITE_PREFIX: &str = "suite_";
const GLOBAL_STATE_CALLS: [&str; 4] = [
    "env::set_var(",
    "env::remove_var(",
    "set_current_dir(",
    "set_child_subreaper(",
];
const GLOBAL_STATE_MARKER: &str = "// process-global: allow — ";

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

/// `(line, call)` of every unmarked process-global call in `text`; comment
/// lines are skipped, and a marker needs a non-empty reason.
fn global_state_calls(text: &str) -> Vec<(usize, &'static str)> {
    let lines: Vec<&str> = text.lines().collect();
    let mut hits = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        let marked = i > 0
            && lines[i - 1]
                .trim_start()
                .strip_prefix(GLOBAL_STATE_MARKER)
                .is_some_and(|reason| !reason.trim().is_empty());
        if let Some(call) = GLOBAL_STATE_CALLS.iter().find(|c| line.contains(*c)) {
            if !marked {
                hits.push((i + 1, *call));
            }
        }
    }
    hits
}

/// `(line, name)` of every `--test <name>` in `text`, in any spelling a
/// command takes: `--test name`, `--test=name`, or an argv list's
/// `"--test", "name"`. `--tests` / `--test-threads` are not hits, and comment
/// lines (`#` / `//`) are skipped.
fn test_flag_targets(text: &str) -> Vec<(usize, String)> {
    let mut hits = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') || trimmed.starts_with("//") {
            continue;
        }
        let mut rest = line;
        while let Some(at) = rest.find("--test") {
            rest = &rest[at + "--test".len()..];
            if !rest.starts_with([' ', '\t', '=', '"', '\'']) {
                continue;
            }
            let name: String = rest
                .trim_start_matches([' ', '\t', '=', '"', '\'', ','])
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                .collect();
            if !name.is_empty() {
                hits.push((i + 1, name));
            }
        }
    }
    hits
}

/// CI and script files whose cargo commands name test targets.
fn command_files(root: &Path) -> Vec<PathBuf> {
    fn walk(path: &Path, out: &mut Vec<PathBuf>) {
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "__pycache__") {
                return;
            }
            for entry in fs::read_dir(path).into_iter().flatten().flatten() {
                walk(&entry.path(), out);
            }
        } else if path.is_file() {
            out.push(path.to_path_buf());
        }
    }
    let mut out = Vec::new();
    walk(&root.join(".github"), &mut out);
    walk(&root.join("scripts"), &mut out);
    out.push(root.join("Makefile"));
    out.sort();
    out
}

fn collect_rs(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_dir() {
        for entry in fs::read_dir(path).into_iter().flatten().flatten() {
            collect_rs(&entry.path(), out);
        }
    } else if path.extension().is_some_and(|e| e == "rs") {
        out.push(path.to_path_buf());
    }
}

fn violations(root: &Path) -> Vec<String> {
    let targets = test_target_sources(root);
    let mut out = Vec::new();
    // Suite module name -> its root's repo-relative path.
    let mut suite_modules: BTreeMap<String, String> = BTreeMap::new();
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
            for module in declared_modules(text)
                .into_iter()
                .filter(|m| *m != "common")
            {
                let mut sources = Vec::new();
                collect_rs(&tests.join(format!("{module}.rs")), &mut sources);
                collect_rs(&tests.join(module), &mut sources);
                sources.sort();
                for source in sources {
                    let body = fs::read_to_string(&source).unwrap_or_default();
                    for (line, call) in global_state_calls(&body) {
                        out.push(format!(
                            "{}:{line}: `{call}` changes process-global state in a module of {}; \
                             make the file a standalone [[test]] target (plain `cargo test` runs \
                             a suite's tests as threads of one process), or mark a call that only \
                             runs in a separately exec'd child with `{GLOBAL_STATE_MARKER}<reason>`",
                            source.strip_prefix(root).unwrap().display(),
                            rel(stem)
                        ));
                    }
                }
            }
            suite_modules.extend(
                declared_modules(text)
                    .into_iter()
                    .filter(|m| *m != "common")
                    .map(|m| (m.to_owned(), rel(stem))),
            );
        }
    }
    for file in command_files(root) {
        let Ok(body) = fs::read_to_string(&file) else {
            continue;
        };
        for (line, name) in test_flag_targets(&body) {
            if let Some(suite) = suite_modules.get(&name) {
                let suite_target = Path::new(suite).file_stem().unwrap().to_string_lossy();
                out.push(format!(
                    "{}:{line}: `--test {name}` names a module of {suite}, not a test target \
                     (cargo fails with \"no test target named `{name}`\"); use \
                     `--test {suite_target}` with a `{name}::` filter",
                    file.strip_prefix(root).unwrap().display()
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

#[test]
fn process_global_calls_need_a_reasoned_marker() {
    let text = "fn a() {\n    std::env::set_var(\"X\", \"1\");\n}\n\
                // std::env::remove_var(\"X\") in a comment\n\
                // process-global: allow — child only\n\
                nix::sys::prctl::set_child_subreaper(true)?;\n\
                // process-global: allow — \n\
                std::env::set_current_dir(\"/\");\n";
    assert_eq!(
        global_state_calls(text),
        [(2, "env::set_var("), (8, "set_current_dir(")]
    );
}

#[test]
fn test_flags_are_found_in_every_spelling() {
    let text = "cargo test -p intentd --test e2e_a 'x'\n\
                run([\"cargo\", \"test\", \"--test\", \"e2e_b\", \"lifecycle_\"])\n\
                cargo test --test=e2e-c --tests --test-threads=2\n\
                # cargo test --test e2e_commented\n\
                \"--test\" , \"suite_d\" --test $VAR\n";
    let found = test_flag_targets(text);
    let names: Vec<(usize, &str)> = found.iter().map(|(l, n)| (*l, n.as_str())).collect();
    assert_eq!(
        names,
        [(1, "e2e_a"), (2, "e2e_b"), (3, "e2e-c"), (5, "suite_d")]
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
    write(
        "crates/k/tests/one/env.rs",
        "fn f() { std::env::set_var(\"K\", \"1\"); }\n",
    );
    write("crates/k/tests/two.rs", "");
    write("crates/k/tests/common/mod.rs", "");
    write(
        "scripts/run.py",
        "run([\"cargo\", \"test\", \"--test\", \"one\"])\n\
         run([\"cargo\", \"test\", \"--test\", \"suite_k\", \"one::\"])\n",
    );
    write(
        ".github/workflows/ci.yml",
        "run: cargo test --test suite_k\n",
    );
    let found = violations(&root.canonicalize().unwrap());
    assert_eq!(found.len(), 3, "{found:?}");
    assert!(
        found[0].starts_with("crates/k/tests/two.rs is neither"),
        "{found:?}"
    );
    assert!(
        found[1].starts_with("crates/k/tests/one/env.rs:1: `env::set_var(`"),
        "{found:?}"
    );
    assert!(
        found[2].starts_with(
            "scripts/run.py:1: `--test one` names a module of crates/k/tests/suite_k.rs"
        ),
        "{found:?}"
    );
}
