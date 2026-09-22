//! Source-lint discovery lint.
//!
//! The source-scanning lints (`repo_slug_fold_lint`, `event_type_lint`,
//! `fixed_sleep_lint`, `raw_child_lint`, …) used to be hand-wired in three
//! places — one CI step each in `.github/workflows/ci.yml`, one monorepo
//! Makefile target each, and one AGENTS.md paragraph each — so every new lint
//! touched the same lines (rebase conflicts between intent-hq/intentd#1926 and
//! #1928) and a lint that missed its CI step simply never ran on PRs. The
//! `check` job now selects lints by convention: every
//! `crates/*/tests/*_lint.rs` integration test is a source lint and
//! `cargo test --workspace --test '*_lint'` runs all of them. This test proves
//! the convention is complete, failing when:
//!
//! - a file matching `crates/*/tests/**/*_lint.rs` is not the `src_path` of a
//!   cargo test target whose name ends in `_lint` (per `cargo metadata
//!   --no-deps`, no build): a crate with `autotests = false`, a lint placed in
//!   a `tests/<subdir>/` cargo does not auto-discover, or a `[[test]]` renamed
//!   away from the suffix would otherwise be silently skipped by the glob;
//! - the `check` job in `.github/workflows/ci.yml` has no non-comment `run:`
//!   line carrying the literal glob invocation, so the wiring cannot quietly
//!   revert to hand-listing, be commented out, or drift into another job. This
//!   is a bounded textual check (no YAML parser): the job block is the lines
//!   between the two-space-indented `check:` key and the next key at that
//!   indent or shallower, and the invocation must sit on the `run:` line
//!   itself, not inside a `run: |` block;
//! - a `crates/*/tests/**/*_lint.rs` file defines its own `fn lex(`,
//!   `fn markers_by_line(`, `fn blank_cfg_test_items(`,
//!   `fn cfg_test_item_ranges(`, or `fn split_statements(` (`pub` optional,
//!   whitespace-tolerant, matched on comment- and literal-blanked text). Those
//!   are the shared scaffolding in `intentd_test_support::source_lint`; the
//!   lints used to carry private copies, and two #2073 fixes (`102c347a`
//!   statement line accounting, `f2c685af` `cfg(test)` bracket nesting) had
//!   to be re-applied copy by copy. Opt out with a standalone
//!   `// source-lint-scaffolding: allow — <reason>` on the line above the
//!   definition; a marker without a reason is itself a failure. The shared
//!   module lives under `src/`, so the rule never sees it.
//!
//! The workspace root is located from `CARGO_MANIFEST_DIR`, as the other lints
//! do; the checks themselves take any root, which is how the fixture tests
//! below exercise them against throwaway workspaces.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use intentd_test_support::source_lint::{lex, markers_by_line, Marker};

const LINT_SUFFIX: &str = "_lint";
const RS_LINT_SUFFIX: &str = "_lint.rs";
const CI_WORKFLOW: &str = ".github/workflows/ci.yml";
const CI_INVOCATION: &str = "cargo test --workspace --test '*_lint'";
const SELF_FILE: &str = "crates/intent-core/tests/source_lint_discovery_lint.rs";
const SHARED_MODULE: &str = "intentd_test_support::source_lint";
const SCAFFOLDING_TAG: &str = "source-lint-scaffolding";
/// Shared scaffolding functions a lint must import rather than define.
const SCAFFOLDING_FNS: [&str; 5] = [
    "lex",
    "markers_by_line",
    "blank_cfg_test_items",
    "cfg_test_item_ranges",
    "split_statements",
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize workspace root")
}

fn collect_lint_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_lint_sources(&path, out);
        } else if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().ends_with(RS_LINT_SUFFIX))
        {
            out.push(path);
        }
    }
}

/// Every `crates/*/tests/**/*_lint.rs` under `root`, sorted, as paths
/// relative to `root`.
fn lint_files(root: &Path) -> Vec<PathBuf> {
    let crates_dir = root.join("crates");
    let crates = fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()));
    let mut files = Vec::new();
    for entry in crates.flatten() {
        let tests = entry.path().join("tests");
        if tests.is_dir() {
            collect_lint_sources(&tests, &mut files);
        }
    }
    let mut rel: Vec<PathBuf> = files
        .into_iter()
        .map(|p| p.strip_prefix(root).expect("under root").to_path_buf())
        .collect();
    rel.sort();
    rel
}

/// Canonical `src_path` of every cargo test target under `root` whose name
/// ends in `_lint`, read from `cargo metadata --no-deps` (manifests only, no
/// build, no dependency resolution).
fn lint_target_sources(root: &Path) -> BTreeSet<PathBuf> {
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
    let targets = packages.flat_map(|p| p["targets"].as_array().into_iter().flatten());
    targets
        .filter(|t| {
            t["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|k| k == "test"))
        })
        .filter(|t| t["name"].as_str().is_some_and(|n| n.ends_with(LINT_SUFFIX)))
        .filter_map(|t| t["src_path"].as_str())
        .map(|src| fs::canonicalize(src).unwrap_or_else(|_| PathBuf::from(src)))
        .collect()
}

/// Whether the `check` job in the workflow text has a non-comment `run:` line
/// invoking the lint glob.
fn check_job_runs_glob(ci: &str) -> bool {
    let mut in_check = false;
    for line in ci.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = line.len() - trimmed.len();
        if indent <= 2 {
            in_check = indent == 2 && trimmed == "check:";
            continue;
        }
        if in_check
            && trimmed
                .strip_prefix("run:")
                .is_some_and(|rest| rest.contains(CI_INVOCATION))
        {
            return true;
        }
    }
    false
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// `Some(name)` when `line` (one line of comment- and literal-blanked source)
/// defines a function named after one of [`SCAFFOLDING_FNS`]: optional `pub`
/// (with or without a `(…)` restriction), `fn`, the name, then `(`, any
/// whitespace between them.
fn scaffolding_definition(line: &str) -> Option<&'static str> {
    let mut rest = line.trim_start();
    if let Some(after_pub) = rest.strip_prefix("pub") {
        let after_pub = after_pub.trim_start();
        rest = match after_pub.strip_prefix('(') {
            Some(restriction) => restriction.split_once(')')?.1.trim_start(),
            None => after_pub,
        };
    }
    let after_fn = rest.strip_prefix("fn")?;
    if !after_fn.starts_with(char::is_whitespace) {
        return None;
    }
    let after_fn = after_fn.trim_start();
    SCAFFOLDING_FNS.into_iter().find(|name| {
        after_fn
            .strip_prefix(name)
            .is_some_and(|tail| tail.trim_start().starts_with('('))
    })
}

/// Every private copy of a shared scaffolding function defined in a
/// `*_lint.rs` file under `root`, each rendered as one failure line; a
/// `// source-lint-scaffolding: allow` marker on the line above with a
/// reason suppresses the hit, one without a reason is reported instead.
fn scaffolding_copies(root: &Path) -> Vec<String> {
    let mut failures = Vec::new();
    for rel in lint_files(root) {
        let path = root.join(&rel);
        let src =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let lexed = lex(&src);
        let markers = markers_by_line(&src, &lexed.line_comments, SCAFFOLDING_TAG);
        for (idx, line) in lexed.blanked.lines().enumerate() {
            let Some(name) = scaffolding_definition(line) else {
                continue;
            };
            let line_no = idx + 1;
            let marker_above = markers.get(line_no - 1).copied().unwrap_or(Marker::Absent);
            let file = display_rel(&rel);
            match marker_above {
                Marker::WithReason => {}
                Marker::Absent => failures.push(format!(
                    "{file}:{line_no} — private copy of source_lint::{name}; import \
                     {SHARED_MODULE} instead"
                )),
                Marker::Malformed => failures.push(format!(
                    "{file}:{line_no} — private copy of source_lint::{name} under a \
                     `// {SCAFFOLDING_TAG}: allow` marker with no reason; write \
                     `// {SCAFFOLDING_TAG}: allow — <reason>` or import {SHARED_MODULE}"
                )),
            }
        }
    }
    failures
}

/// Every lint file under `root` that the `--test '*_lint'` glob would not
/// select, plus the CI wiring check and the scaffolding-copy check, each
/// rendered as one failure line.
fn undiscovered_lints(root: &Path) -> Vec<String> {
    let targets = lint_target_sources(root);
    let mut failures: Vec<String> = lint_files(root)
        .into_iter()
        .filter(|rel| {
            let abs = root.join(rel);
            !targets.contains(&fs::canonicalize(&abs).unwrap_or(abs))
        })
        .map(|rel| {
            format!(
                "{}: not a `*_lint` cargo test target — `cargo test --workspace --test '*_lint'` \
                 never runs it (lint files must sit directly under `crates/<crate>/tests/` in \
                 a crate that auto-discovers tests, with no `[[test]]` renaming them)",
                display_rel(&rel)
            )
        })
        .collect();
    let ci = fs::read_to_string(root.join(CI_WORKFLOW)).unwrap_or_default();
    if !check_job_runs_glob(&ci) {
        failures.push(format!(
            "{CI_WORKFLOW}: no non-comment `run:` line in the `check` job runs \
             `{CI_INVOCATION}` — the check job must select source lints by the `*_lint` \
             glob, not by hand-listing them"
        ));
    }
    failures.extend(scaffolding_copies(root));
    failures
}

#[test]
fn every_lint_file_is_a_lint_test_target() {
    let root = workspace_root();
    let files = lint_files(&root);
    assert!(
        files.iter().any(|rel| display_rel(rel) == SELF_FILE),
        "lint discovery is broken: {SELF_FILE} was not found under {}",
        root.display()
    );
    let failures = undiscovered_lints(&root);
    assert!(
        failures.is_empty(),
        "\n\nA source lint exists that the `check` gate does not run, or one carries a \
         private copy of the shared scaffolding. Every `crates/*/tests/**/*_lint.rs` must \
         be a cargo test target named `*_lint` so `{CI_INVOCATION}` selects it, and must \
         import `{SHARED_MODULE}` rather than redefine its functions.\n\n{}\n",
        failures.join("\n")
    );
}

#[cfg(test)]
mod fixture {
    use super::{scaffolding_definition, undiscovered_lints, CI_INVOCATION, CI_WORKFLOW};
    use std::fs;
    use std::path::Path;

    /// A throwaway workspace with one crate `a` holding `tests/x_lint.rs`
    /// (`manifest_extra` is appended to its `Cargo.toml`) and a ci.yml whose
    /// `check` job carries the glob step.
    fn fixture(dir: &Path, manifest_extra: &str) {
        fs::write(
            dir.join("Cargo.toml"),
            "[workspace]\nmembers = [\"crates/*\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        let krate = dir.join("crates/a");
        fs::create_dir_all(krate.join("src")).unwrap();
        fs::create_dir_all(krate.join("tests")).unwrap();
        let mut manifest =
            String::from("[package]\nname = \"a\"\nversion = \"0.0.0\"\nedition = \"2021\"\n");
        manifest.push_str(manifest_extra);
        fs::write(krate.join("Cargo.toml"), manifest).unwrap();
        fs::write(krate.join("src/lib.rs"), "").unwrap();
        fs::write(krate.join("tests/x_lint.rs"), "#[test]\nfn t() {}\n").unwrap();
        write_ci(
            dir,
            &format!(
                "jobs:\n  check:\n    steps:\n      - name: Source lints\n        run: {CI_INVOCATION}\n"
            ),
        );
    }

    fn write_ci(dir: &Path, content: &str) {
        let ci = dir.join(CI_WORKFLOW);
        fs::create_dir_all(ci.parent().unwrap()).unwrap();
        fs::write(ci, content).unwrap();
    }

    fn assert_single_failure_at(failures: &[String], prefix: &str) {
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].starts_with(prefix), "{failures:?}");
    }

    #[test]
    fn all_lint_files_discovered_passes() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        assert_eq!(undiscovered_lints(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn nested_lint_file_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        let nested = tmp.path().join("crates/a/tests/sub");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("y_lint.rs"), "#[test]\nfn t() {}\n").unwrap();
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/sub/y_lint.rs: ",
        );
    }

    #[test]
    fn autotests_false_crate_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "autotests = false\n");
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/x_lint.rs: ",
        );
    }

    #[test]
    fn test_target_renamed_away_from_the_suffix_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(
            tmp.path(),
            "[[test]]\nname = \"x\"\npath = \"tests/x_lint.rs\"\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/x_lint.rs: ",
        );
    }

    #[test]
    fn ci_without_the_glob_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_ci(
            tmp.path(),
            "jobs:\n  check:\n    steps:\n      - name: Fixed-sleep lint\n        run: cargo test -p a --test x_lint\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            ".github/workflows/ci.yml: ",
        );
    }

    #[test]
    fn ci_with_the_glob_commented_out_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_ci(
            tmp.path(),
            &format!(
                "jobs:\n  check:\n    steps:\n      # - name: Source lints\n      #   run: {CI_INVOCATION}\n      - run: cargo fmt --check\n"
            ),
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            ".github/workflows/ci.yml: ",
        );
    }

    #[test]
    fn ci_with_the_glob_only_in_another_job_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_ci(
            tmp.path(),
            &format!(
                "jobs:\n  check:\n    steps:\n      - run: cargo fmt --check\n  build:\n    steps:\n      - name: Source lints\n        run: {CI_INVOCATION}\n"
            ),
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            ".github/workflows/ci.yml: ",
        );
    }

    fn write_lint(dir: &Path, body: &str) {
        fs::write(dir.join("crates/a/tests/x_lint.rs"), body).unwrap();
    }

    #[test]
    fn private_copy_of_a_scaffolding_fn_is_reported_at_its_line() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "use std::fs;\n\nfn lex(src: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        let failures = undiscovered_lints(tmp.path());
        assert_single_failure_at(
            &failures,
            "crates/a/tests/x_lint.rs:3 — private copy of source_lint::lex; import \
             intentd_test_support::source_lint instead",
        );
    }

    #[test]
    fn pub_copy_with_odd_spacing_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "  pub(crate)  fn   split_statements  (text: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/x_lint.rs:1 — private copy of source_lint::split_statements; ",
        );
    }

    #[test]
    fn reasoned_scaffolding_marker_above_the_copy_passes() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "// source-lint-scaffolding: allow — fixture exercising a deliberately different lexer\n\
             fn lex(src: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_eq!(undiscovered_lints(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn bare_scaffolding_marker_above_the_copy_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "// source-lint-scaffolding: allow\nfn lex(src: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        let failures = undiscovered_lints(tmp.path());
        assert_single_failure_at(
            &failures,
            "crates/a/tests/x_lint.rs:2 — private copy of source_lint::lex under a \
             `// source-lint-scaffolding: allow` marker with no reason; ",
        );
    }

    #[test]
    fn scaffolding_names_in_comments_strings_or_calls_are_not_hits() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "// fn lex(src) used to live here\n\
             const DOC: &str = \"fn split_statements(text)\";\n\
             fn run() { let _ = lex(DOC); }\n\
             fn lexer(src: &str) -> usize { src.len() }\n\
             fn lex_all() {}\n\n#[test]\nfn t() { run(); lexer(\"\"); lex_all(); }\n",
        );
        assert_eq!(undiscovered_lints(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn scaffolding_definition_matches_each_shared_name() {
        for name in super::SCAFFOLDING_FNS {
            assert_eq!(
                scaffolding_definition(&format!("fn {name}(x: &str) {{")),
                Some(name)
            );
            assert_eq!(
                scaffolding_definition(&format!("pub fn {name}(x: &str) {{")),
                Some(name)
            );
        }
        assert_eq!(scaffolding_definition("fn lexer(x: &str) {"), None);
        assert_eq!(scaffolding_definition("fnlex(x: &str) {"), None);
        assert_eq!(scaffolding_definition("pub_fn lex(x: &str) {"), None);
    }
}
