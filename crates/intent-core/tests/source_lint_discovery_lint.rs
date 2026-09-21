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
//!   itself, not inside a `run: |` block.
//!
//! The workspace root is located from `CARGO_MANIFEST_DIR`, as the other lints
//! do; the checks themselves take any root, which is how the fixture tests
//! below exercise them against throwaway workspaces.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const LINT_SUFFIX: &str = "_lint";
const RS_LINT_SUFFIX: &str = "_lint.rs";
const CI_WORKFLOW: &str = ".github/workflows/ci.yml";
const CI_INVOCATION: &str = "cargo test --workspace --test '*_lint'";
const SELF_FILE: &str = "crates/intent-core/tests/source_lint_discovery_lint.rs";

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

/// Every lint file under `root` that the `--test '*_lint'` glob would not
/// select, plus the CI wiring check, each rendered as one failure line.
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
        "\n\nA source lint exists that the `check` gate does not run. Every \
         `crates/*/tests/**/*_lint.rs` must be a cargo test target named `*_lint` so \
         `{CI_INVOCATION}` selects it.\n\n{}\n",
        failures.join("\n")
    );
}

#[cfg(test)]
mod fixture {
    use super::{undiscovered_lints, CI_INVOCATION, CI_WORKFLOW};
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
}
