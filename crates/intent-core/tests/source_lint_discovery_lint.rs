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
//! - a `crates/*/tests/**/*_lint.rs` file defines its own copy of a shared
//!   scaffolding function — `fn lex(`, `fn markers_by_line(`,
//!   `fn blank_cfg_test_items(`, `fn cfg_test_item_ranges(`,
//!   `fn split_statements(`, `fn tokenize(`, `fn tokenize_str(`,
//!   `fn tokenize_from(`, `fn matching_close(`, or `fn skip_group(` (`pub`
//!   optional; generics allowed; any whitespace, line breaks included,
//!   between `fn`, the name, and `(` or `<`; matched on the whole comment- and literal-blanked text and
//!   reported at the line of the `fn` keyword). Those are the shared
//!   scaffolding in `intentd_test_support::source_lint`; the lints used to
//!   carry private copies, and two #2073 fixes (`102c347a` statement line
//!   accounting, `f2c685af` `cfg(test)` bracket nesting) had to be re-applied
//!   copy by copy;
//! - a `*_lint.rs` file defines its own token reader: a `fn tokens(`, or an
//!   `enum` / `struct` named exactly `Tok`, `Token`, or `TokenKind` (reported
//!   at the line of its keyword). Five lints once carried their own readers,
//!   and the nine intent-hq/intentd#2822 review fixes to one of them (`&&` /
//!   `||` splitting, closure `||`, bracket matching) never reached the
//!   others; rules now read tokens with `source_lint::tokenize`. This is a
//!   name check, not a proof: a reader under another name passes.
//! - `AGENTS.md` drifts from the tree (a separate test,
//!   `agents_md_lists_every_crate_and_lint`): a `*_lint.rs` file whose stem
//!   has no row in the source-lint table, a row whose first cell names no
//!   lint file (or repeats one), a crate directory (one with a `Cargo.toml`)
//!   under `crates/` with no `├── <crate>/` line in the fenced block under
//!   `## Project layout`, or a layout line naming no crate. Agents navigate by
//!   those lists; intentd#2853 found six crates and one lint row missing. The
//!   table is the rows after the `| Lint |` header up to the first line not
//!   starting with `|`; a missing table or layout block fails rather than
//!   passing vacuously.
//!
//! Opt out of the scaffolding or token-reader rule with a standalone
//! `// source-lint-scaffolding: allow — <reason>` on the line above the `fn`
//! / `enum` / `struct` keyword; a marker without a reason is itself a
//! failure. The shared module lives under `src/`, so the rules never see it.
//!
//! The workspace root is located from `CARGO_MANIFEST_DIR`, as the other lints
//! do; the checks themselves take any root, which is how the fixture tests
//! below exercise them against throwaway workspaces.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use intentd_test_support::source_lint::{lex, markers_by_line, tokenize_str, Marker, Token};

const LINT_SUFFIX: &str = "_lint";
const RS_LINT_SUFFIX: &str = "_lint.rs";
const CI_WORKFLOW: &str = ".github/workflows/ci.yml";
const CI_INVOCATION: &str = "cargo test --workspace --test '*_lint'";
const SELF_FILE: &str = "crates/intent-core/tests/source_lint_discovery_lint.rs";
const SHARED_MODULE: &str = "intentd_test_support::source_lint";
const SCAFFOLDING_TAG: &str = "source-lint-scaffolding";
/// Shared scaffolding functions a lint must import rather than define.
const SCAFFOLDING_FNS: [&str; 10] = [
    "lex",
    "markers_by_line",
    "blank_cfg_test_items",
    "cfg_test_item_ranges",
    "split_statements",
    "tokenize",
    "tokenize_str",
    "tokenize_from",
    "matching_close",
    "skip_group",
];
/// Function names that mark a private token reader even though no shared
/// function carries them (`fn tokens(` was the usual spelling).
const TOKEN_READER_FNS: [&str; 1] = ["tokens"];
/// Type names that mark a private token reader (`enum Tok`, `struct Token`).
const TOKEN_READER_TYPES: [&str; 3] = ["Tok", "Token", "TokenKind"];

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

/// `(line, name)` of every `fn <name>(` / `fn <name><…>(` definition in
/// `tokens` (of comment- and literal-blanked source), so any whitespace —
/// line breaks included — may sit between `fn`, the name, and `(` or the
/// generics, while `fnlex`, `pub_fn`, and the `fn(` of a function-pointer
/// type are not definitions. `line` is the 1-based line of the `fn` keyword.
fn fn_definitions(tokens: &[Token]) -> Vec<(usize, &str)> {
    tokens
        .windows(3)
        .filter(|w| w[0].is("fn") && w[1].is_ident() && (w[2].is("(") || w[2].is("<")))
        .map(|w| (w[0].line, w[1].text.as_str()))
        .collect()
}

/// `(line, name)` for every definition in `blanked` of a function named
/// after one of [`SCAFFOLDING_FNS`].
fn scaffolding_definitions(blanked: &str) -> Vec<(usize, &'static str)> {
    let tokens = tokenize_str(blanked);
    fn_definitions(&tokens)
        .into_iter()
        .filter_map(|(line, word)| {
            SCAFFOLDING_FNS
                .into_iter()
                .find(|name| *name == word)
                .map(|name| (line, name))
        })
        .collect()
}

/// `(line, what)` for every private token reader in `blanked`: a `fn` named
/// after one of [`TOKEN_READER_FNS`], or an `enum` / `struct` named exactly
/// one of [`TOKEN_READER_TYPES`] (`TokenBudget` is not). `what` is the
/// definition as written (`fn tokens`, `enum Tok`); `line` is the line of
/// its keyword.
fn token_reader_definitions(blanked: &str) -> Vec<(usize, String)> {
    let tokens = tokenize_str(blanked);
    let fns = fn_definitions(&tokens)
        .into_iter()
        .filter(|(_, word)| TOKEN_READER_FNS.contains(word))
        .map(|(line, word)| (line, format!("fn {word}")));
    let types = tokens
        .windows(2)
        .filter(|w| {
            (w[0].is("enum") || w[0].is("struct"))
                && TOKEN_READER_TYPES.contains(&w[1].text.as_str())
        })
        .map(|w| (w[0].line, format!("{} {}", w[0].text, w[1].text)));
    let mut out: Vec<_> = fns.chain(types).collect();
    out.sort();
    out
}

/// Every private copy of a shared scaffolding function defined in a
/// `*_lint.rs` file under `root`, each rendered as one failure line; a
/// `// source-lint-scaffolding: allow` marker on the line above the `fn`
/// keyword with a reason suppresses the hit, one without a reason is
/// reported instead.
fn scaffolding_copies(root: &Path) -> Vec<String> {
    let mut failures = Vec::new();
    for rel in lint_files(root) {
        let path = root.join(&rel);
        let src =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let lexed = lex(&src);
        let markers = markers_by_line(&src, &lexed.line_comments, SCAFFOLDING_TAG);
        let copies = scaffolding_definitions(&lexed.blanked)
            .into_iter()
            .map(|(line, name)| (line, format!("private copy of source_lint::{name}")));
        let readers = token_reader_definitions(&lexed.blanked)
            .into_iter()
            .map(|(line, what)| {
                (
                    line,
                    format!(
                        "private token reader (`{what}`); read tokens with source_lint::tokenize"
                    ),
                )
            });
        let mut found: Vec<_> = copies.chain(readers).collect();
        found.sort();
        for (line_no, what) in found {
            let marker_above = markers.get(line_no - 1).copied().unwrap_or(Marker::Absent);
            let file = display_rel(&rel);
            match marker_above {
                Marker::WithReason => {}
                Marker::Absent => failures.push(format!(
                    "{file}:{line_no} — {what}; import {SHARED_MODULE} instead"
                )),
                Marker::Malformed => failures.push(format!(
                    "{file}:{line_no} — {what} under a `// {SCAFFOLDING_TAG}: allow` marker \
                     with no reason; write `// {SCAFFOLDING_TAG}: allow — <reason>` or import \
                     {SHARED_MODULE}"
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

/// The first line of the `AGENTS.md` lint table (its header row).
const LINT_TABLE_HEADER: &str = "| Lint |";
/// The `AGENTS.md` heading whose first fenced block is the crate tree.
const LAYOUT_HEADING: &str = "## Project layout";

/// `(line, name)` for every row of the `AGENTS.md` source-lint table: the
/// rows that follow the `| Lint |` header up to the first line that does not
/// start with `|`, minus the `| --- |` separator. `name` is the first cell
/// with its backticks removed, so that cell must hold exactly one backticked
/// lint name (a list, a link or an empty cell reads as a stale row); `None`
/// when no table header exists. Rows match lint files by file stem, so two
/// crates' `*_lint.rs` files with the same name share one row.
fn lint_table_rows(agents: &str) -> Option<Vec<(usize, String)>> {
    let mut lines = agents.lines().enumerate();
    lines.find(|(_, l)| l.trim_start().starts_with(LINT_TABLE_HEADER))?;
    let rows = lines
        .take_while(|(_, l)| l.trim_start().starts_with('|'))
        .filter_map(|(i, l)| {
            let cell = l.trim_start().strip_prefix('|')?.split('|').next()?.trim();
            let separator =
                cell.contains('-') && cell.chars().all(|c| matches!(c, '-' | ':' | ' '));
            (!separator).then(|| (i + 1, cell.trim_matches('`').to_string()))
        })
        .collect();
    Some(rows)
}

/// `(line, crate)` for every top-level `├── <crate>/` / `└── <crate>/` entry
/// (the glyph at column 0, so a nested `│   └── tests/` subtree is not a
/// crate; the trailing `/` and backticks are optional) of the first fenced
/// block under `## Project layout` in `AGENTS.md`. The `crates/` root line
/// carries no tree glyph and is skipped. `None` when the heading is missing,
/// when the section ends (the next `#` heading) before a fenced block opens,
/// or when the block never closes.
fn layout_crates(agents: &str) -> Option<Vec<(usize, String)>> {
    let mut lines = agents.lines().enumerate();
    lines.find(|(_, l)| l.trim_end() == LAYOUT_HEADING)?;
    for (_, line) in lines.by_ref() {
        if line.trim_start().starts_with("```") {
            break;
        }
        if line.starts_with('#') {
            return None;
        }
    }
    let mut out = Vec::new();
    for (i, line) in lines {
        if line.trim_start().starts_with("```") {
            return Some(out);
        }
        let Some(entry) = line
            .strip_prefix("├── ")
            .or_else(|| line.strip_prefix("└── "))
        else {
            continue;
        };
        let name = entry.split_whitespace().next().unwrap_or("");
        let name = name.trim_matches('`');
        let name = name.strip_suffix('/').unwrap_or(name);
        if !name.is_empty() {
            out.push((i + 1, name.to_string()));
        }
    }
    None
}

/// Every crate directory under `root/crates` (one holding a `Cargo.toml`, so
/// a stray directory left by a branch switch is not a crate), sorted.
fn crate_dirs(root: &Path) -> BTreeSet<String> {
    let crates_dir = root.join("crates");
    fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()))
        .flatten()
        .filter(|e| e.path().join("Cargo.toml").is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

/// Drift between `root/AGENTS.md` and the tree: a `*_lint.rs` file with no
/// lint-table row, a row naming no lint file (or named twice), a crate
/// directory missing from the project-layout block, or a layout entry naming
/// no crate — each rendered as one failure line naming the entry.
fn agents_md_drift(root: &Path) -> Vec<String> {
    let agents = match fs::read_to_string(root.join("AGENTS.md")) {
        Ok(agents) => agents,
        Err(e) => return vec![format!("AGENTS.md: cannot read it: {e}")],
    };
    let mut failures = Vec::new();

    let lints: Vec<(String, PathBuf)> = lint_files(root)
        .into_iter()
        .filter_map(|rel| {
            let stem = rel.file_stem()?.to_string_lossy().into_owned();
            Some((stem, rel))
        })
        .collect();
    match lint_table_rows(&agents) {
        None => failures.push(format!(
            "AGENTS.md: no source-lint table (a `{LINT_TABLE_HEADER} …` header row) — the \
             table under \"Gates\" must list every `*_lint.rs`"
        )),
        Some(rows) => {
            let mut seen = BTreeSet::new();
            for (line, name) in &rows {
                if !seen.insert(name.as_str()) {
                    failures.push(format!(
                        "AGENTS.md:{line}: lint table row `{name}` is a duplicate — remove it"
                    ));
                } else if !lints.iter().any(|(stem, _)| stem == name) {
                    failures.push(format!(
                        "AGENTS.md:{line}: lint table row `{name}` names no \
                         `crates/*/tests/{name}.rs` — remove the stale row or fix its name"
                    ));
                }
            }
            for (stem, rel) in &lints {
                if !seen.contains(stem.as_str()) {
                    failures.push(format!(
                        "AGENTS.md: lint table has no row for `{stem}` ({}) — add one saying \
                         what it fails on and how to opt out",
                        display_rel(rel)
                    ));
                }
            }
        }
    }

    let crates = crate_dirs(root);
    match layout_crates(&agents) {
        None => failures.push(format!(
            "AGENTS.md: no fenced crate tree under `{LAYOUT_HEADING}` — the layout block \
             must list every crate under `crates/`"
        )),
        Some(entries) => {
            let mut seen = BTreeSet::new();
            for (line, name) in &entries {
                if !seen.insert(name.as_str()) {
                    failures.push(format!(
                        "AGENTS.md:{line}: project layout lists `{name}/` twice — remove one"
                    ));
                } else if !crates.contains(name) {
                    failures.push(format!(
                        "AGENTS.md:{line}: project layout lists `{name}/`, but \
                         `crates/{name}/Cargo.toml` does not exist — remove the stale line or \
                         fix its name"
                    ));
                }
            }
            for name in &crates {
                if !seen.contains(name.as_str()) {
                    failures.push(format!(
                        "AGENTS.md: project layout does not list crate `{name}/` \
                         (crates/{name}/Cargo.toml) — add a `├── {name}/  # <purpose>` line"
                    ));
                }
            }
        }
    }
    failures
}

#[test]
fn agents_md_lists_every_crate_and_lint() {
    let root = workspace_root();
    let failures = agents_md_drift(&root);
    assert!(
        failures.is_empty(),
        "\n\nAGENTS.md has drifted from the tree. Agents use its crate layout and source-lint \
         table to find code, so every crate under `crates/` needs a layout line and every \
         `crates/*/tests/*_lint.rs` needs a lint-table row — and neither list may name \
         something that no longer exists.\n\n{}\n",
        failures.join("\n")
    );
}

#[cfg(test)]
mod fixture {
    use super::{
        agents_md_drift, scaffolding_definitions, token_reader_definitions, undiscovered_lints,
        CI_INVOCATION, CI_WORKFLOW,
    };
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
    fn copy_split_between_fn_and_its_name_is_reported_at_the_fn_line() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "use std::fs;\n\nfn\nlex(src: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/x_lint.rs:3 — private copy of source_lint::lex; ",
        );
    }

    #[test]
    fn copy_split_between_its_name_and_the_paren_is_reported_at_the_fn_line() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "use std::fs;\n\npub fn lex\n    (src: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/x_lint.rs:3 — private copy of source_lint::lex; ",
        );
    }

    #[test]
    fn reasoned_marker_above_the_fn_line_of_a_split_copy_passes() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "// source-lint-scaffolding: allow — fixture exercising a deliberately different lexer\n\
             fn\nlex(src: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_eq!(undiscovered_lints(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn bare_marker_above_the_fn_line_of_a_split_copy_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "// source-lint-scaffolding: allow\nfn\nlex(src: &str) -> Vec<()> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
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
             /* fn\n   lex(src) used to live here too */\n\
             const DOC: &str = \"fn split_statements(text)\";\n\
             const SPLIT: &str = \"fn\n   lex(src)\";\n\
             fn run() { let _ = lex(DOC); }\n\
             fn lexer(src: &str) -> usize { src.len() }\n\
             fn lex_all() {}\n\n#[test]\nfn t() { run(); lexer(SPLIT); lex_all(); }\n",
        );
        assert_eq!(undiscovered_lints(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn scaffolding_definitions_match_each_shared_name() {
        for name in super::SCAFFOLDING_FNS {
            assert_eq!(
                scaffolding_definitions(&format!("fn {name}(x: &str) {{")),
                vec![(1, name)]
            );
            assert_eq!(
                scaffolding_definitions(&format!("\npub(crate) fn {name}(x: &str) {{")),
                vec![(2, name)]
            );
            assert_eq!(
                scaffolding_definitions(&format!("\n\nfn\n{name}\n(x: &str) {{")),
                vec![(3, name)]
            );
        }
        assert_eq!(scaffolding_definitions("fn lexer(x: &str) {"), vec![]);
        assert_eq!(scaffolding_definitions("fnlex(x: &str) {"), vec![]);
        assert_eq!(scaffolding_definitions("pub_fn lex(x: &str) {"), vec![]);
        assert_eq!(scaffolding_definitions("let f: fn(&str) = lex;"), vec![]);
        assert_eq!(
            scaffolding_definitions("fn matching_close<T>(x: T) {"),
            vec![(1, "matching_close")]
        );
        assert_eq!(
            scaffolding_definitions("fn tokenize<'a>(x: &'a str) {"),
            vec![(1, "tokenize")]
        );
    }

    #[test]
    fn private_token_reader_type_is_reported_at_its_keyword_line() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "use std::fs;\n\n#[derive(Debug)]\nenum Tok<'a> {\n    Word(&'a str),\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/x_lint.rs:4 — private token reader (`enum Tok`); read tokens with \
             source_lint::tokenize; import intentd_test_support::source_lint instead",
        );
    }

    #[test]
    fn private_tokens_fn_split_across_lines_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "struct\n    Token {\n    text: String,\n}\n\nfn\ntokens\n(text: &str) -> Vec<Token> {\n    vec![]\n}\n\n#[test]\nfn t() {}\n",
        );
        let failures = undiscovered_lints(tmp.path());
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(
            failures[0]
                .starts_with("crates/a/tests/x_lint.rs:1 — private token reader (`struct Token`)"),
            "{failures:?}"
        );
        assert!(
            failures[1]
                .starts_with("crates/a/tests/x_lint.rs:6 — private token reader (`fn tokens`)"),
            "{failures:?}"
        );
    }

    #[test]
    fn reasoned_marker_above_a_private_token_reader_passes() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "// source-lint-scaffolding: allow — fixture reading shell, not Rust\n\
             enum Tok {\n    Pipe,\n}\n\n#[test]\nfn t() {}\n",
        );
        assert_eq!(undiscovered_lints(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn bare_marker_above_a_private_token_reader_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_lint(
            tmp.path(),
            "// source-lint-scaffolding: allow\nstruct TokenKind;\n\n#[test]\nfn t() {}\n",
        );
        assert_single_failure_at(
            &undiscovered_lints(tmp.path()),
            "crates/a/tests/x_lint.rs:2 — private token reader (`struct TokenKind`); read tokens \
             with source_lint::tokenize under a `// source-lint-scaffolding: allow` marker with \
             no reason; ",
        );
    }

    #[test]
    fn token_reader_lookalikes_are_not_hits() {
        let src = "// enum Tok was here\n\
                   const DOC: &str = \"struct Token\";\n\
                   struct TokenBudget;\n\
                   enum Tokens { A }\n\
                   fn tokens_for(x: u8) {}\n\
                   fn run(t: Token) -> Vec<Tok> { tokens(DOC) }\n\
                   use intentd_test_support::source_lint::Token;\n";
        let blanked = intentd_test_support::source_lint::lex(src).blanked;
        assert_eq!(token_reader_definitions(&blanked), Vec::new());
    }

    #[test]
    fn token_reader_definitions_match_each_name() {
        for name in super::TOKEN_READER_TYPES {
            for keyword in ["enum", "struct"] {
                assert_eq!(
                    token_reader_definitions(&format!("\npub(crate) {keyword} {name} {{")),
                    vec![(2, format!("{keyword} {name}"))]
                );
            }
        }
        for name in super::TOKEN_READER_FNS {
            assert_eq!(
                token_reader_definitions(&format!("fn {name}(x: &str) {{")),
                vec![(1, format!("fn {name}"))]
            );
            assert_eq!(
                token_reader_definitions(&format!("fn {name}<'a>(x: &'a str) {{")),
                vec![(1, format!("fn {name}"))]
            );
        }
    }

    const AGENTS_MD: &str = "# Guide\n\n## Project layout\n\n```text\ncrates/\n\
                             └── a/  # the only crate\n```\n\n## Gates\n\n\
                             | Lint | Fails on | Opt-out / baseline |\n| --- | --- | --- |\n\
                             | `x_lint` | everything | none |\n\nTrailing prose.\n";

    fn write_agents(dir: &Path, content: &str) {
        fs::write(dir.join("AGENTS.md"), content).unwrap();
    }

    #[test]
    fn agents_md_listing_every_crate_and_lint_passes() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(tmp.path(), AGENTS_MD);
        assert_eq!(agents_md_drift(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn lint_file_without_a_table_row_is_reported_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        fs::write(
            tmp.path().join("crates/a/tests/y_lint.rs"),
            "#[test]\nfn t() {}\n",
        )
        .unwrap();
        write_agents(tmp.path(), AGENTS_MD);
        assert_single_failure_at(
            &agents_md_drift(tmp.path()),
            "AGENTS.md: lint table has no row for `y_lint` (crates/a/tests/y_lint.rs)",
        );
    }

    #[test]
    fn table_row_naming_no_lint_file_is_reported_at_its_line() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(
            tmp.path(),
            &AGENTS_MD.replace(
                "| `x_lint` | everything | none |\n",
                "| `x_lint` | everything | none |\n| `gone_lint` | nothing | none |\n",
            ),
        );
        assert_single_failure_at(
            &agents_md_drift(tmp.path()),
            "AGENTS.md:15: lint table row `gone_lint` names no `crates/*/tests/gone_lint.rs`",
        );
    }

    #[test]
    fn duplicate_table_row_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(
            tmp.path(),
            &AGENTS_MD.replace(
                "| `x_lint` | everything | none |\n",
                "| `x_lint` | everything | none |\n| `x_lint` | again | none |\n",
            ),
        );
        assert_single_failure_at(
            &agents_md_drift(tmp.path()),
            "AGENTS.md:15: lint table row `x_lint` is a duplicate",
        );
    }

    #[test]
    fn crate_missing_from_the_layout_is_reported_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        let b = tmp.path().join("crates/b");
        fs::create_dir_all(b.join("src")).unwrap();
        fs::write(
            b.join("Cargo.toml"),
            "[package]\nname = \"b\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::write(b.join("src/lib.rs"), "").unwrap();
        fs::create_dir_all(tmp.path().join("crates/not-a-crate")).unwrap();
        write_agents(tmp.path(), AGENTS_MD);
        assert_single_failure_at(
            &agents_md_drift(tmp.path()),
            "AGENTS.md: project layout does not list crate `b/` (crates/b/Cargo.toml)",
        );
    }

    #[test]
    fn layout_line_naming_no_crate_is_reported_at_its_line() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(
            tmp.path(),
            &AGENTS_MD.replace("└── a/", "├── gone/         # removed crate\n└── a/"),
        );
        assert_single_failure_at(
            &agents_md_drift(tmp.path()),
            "AGENTS.md:7: project layout lists `gone/`, but `crates/gone/Cargo.toml` does not exist",
        );
    }

    #[test]
    fn missing_table_and_layout_are_reported_not_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(tmp.path(), "# Guide\n\nNo lists here.\n");
        let failures = agents_md_drift(tmp.path());
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(
            failures[0].starts_with("AGENTS.md: no source-lint table"),
            "{failures:?}"
        );
        assert!(
            failures[1].starts_with("AGENTS.md: no fenced crate tree"),
            "{failures:?}"
        );
    }

    #[test]
    fn unclosed_layout_fence_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(
            tmp.path(),
            "## Project layout\n\n```text\ncrates/\n└── a/\n",
        );
        let failures = agents_md_drift(tmp.path());
        assert!(
            failures
                .iter()
                .any(|f| f.starts_with("AGENTS.md: no fenced crate tree")),
            "{failures:?}"
        );
    }

    #[test]
    fn nested_layout_entries_are_not_crates() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(
            tmp.path(),
            &AGENTS_MD.replace(
                "└── a/  # the only crate\n",
                "└── a/  # the only crate\n    ├── src/\n    └── tests/\n",
            ),
        );
        assert_eq!(agents_md_drift(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn layout_entry_without_slash_or_with_backticks_still_counts() {
        for entry in ["└── a  # the only crate", "└── `a/`  # the only crate"] {
            let tmp = tempfile::tempdir().unwrap();
            fixture(tmp.path(), "");
            write_agents(
                tmp.path(),
                &AGENTS_MD.replace("└── a/  # the only crate", entry),
            );
            assert_eq!(agents_md_drift(tmp.path()), Vec::<String>::new(), "{entry}");
        }
    }

    #[test]
    fn table_row_with_an_empty_first_cell_is_reported() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(
            tmp.path(),
            &AGENTS_MD.replace(
                "| `x_lint` | everything | none |\n",
                "| `x_lint` | everything | none |\n| | nameless | none |\n",
            ),
        );
        assert_single_failure_at(
            &agents_md_drift(tmp.path()),
            "AGENTS.md:15: lint table row `` names no",
        );
    }

    #[test]
    fn crlf_line_endings_and_escaped_pipes_in_later_cells_pass() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        let agents = AGENTS_MD
            .replace("| everything |", "| `a \\| b` pipelines |")
            .replace('\n', "\r\n");
        write_agents(tmp.path(), &agents);
        assert_eq!(agents_md_drift(tmp.path()), Vec::<String>::new());
    }

    #[test]
    fn layout_section_without_a_fence_does_not_borrow_a_later_block() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        write_agents(
            tmp.path(),
            &(AGENTS_MD.replace(
                "```text\ncrates/\n└── a/  # the only crate\n```\n",
                "Prose.\n",
            ) + "\n## Later\n\n```text\n└── a/\n```\n"),
        );
        assert_single_failure_at(
            &agents_md_drift(tmp.path()),
            "AGENTS.md: no fenced crate tree",
        );
    }

    #[test]
    fn missing_agents_md_is_reported_as_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        fixture(tmp.path(), "");
        assert_single_failure_at(&agents_md_drift(tmp.path()), "AGENTS.md: cannot read it");
    }
}
