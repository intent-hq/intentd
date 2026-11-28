//! npx launch flag lint.
//!
//! Intent launches an npx provider with npm flags that only its builders
//! spell: `intent_providers::NPM_INSTALL_ARGS` and `NPM_ALLOW_SCRIPTS_ARG`,
//! joined by `intent_providers::npx_install_args` and
//! `intent_acp::spawn::npx_run_args`. A test that types those flags out
//! breaks on every flag change: intent-hq/intentd#3827 broke 81 tests this
//! way, and after intent-hq/intentd#3844 moved the known copies onto the
//! shared fake (`intent_acp::fake_npx`) one more copy was found only by hand.
//! This test fails, naming `file:line`, for every hand-typed copy.
//!
//! What it scans, under `crates/` (`node_modules/`, `target/` and `.git/`
//! skipped; generated goldens are JSON and never scanned):
//!
//! - every string literal in every `*.rs` file, test code included, matched
//!   by cooked value;
//! - the whole text of every `*.mjs`, `*.cjs`, `*.js`, `*.py` and `*.sh`
//!   fixture script.
//!
//! The flags are read from the constants, so the lint follows a flag change;
//! a text holding any of them as a substring (`--allow-scripts=pkg`) counts.
//! The builders and the one pin test
//! (`intent_acp::fake_npx::tests::production_npx_argv_is_pinned`) are
//! allowed by item, see [`ALLOWED_ITEMS`]; an allowed item that no longer
//! exists fails the lint.
//!
//! Another npm command may share a flag on purpose (package preparation's
//! `--include=optional`). Read the constant where it can, or opt out with a
//! reasoned `// npx-launch-flags: allow — <reason>` (`#` in shell/Python) on
//! the line directly above the line holding the flag, or above a multi-line
//! literal's opening line. A malformed marker never suppresses a hit, and a
//! marker that covers no flag is reported as stale.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    classify_marker, lex, markers_by_line, matching_close, tokenize, workspace_root, Marker,
};

const OPT_OUT_TAG: &str = "npx-launch-flags";
const SCRIPT_EXTENSIONS: &[&str] = &["mjs", "cjs", "js", "py", "sh"];
const SKIPPED_DIRS: &[&str] = &["node_modules", "target", ".git"];

/// The items that may spell the flags: the builder's constants and the pin
/// test that holds the one literal copy of the full production argv.
const ALLOWED_ITEMS: &[(&str, &str, &str)] = &[
    (
        "crates/intent-providers/src/config.rs",
        "NPM_INSTALL_ARGS",
        "defines the npm install flags",
    ),
    (
        "crates/intent-providers/src/config.rs",
        "NPM_ALLOW_SCRIPTS_ARG",
        "defines npm 12's allow-scripts flag",
    ),
    (
        "crates/intent-acp/src/fake_npx.rs",
        "production_npx_argv_is_pinned",
        "pins the production argv so a flag change is a reviewed edit",
    ),
];

const ADVICE: &str = "Build the npx launch argv with intent_acp::spawn::npx_run_args or \
intent_providers::npx_install_args (a fake npx: intent_acp::fake_npx::FakeNpx), or read \
intent_providers::NPM_INSTALL_ARGS / NPM_ALLOW_SCRIPTS_ARG, instead of typing the flags. A \
different npm command that shares a flag on purpose takes a \
`// npx-launch-flags: allow — <reason>` marker on the line above.";

/// The flags a copy is recognized by, read from the builder's constants.
fn flags() -> Vec<&'static str> {
    let mut out = intent_providers::NPM_INSTALL_ARGS.to_vec();
    out.push(intent_providers::NPM_ALLOW_SCRIPTS_ARG);
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Finding {
    /// A hand-typed flag no reasoned marker covers.
    Flag {
        line: usize,
        flags: Vec<String>,
        marker_malformed: bool,
    },
    /// A reasoned marker that covers no flag.
    StaleMarker { line: usize },
    /// An [`ALLOWED_ITEMS`] entry naming an item the file no longer has.
    MissingItem { name: String },
}

/// One text that may hold flags: a Rust literal or a whole script.
struct Region {
    /// Line the text starts on.
    first_line: usize,
    /// The text as written, for locating flags by line.
    raw: String,
    /// The value the program sees (Rust escapes evaluated).
    cooked: String,
}

/// The flags in `region`, each with the source line it sits on. A flag only
/// visible once escapes are cooked is reported at the region's first line.
fn flag_lines(region: &Region, flags: &[&str]) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for flag in flags {
        let mut seen_raw = false;
        for (k, text) in region.raw.split('\n').enumerate() {
            if text.contains(flag) {
                seen_raw = true;
                out.push((region.first_line + k, (*flag).to_owned()));
            }
        }
        if !seen_raw && region.cooked.contains(flag) {
            out.push((region.first_line, (*flag).to_owned()));
        }
    }
    out
}

/// Char ranges of the `fn` / `const` / `static` items named `name` in
/// blanked text: a `fn` ends at the `}` closing its body, a `const` or
/// `static` at the first `;` at its own depth.
fn item_ranges(blanked: &[char], name: &str) -> Vec<(usize, usize)> {
    let tokens = tokenize(blanked);
    let mut out = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        let named = tokens.get(i + 1).is_some_and(|n| n.is(name));
        if !named || !(t.is("fn") || t.is("const") || t.is("static")) {
            continue;
        }
        let end = if t.is("fn") {
            tokens[i..]
                .iter()
                .position(|x| x.is("{"))
                .and_then(|open| matching_close(&tokens, i + open))
        } else {
            tokens[i..]
                .iter()
                .position(|x| x.is(";") && x.depth == t.depth)
                .map(|k| i + k)
        };
        if let Some(end) = end {
            out.push((t.start, tokens[end].end));
        }
    }
    out
}

/// Applies the marker rule to every flag found in `regions`, given each
/// line's marker state (index 0 unused).
fn judge(regions: &[Region], markers: &[Marker], flags: &[&str]) -> Vec<Finding> {
    let marker_at = |line: usize| markers.get(line).copied().unwrap_or(Marker::Absent);
    let mut used = BTreeSet::new();
    let mut by_line: Vec<(usize, Vec<String>, bool)> = Vec::new();
    for region in regions {
        for (line, flag) in flag_lines(region, flags) {
            let candidates = [line.checked_sub(1), region.first_line.checked_sub(1)];
            let covering = candidates
                .into_iter()
                .flatten()
                .find(|&l| marker_at(l) == Marker::WithReason);
            if let Some(l) = covering {
                used.insert(l);
                continue;
            }
            let malformed = candidates
                .into_iter()
                .flatten()
                .any(|l| marker_at(l) == Marker::Malformed);
            match by_line.iter_mut().find(|(l, _, _)| *l == line) {
                Some((_, fs, m)) => {
                    if !fs.contains(&flag) {
                        fs.push(flag);
                    }
                    *m |= malformed;
                }
                None => by_line.push((line, vec![flag], malformed)),
            }
        }
    }
    by_line.sort();
    let mut out: Vec<Finding> = by_line
        .into_iter()
        .map(|(line, flags, marker_malformed)| Finding::Flag {
            line,
            flags,
            marker_malformed,
        })
        .collect();
    out.extend(
        (1..markers.len())
            .filter(|&l| markers[l] == Marker::WithReason && !used.contains(&l))
            .map(|line| Finding::StaleMarker { line }),
    );
    out
}

/// Findings for one Rust file; `allowed` names the items of this file that
/// may spell the flags.
fn rust_findings(src: &str, allowed: &[&str], flags: &[&str]) -> Vec<Finding> {
    let lexed = lex(src);
    let chars: Vec<char> = lexed.blanked.chars().collect();
    let mut out = Vec::new();
    let mut ranges = Vec::new();
    for name in allowed {
        let found = item_ranges(&chars, name);
        if found.is_empty() {
            out.push(Finding::MissingItem {
                name: (*name).to_owned(),
            });
        }
        ranges.extend(found);
    }
    let regions: Vec<Region> = lexed
        .literals
        .iter()
        .filter(|lit| !ranges.iter().any(|(s, e)| (*s..*e).contains(&lit.offset)))
        .map(|lit| Region {
            first_line: lit.line,
            raw: lit.text.clone(),
            cooked: lit.cooked(),
        })
        .collect();
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    out.extend(judge(&regions, &markers, flags));
    out
}

/// Findings for one fixture script, whose `//` or `#` line comments carry
/// the marker.
fn script_findings(src: &str, flags: &[&str]) -> Vec<Finding> {
    let mut markers = vec![Marker::Absent];
    markers.extend(src.lines().map(|line| {
        let t = line.trim_start();
        match t.strip_prefix('#') {
            Some(rest) => classify_marker(&format!("//{rest}"), OPT_OUT_TAG),
            None => classify_marker(t, OPT_OUT_TAG),
        }
    }));
    let region = Region {
        first_line: 1,
        raw: src.to_owned(),
        cooked: src.to_owned(),
    };
    judge(&[region], &markers, flags)
}

fn render(rel: &str, src: &str, finding: &Finding) -> String {
    let text = |line: usize| src.lines().nth(line - 1).map_or("", str::trim);
    match finding {
        Finding::Flag {
            line,
            flags,
            marker_malformed,
        } => {
            let note = if *marker_malformed {
                " (the marker above is malformed)"
            } else {
                ""
            };
            format!(
                "{rel}:{line}: hand-typed {}{note}: {}",
                flags.join(", "),
                text(*line)
            )
        }
        Finding::StaleMarker { line } => {
            format!("{rel}:{line}: stale marker covers no npx launch flag; remove it")
        }
        Finding::MissingItem { name } => {
            format!("{rel}: allowed item `{name}` not found; update ALLOWED_ITEMS")
        }
    }
}

fn collect_scripts(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if !SKIPPED_DIRS.contains(&name.as_str()) {
                collect_scripts(&path, out);
            }
        } else if path
            .extension()
            .is_some_and(|e| SCRIPT_EXTENSIONS.contains(&e.to_string_lossy().as_ref()))
        {
            out.push(path);
        }
    }
}

fn rel_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn no_hand_typed_npx_launch_flags() {
    let root = workspace_root();
    let crates = root.join("crates");
    let rust = intentd_test_support::source_lint::rust_files(&crates);
    assert!(
        rust.len() > 100,
        "scanned only {} Rust files under {}; the lint is not seeing the source tree",
        rust.len(),
        crates.display()
    );
    let mut scripts = Vec::new();
    collect_scripts(&crates, &mut scripts);
    scripts.sort();
    let flags = flags();
    let mut seen_allowed = BTreeSet::new();
    let mut report = Vec::new();
    for path in rust.iter().chain(&scripts) {
        let rel = rel_path(&root, path);
        let src =
            fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let findings = if path.extension().is_some_and(|e| e == "rs") {
            let allowed: Vec<&str> = ALLOWED_ITEMS
                .iter()
                .filter(|(p, _, _)| *p == rel)
                .map(|(_, name, _)| *name)
                .collect();
            if !allowed.is_empty() {
                seen_allowed.insert(rel.clone());
            }
            rust_findings(&src, &allowed, &flags)
        } else {
            script_findings(&src, &flags)
        };
        report.extend(findings.iter().map(|f| render(&rel, &src, f)));
    }
    for (path, name, _) in ALLOWED_ITEMS {
        if !seen_allowed.contains(*path) {
            report.push(format!(
                "{path}: allowed item `{name}` names a missing file"
            ));
        }
    }
    assert!(
        report.is_empty(),
        "found hand-typed npx launch flags:\n  {}\n\n{ADVICE}",
        report.join("\n  ")
    );
}

fn allow() -> String {
    intent_providers::NPM_ALLOW_SCRIPTS_ARG.to_owned()
}

fn include() -> String {
    intent_providers::NPM_INSTALL_ARGS[1].to_owned()
}

fn marker() -> String {
    format!("// {OPT_OUT_TAG}: allow — a reason")
}

fn flag_lines_of(findings: &[Finding]) -> Vec<usize> {
    findings
        .iter()
        .filter_map(|f| match f {
            Finding::Flag { line, .. } => Some(*line),
            _ => None,
        })
        .collect()
}

#[test]
fn flags_literals_in_tests_with_their_line() {
    let src = format!(
        "#[cfg(test)]\nmod tests {{\n    fn t() {{\n        let a = \"{}=x\";\n        \
         let b = r#\"-y {}\"#;\n    }}\n}}\n",
        allow(),
        include()
    );
    let findings = rust_findings(&src, &[], &flags());
    assert_eq!(flag_lines_of(&findings), [4, 5], "{findings:?}");
    assert_eq!(
        render("crates/x/tests/a.rs", &src, &findings[0]),
        format!(
            "crates/x/tests/a.rs:4: hand-typed {}: let a = \"{}=x\";",
            allow(),
            allow()
        )
    );
}

#[test]
fn finds_flags_on_later_lines_and_behind_escapes() {
    let split = format!("let a = \"one\ntwo {}\";\n", include());
    assert_eq!(flag_lines_of(&rust_findings(&split, &[], &flags())), [2]);
    let escaped = format!("let a = \"{}\";\n", allow().replace('-', "\\x2d"));
    assert_eq!(flag_lines_of(&rust_findings(&escaped, &[], &flags())), [1]);
}

#[test]
fn ignores_comments_and_other_flags() {
    let src = format!(
        "// {a}\n/* {i} */\n/// {a}\nlet x = \"--ignore-scripts\";\nlet y = \"allow-scripts\";\n",
        a = allow(),
        i = include()
    );
    assert!(rust_findings(&src, &[], &flags()).is_empty());
}

#[test]
fn reasoned_marker_covers_the_next_line_or_the_literal_opening() {
    let next = format!("{}\nlet a = \"{}\";\n", marker(), allow());
    assert!(rust_findings(&next, &[], &flags()).is_empty());
    let opening = format!("{}\nlet a = r\"x\n{}\";\n", marker(), include());
    assert!(rust_findings(&opening, &[], &flags()).is_empty());
    let far = format!("{}\nlet b = 1;\nlet a = \"{}\";\n", marker(), allow());
    let findings = rust_findings(&far, &[], &flags());
    assert!(
        findings.contains(&Finding::StaleMarker { line: 1 }),
        "{findings:?}"
    );
    assert_eq!(flag_lines_of(&findings), [3]);
}

#[test]
fn malformed_marker_never_suppresses() {
    let src = format!("// {OPT_OUT_TAG}: allow\nlet a = \"{}\";\n", allow());
    assert_eq!(
        rust_findings(&src, &[], &flags()),
        [Finding::Flag {
            line: 2,
            flags: vec![allow()],
            marker_malformed: true,
        }]
    );
}

#[test]
fn allowed_items_cover_only_themselves() {
    let src = format!(
        "pub const ARGS: [&str; 1] = [\"{i}\"];\nfn pinned() {{\n    let v = [\"{a}=p\"];\n}}\n\
         fn other() {{ let v = \"{a}\"; }}\n",
        a = allow(),
        i = include()
    );
    let findings = rust_findings(&src, &["ARGS", "pinned"], &flags());
    assert_eq!(flag_lines_of(&findings), [5], "{findings:?}");
    assert_eq!(
        rust_findings("fn x() {}\n", &["pinned"], &flags()),
        [Finding::MissingItem {
            name: "pinned".into()
        }]
    );
}

#[test]
fn scripts_are_scanned_with_hash_markers() {
    let src = format!(
        "#!/bin/sh\nexec npx {a}=x \"$@\"\n# {OPT_OUT_TAG}: allow — reason\nnpm i {i}\n",
        a = allow(),
        i = include()
    );
    assert_eq!(flag_lines_of(&script_findings(&src, &flags())), [2]);
}
