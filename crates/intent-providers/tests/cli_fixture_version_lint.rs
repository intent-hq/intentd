//! CLI fixture version lint.
//!
//! Fake Codex and Claude CLIs used to print hand-written versions (`2.0.0`,
//! `fixture-cli 1.0.0`). When intent-hq/intentd#2567 gave Claude a 2.1.0
//! minimum, about ten unrelated tests failed at once because their fixtures
//! suddenly counted as too old, and some tried to start a real npm download.
//! Every adapter or SDK bump that raises a minimum would repeat that.
//! Ordinary fixtures now print
//! `intent_providers::test_support::current_cli_version` (or are
//! `current_cli_script`), which follows the adapter pins; this test keeps
//! hand-written versions from creeping back.
//!
//! What it scans, under `crates/` (tests included — that is where fixtures
//! live; `node_modules/` and `target/` are skipped):
//!
//! - every string literal in every `*.rs` file;
//! - the whole text of every non-Rust fixture script (`*.mjs`, `*.cjs`,
//!   `*.js`, `*.py`, `*.sh`), which tests load with `include_str!` or by path.
//!
//! It fails, naming `file:line`, on every hand-written Codex or Claude
//! `--version` output — old, exact, or newer — since even a version that
//! passes today breaks when a bump overtakes it. The report says when a
//! version is already below the minimum
//! `intent_providers::adapter_cli::requirement` derives today.
//!
//! A version (`N.N.N`, optionally `v`-prefixed and with a pre-release) counts
//! as a CLI's when the word right before it (separated by spaces or tabs)
//! ends with `codex` / `codex-cli` (Codex), `claude` / `claude-code`
//! (Claude), or `fixture-cli` (either, so it must satisfy both), or when it
//! is followed by ` (Claude Code)`. Versions printed with no CLI name (a bare
//! `printf '1.2.3'`) cannot be attributed and are not checked; neither are
//! versions assembled at run time.
//!
//! A test that needs an exact version (an old CLI, a version-reporting
//! assertion, two distinct versions) opts out with a reasoned
//! `cli-fixture-version: allow — <reason>` marker on the line immediately
//! above the line holding the version. In Rust it is a standalone `//`
//! comment in the shared marker grammar of
//! `intentd_test_support::source_lint`; for a version on a later line of a
//! multi-line literal (whose line above is still inside the literal) the
//! marker may instead sit on the line above the literal's opening line. In a
//! non-Rust script it is a line comment in that language (`//` or `#`). A
//! malformed marker never suppresses the hit, and a marker that no longer
//! covers any version is reported as stale, so it cannot silently allow a
//! version written there later.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use intent_providers::adapter_cli::requirement;
use intentd_test_support::source_lint::{
    classify_marker, lex, markers_by_line, rust_files, workspace_root, Marker,
};

const OPT_OUT_TAG: &str = "cli-fixture-version";
const SELF_FILE: &str = "crates/intent-providers/tests/cli_fixture_version_lint.rs";
const SCRIPT_EXTENSIONS: &[&str] = &["mjs", "cjs", "js", "py", "sh"];
const SKIPPED_DIRS: &[&str] = &["node_modules", "target", ".git"];
const CODEX: &str = "codex";
const CLAUDE: &str = "claude-code";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Finding {
    /// A hand-written CLI version no reasoned marker covers.
    Version {
        line: usize,
        /// `codex`, `claude-code`, or both for a `fixture-cli` stand-in.
        provider: String,
        output: String,
        /// `Some(minimum)` when the version is already below it.
        below: Option<String>,
        marker_malformed: bool,
    },
    /// A reasoned marker that covers no version.
    StaleMarker { line: usize },
}

/// Text to search for versions: a Rust string literal, or a whole script.
struct Region<'a> {
    text: &'a str,
    /// 1-based line of the text's first character.
    line: usize,
    /// The literal's opening line, when a marker above it may cover versions
    /// on its later lines.
    opener: Option<usize>,
}

/// The providers whose minimum a version must satisfy, judged by the word
/// just before it and the text just after it.
fn providers_for(before: &str, after: &str) -> &'static [&'static str] {
    if after.starts_with(" (Claude Code)") {
        return &[CLAUDE];
    }
    let trimmed = before.trim_end_matches([' ', '\t']);
    if trimmed.len() == before.len() {
        return &[];
    }
    let word: String = trimmed
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let word = word.to_ascii_lowercase();
    if word.ends_with("fixture-cli") {
        &[CODEX, CLAUDE]
    } else if word.ends_with("codex-cli") || word.ends_with("codex") {
        &[CODEX]
    } else if word.ends_with("claude-code") || word.ends_with("claude") {
        &[CLAUDE]
    } else {
        &[]
    }
}

/// `(byte offset of the token, version)` for every `N.N.N[-pre]` token in
/// `text`, optionally `v`-prefixed (the offset then points at the `v`, the
/// version excludes it), that is not part of a longer dotted or alphanumeric
/// run.
fn versions(text: &str) -> Vec<(usize, &str)> {
    let bytes = text.as_bytes();
    let word = |i: usize| bytes[i].is_ascii_alphanumeric() || matches!(bytes[i], b'.' | b'-');
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let token = if i == 0 || !word(i - 1) {
            Some(i)
        } else if bytes[i - 1] == b'v' && (i == 1 || !word(i - 2)) {
            Some(i - 1)
        } else {
            None
        };
        let start = i;
        let mut parts = 0;
        loop {
            let digits = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i == digits {
                break;
            }
            parts += 1;
            if parts < 3 && i + 1 < bytes.len() && bytes[i] == b'.' && bytes[i + 1].is_ascii_digit()
            {
                i += 1;
            } else {
                break;
            }
        }
        let continues = i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.');
        match token {
            Some(token) if parts == 3 && !continues => {
                let mut end = i;
                if end + 1 < bytes.len()
                    && bytes[end] == b'-'
                    && bytes[end + 1].is_ascii_alphanumeric()
                {
                    end += 1;
                    while end < bytes.len()
                        && (bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'.' | b'-'))
                    {
                        end += 1;
                    }
                    // `1.2.3-beta.1.` would end on a dot; that dot is punctuation.
                    while bytes[end - 1] == b'.' {
                        end -= 1;
                    }
                }
                out.push((token, &text[start..end]));
                i = end;
            }
            _ => {
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                    i += 1;
                }
            }
        }
    }
    out
}

/// Every hand-written CLI version in `regions` that no reasoned marker
/// covers, plus every reasoned marker (by 1-based line in `markers`) that
/// covers none.
fn scan_regions(regions: &[Region<'_>], markers: &[Marker]) -> Vec<Finding> {
    let marker = |line: usize| markers.get(line).copied().unwrap_or(Marker::Absent);
    let mut used = BTreeSet::new();
    let mut findings = Vec::new();
    for region in regions {
        let text = region.text;
        for (offset, version) in versions(text) {
            let prefix = usize::from(text.as_bytes()[offset] == b'v');
            let after = &text[offset + prefix + version.len()..];
            let providers = providers_for(&text[..offset], after);
            if providers.is_empty() {
                continue;
            }
            // `text` is the body as written: its newlines are the source's.
            let line = region.line + text[..offset].matches('\n').count();
            let mut candidates = vec![line - 1];
            if let Some(opener) = region.opener.filter(|&opener| opener < line) {
                candidates.push(opener - 1);
            }
            if let Some(&covering) = candidates
                .iter()
                .find(|&&c| marker(c) == Marker::WithReason)
            {
                used.insert(covering);
                continue;
            }
            let below = providers.iter().find_map(|&provider| {
                let required = requirement(provider)
                    .unwrap_or_else(|| panic!("{provider} declares no CLI requirement"));
                (required.version_ok(version) == Some(false)).then(|| required.minimum.to_string())
            });
            findings.push(Finding::Version {
                line,
                provider: providers.join("/"),
                output: version.to_owned(),
                below,
                marker_malformed: candidates.iter().any(|&c| marker(c) == Marker::Malformed),
            });
        }
    }
    for (line, state) in markers.iter().enumerate() {
        if *state == Marker::WithReason && !used.contains(&line) {
            findings.push(Finding::StaleMarker { line });
        }
    }
    findings.sort_by_key(|finding| match finding {
        Finding::Version { line, .. } | Finding::StaleMarker { line } => *line,
    });
    findings
}

/// Findings for one Rust source file: its string literals, with markers from
/// its standalone `//` comments.
fn scan_rust(src: &str) -> Vec<Finding> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let regions: Vec<_> = lexed
        .literals
        .iter()
        .map(|literal| Region {
            text: &literal.text,
            line: literal.line,
            opener: Some(literal.line),
        })
        .collect();
    scan_regions(&regions, &markers)
}

/// Findings for one non-Rust fixture script: its whole text minus whole-line
/// comments (prose, not output), with markers from any line comment (`//` or
/// `#`) carrying the marker.
fn scan_script(src: &str) -> Vec<Finding> {
    let mut markers = vec![Marker::Absent; src.lines().count() + 1];
    let mut code = String::with_capacity(src.len());
    for (index, text) in src.lines().enumerate() {
        let body = text.trim_start();
        let comment = body
            .strip_prefix("//")
            .or_else(|| body.strip_prefix('#'))
            .map(str::trim_start);
        if let Some(comment) = comment {
            if comment.starts_with(OPT_OUT_TAG) {
                markers[index + 1] = classify_marker(&format!("// {comment}"), OPT_OUT_TAG);
            }
        } else {
            code.push_str(text);
        }
        code.push('\n');
    }
    let regions = [Region {
        text: &code,
        line: 1,
        opener: None,
    }];
    scan_regions(&regions, &markers)
}

/// Every non-Rust fixture script under `dir`, sorted.
fn script_files(dir: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let entries =
            fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
                .path();
            if path.is_dir() {
                let skipped = path
                    .file_name()
                    .is_some_and(|name| SKIPPED_DIRS.iter().any(|skip| name == *skip));
                if !skipped {
                    walk(&path, out);
                }
            } else if path
                .extension()
                .is_some_and(|ext| SCRIPT_EXTENSIONS.iter().any(|want| ext == *want))
            {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    out.sort();
    out
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

fn describe(finding: &Finding) -> String {
    match finding {
        Finding::Version {
            line,
            provider,
            output,
            below,
            marker_malformed,
        } => {
            let below = below
                .as_ref()
                .map(|minimum| format!(" — already below the current minimum {minimum}"))
                .unwrap_or_default();
            let note = if *marker_malformed {
                "  (opt-out marker is malformed: expected `cli-fixture-version: allow — <reason>`)"
            } else {
                ""
            };
            format!("{line}: hand-written {provider} CLI version {output}{below}{note}")
        }
        Finding::StaleMarker { line } => format!(
            "{line}: stale `cli-fixture-version` marker — the next line holds no hand-written \
             CLI version; remove it"
        ),
    }
}

#[test]
fn cli_fixtures_never_hand_write_a_codex_or_claude_version() {
    let root = workspace_root();
    assert!(
        root.join(SELF_FILE).is_file(),
        "{SELF_FILE} moved; update SELF_FILE so the lint keeps skipping its own examples"
    );
    let crates = root.join("crates");
    let rust = rust_files(&crates);
    let scripts = script_files(&crates);
    assert!(
        rust.len() > 100 && !scripts.is_empty(),
        "too few sources under {}",
        crates.display()
    );

    let mut report = Vec::new();
    let scanned = rust
        .iter()
        .map(|file| (file, true))
        .chain(scripts.iter().map(|file| (file, false)));
    for (file, is_rust) in scanned {
        let rel = display_rel(
            file.strip_prefix(&root)
                .expect("source path under the workspace root"),
        );
        if rel == SELF_FILE {
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let findings = if is_rust {
            scan_rust(&src)
        } else {
            scan_script(&src)
        };
        report.extend(
            findings
                .iter()
                .map(|finding| format!("{rel}:{}", describe(finding))),
        );
    }

    assert!(
        report.is_empty(),
        "fake Codex/Claude CLIs report hand-written versions:\n\n{}\n\n\
         An ordinary fixture should print \
         `intent_providers::test_support::current_cli_version(cli)` or be \
         `current_cli_script(cli)` (enable the intent-providers `test-support` feature from \
         [dev-dependencies]), so it stays current when an adapter bump raises the minimum. A \
         test that needs this exact version (an old CLI, a version-reporting assertion, two \
         distinct versions) may keep the literal with `// cli-fixture-version: allow — <reason>` \
         (`#` in shell/Python) on the line immediately above it; the reason is required.",
        report.join("\n")
    );
}

#[test]
fn versions_finds_whole_semver_tokens_only() {
    let found = |text: &str| -> Vec<(usize, String)> {
        versions(text)
            .into_iter()
            .map(|(offset, v)| (offset, v.to_owned()))
            .collect()
    };
    assert_eq!(found("codex-cli 0.114.0\\n"), [(10, "0.114.0".into())]);
    assert_eq!(found("1.0.128 (Claude Code)"), [(0, "1.0.128".into())]);
    assert_eq!(
        found("codex-cli 0.159.1-beta.1."),
        [(10, "0.159.1-beta.1".into())]
    );
    assert_eq!(
        found("v1.2.3 and 4.5.6"),
        [(0, "1.2.3".into()), (11, "4.5.6".into())]
    );
    for text in [
        "1.2", "1.2.3.4", "a1.2.3", "1.2.3x", "10.9", "x-1.2.3", "dev1.2.3",
    ] {
        assert!(found(text).is_empty(), "{text}");
    }
}

#[test]
fn providers_follow_the_cli_name_or_claude_suffix() {
    assert_eq!(providers_for("printf 'codex-cli ", ""), [CODEX]);
    assert_eq!(providers_for("\\ncodex ", ""), [CODEX]);
    assert_eq!(providers_for("codex-cli\t", ""), [CODEX]);
    assert_eq!(providers_for("codex-cli  ", ""), [CODEX]);
    assert_eq!(providers_for("printf 'claude ", ""), [CLAUDE]);
    assert_eq!(providers_for("printf 'claude-code ", ""), [CLAUDE]);
    assert_eq!(providers_for("fixture-claude ", ""), [CLAUDE]);
    assert_eq!(providers_for("fixture-cli ", ""), [CODEX, CLAUDE]);
    assert_eq!(providers_for("printf '", " (Claude Code)\\n"), [CLAUDE]);
    for before in [
        "claude-agent-acp ",
        "npx ",
        "codex-cli",
        "Claude Code CLI ",
        "",
    ] {
        assert!(providers_for(before, "").is_empty(), "{before:?}");
    }
}

fn summary(findings: &[Finding]) -> Vec<(usize, String, String, bool, bool)> {
    findings
        .iter()
        .map(|finding| match finding {
            Finding::Version {
                line,
                provider,
                output,
                below,
                marker_malformed,
            } => (
                *line,
                provider.clone(),
                output.clone(),
                below.is_some(),
                *marker_malformed,
            ),
            Finding::StaleMarker { line } => (*line, "stale".into(), String::new(), false, false),
        })
        .collect()
}

#[test]
fn scan_flags_every_unmarked_cli_version_and_skips_marked_ones() {
    let src = r##"
fn f() {
    let a = "#!/bin/sh\nprintf 'codex-cli 0.0.1\\n'\n";
    let b = "0.0.1 (Claude Code)";
    let c = "claude 999.0.0";
    // cli-fixture-version: allow — exercises the bundled fallback
    let d = "codex-cli 0.0.1";
    // cli-fixture-version: allow
    let e = "fixture-cli 0.0.1";
    let f = r"#!/usr/bin/python3
print('claude 0.0.2')
";
    let g = "claude-agent-acp 0.0.1";
    let h = "npx 10.9.2";
    let i = "codex-cli v0.0.3";
}
"##;
    assert_eq!(
        summary(&scan_rust(src)),
        [
            (3, CODEX.into(), "0.0.1".into(), true, false),
            (4, CLAUDE.into(), "0.0.1".into(), true, false),
            (5, CLAUDE.into(), "999.0.0".into(), false, false),
            (9, "codex/claude-code".into(), "0.0.1".into(), true, true),
            (11, CLAUDE.into(), "0.0.2".into(), true, false),
            (15, CODEX.into(), "0.0.3".into(), true, false),
        ]
    );
    let Finding::Version { below, .. } = &scan_rust(src)[0] else {
        panic!("first finding is a version");
    };
    assert_eq!(
        below.as_deref(),
        Some(requirement(CODEX).unwrap().minimum.to_string().as_str())
    );
}

#[test]
fn a_marker_above_a_multi_line_literal_covers_its_later_lines() {
    let src = r##"
// cli-fixture-version: allow — an old Claude inside a script
let script = r"#!/bin/sh
printf 'claude 0.0.2'
printf 'codex-cli 0.0.3'
";
let other = r"#!/bin/sh
printf 'claude 0.0.4'
";
"##;
    assert_eq!(
        summary(&scan_rust(src)),
        [(8, CLAUDE.into(), "0.0.4".into(), true, false)]
    );
}

#[test]
fn stale_markers_are_reported() {
    let src = r#"
// cli-fixture-version: allow — the literal below moved away
let a = 1;
// cli-fixture-version: allow — still covers a version
let b = "codex-cli 0.0.1";
"#;
    assert_eq!(
        summary(&scan_rust(src)),
        [(2, "stale".into(), String::new(), false, false)]
    );
}

#[test]
fn scripts_are_scanned_with_their_own_comment_syntax() {
    let src = "#!/usr/bin/env node\n\
               console.log('codex-cli 0.0.1');\n\
               // Codex 0.0.9 prose in a comment is not output.\n\
               // cli-fixture-version: allow — doctor reports this version\n\
               console.log('0.0.2 (Claude Code)');\n\
               # cli-fixture-version: allow — python spelling\n\
               print('claude 0.0.3')\n\
               # cli-fixture-version: allow\n\
               print('claude 0.0.4')\n";
    assert_eq!(
        summary(&scan_script(src)),
        [
            (2, CODEX.into(), "0.0.1".into(), true, false),
            (9, CLAUDE.into(), "0.0.4".into(), true, true),
        ]
    );
}
