//! CLI fixture version lint.
//!
//! Fake Codex and Claude CLIs used to print hand-written versions (`2.0.0`,
//! `fixture-cli 1.0.0`). When intent-hq/intentd#2567 gave Claude a 2.1.0
//! minimum, about ten unrelated tests failed at once because their fixtures
//! suddenly counted as too old, and some tried to start a real npm download.
//! Every adapter or SDK bump that raises a minimum would repeat that.
//! Ordinary fixtures now print
//! `intent_providers::test_support::current_cli_version`, which follows the
//! adapter pins; this test keeps hand-written versions from creeping back.
//!
//! It scans every string literal in every `crates/**/*.rs` file (tests
//! included — that is where fixtures live) for a Codex or Claude `--version`
//! output and fails, naming `file:line`, on every hand-written version —
//! old, exact, or newer — since even a version that passes today breaks when
//! a bump overtakes it. The report says when a version is already below the
//! minimum `intent_providers::adapter_cli::requirement` derives today. A
//! version counts as a CLI's when the word right before it (one space apart)
//! ends with `codex` / `codex-cli` (Codex), `claude` (Claude), or
//! `fixture-cli` (either, so it must satisfy both), or when it is followed by
//! ` (Claude Code)`. Versions printed with no CLI name (a bare
//! `printf '1.2.3'`) cannot be attributed and are not checked.
//!
//! A test that needs an exact version (an old CLI, a version-reporting
//! assertion, two distinct versions) opts out with a standalone
//! `// cli-fixture-version: allow — <reason>` on the line immediately above
//! the line holding the version, in the shared marker grammar of
//! `intentd_test_support::source_lint`. A malformed marker never suppresses
//! the hit; the report says so.

use std::fs;
use std::path::Path;

use intent_providers::adapter_cli::requirement;
use intentd_test_support::source_lint::{lex, markers_by_line, rust_files, workspace_root, Marker};

const OPT_OUT_TAG: &str = "cli-fixture-version";
const SELF_FILE: &str = "crates/intent-providers/tests/cli_fixture_version_lint.rs";
const CODEX: &str = "codex";
const CLAUDE: &str = "claude-code";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    /// `codex`, `claude-code`, or both for a `fixture-cli` stand-in.
    provider: String,
    output: String,
    /// `Some(minimum)` when the version is already below it.
    below: Option<String>,
    marker_malformed: bool,
}

/// The providers whose minimum a version must satisfy, judged by the word
/// just before it and the text just after it.
fn providers_for(before: &str, after: &str) -> &'static [&'static str] {
    if after.starts_with(" (Claude Code)") {
        return &[CLAUDE];
    }
    let Some(before) = before.strip_suffix(' ') else {
        return &[];
    };
    let word: String = before
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
    } else if word.ends_with("claude") {
        &[CLAUDE]
    } else {
        &[]
    }
}

/// `(byte offset, version)` of every `N.N.N[-pre]` token in `text` that is
/// not part of a longer dotted or alphanumeric run.
fn versions(text: &str) -> Vec<(usize, &str)> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let boundary = i == 0
            || !(bytes[i - 1].is_ascii_alphanumeric() || matches!(bytes[i - 1], b'.' | b'-'));
        if !(boundary && bytes[i].is_ascii_digit()) {
            i += 1;
            continue;
        }
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
        if parts == 3 && !continues {
            let mut end = i;
            if end + 1 < bytes.len() && bytes[end] == b'-' && bytes[end + 1].is_ascii_alphanumeric()
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
            out.push((start, &text[start..end]));
            i = end;
        } else {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.') {
                i += 1;
            }
        }
    }
    out
}

/// Every hand-written Codex/Claude fixture version in one Rust source file
/// that is not suppressed by a reasoned opt-out marker.
fn scan_source(src: &str) -> Vec<Hit> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let mut hits = Vec::new();
    for literal in &lexed.literals {
        // The body as written: its newlines are the source's, so a version
        // inside a multi-line fixture is reported on its own line.
        let text = &literal.text;
        for (offset, version) in versions(text) {
            let line = literal.line + text[..offset].matches('\n').count();
            let after = &text[offset + version.len()..];
            let providers = providers_for(&text[..offset], after);
            if providers.is_empty() {
                continue;
            }
            let marker = markers.get(line - 1).copied().unwrap_or(Marker::Absent);
            if marker == Marker::WithReason {
                continue;
            }
            let below = providers.iter().find_map(|&provider| {
                let required = requirement(provider)
                    .unwrap_or_else(|| panic!("{provider} declares no CLI requirement"));
                (required.version_ok(version) == Some(false)).then(|| required.minimum.to_string())
            });
            hits.push(Hit {
                line,
                provider: providers.join("/"),
                output: version.to_owned(),
                below,
                marker_malformed: marker == Marker::Malformed,
            });
        }
    }
    hits
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn cli_fixtures_never_hand_write_a_codex_or_claude_version() {
    let root = workspace_root();
    assert!(
        root.join(SELF_FILE).is_file(),
        "{SELF_FILE} moved; update SELF_FILE so the lint keeps skipping its own examples"
    );
    let files = rust_files(&root.join("crates"));
    assert!(
        files.len() > 100,
        "too few Rust sources under {}",
        root.join("crates").display()
    );

    let mut report = Vec::new();
    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        let rel = display_rel(rel);
        if rel == SELF_FILE {
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        for hit in scan_source(&src) {
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// cli-fixture-version: allow — <reason>`)"
            } else {
                ""
            };
            let below = hit
                .below
                .map(|minimum| format!(" — already below the current minimum {minimum}"))
                .unwrap_or_default();
            report.push(format!(
                "{rel}:{}: hand-written {} CLI version {}{below}{note}",
                hit.line, hit.provider, hit.output
            ));
        }
    }

    assert!(
        report.is_empty(),
        "fake Codex/Claude CLIs report hand-written versions:\n\n{}\n\n\
         An ordinary fixture should print \
         `intent_providers::test_support::current_cli_version(cli)` (enable the \
         intent-providers `test-support` feature from [dev-dependencies]), so it stays current \
         when an adapter bump raises the minimum. A test that needs this exact version (an old \
         CLI, a version-reporting assertion, two distinct versions) may keep the literal with \
         `// cli-fixture-version: allow — <reason>` on the line immediately above it; the \
         reason is required.",
        report.join("\n")
    );
}

#[test]
fn versions_finds_whole_semver_tokens_only() {
    let found = |text: &str| -> Vec<String> {
        versions(text)
            .into_iter()
            .map(|(_, v)| v.to_owned())
            .collect()
    };
    assert_eq!(found("codex-cli 0.114.0\\n"), ["0.114.0"]);
    assert_eq!(found("1.0.128 (Claude Code)"), ["1.0.128"]);
    assert_eq!(found("codex-cli 0.159.1-beta.1."), ["0.159.1-beta.1"]);
    assert_eq!(found("v1.2.3 and 4.5.6"), ["4.5.6"]);
    for text in ["1.2", "1.2.3.4", "a1.2.3", "1.2.3x", "10.9", "x-1.2.3"] {
        assert!(found(text).is_empty(), "{text}");
    }
}

#[test]
fn providers_follow_the_cli_name_or_claude_suffix() {
    assert_eq!(providers_for("printf 'codex-cli ", ""), [CODEX]);
    assert_eq!(providers_for("\\ncodex ", ""), [CODEX]);
    assert_eq!(providers_for("printf 'claude ", ""), [CLAUDE]);
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
}
"##;
    let hits = scan_source(src);
    let summary: Vec<_> = hits
        .iter()
        .map(|h| {
            (
                h.line,
                h.provider.as_str(),
                h.output.as_str(),
                h.below.is_some(),
                h.marker_malformed,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (3, CODEX, "0.0.1", true, false),
            (4, CLAUDE, "0.0.1", true, false),
            (5, CLAUDE, "999.0.0", false, false),
            (9, "codex/claude-code", "0.0.1", true, true),
            (11, CLAUDE, "0.0.2", true, false),
        ]
    );
    assert_eq!(
        hits[0].below.as_deref(),
        Some(requirement(CODEX).unwrap().minimum.to_string().as_str())
    );
}
