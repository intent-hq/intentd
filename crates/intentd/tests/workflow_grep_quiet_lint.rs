//! Source lint: workflow shell must not pipe into a quiet `grep`.
//!
//! GitHub Actions runs `shell: bash` steps under `bash -eo pipefail`. In a
//! `producer | grep -q pattern` pipeline grep exits at the first match, the
//! producer takes SIGPIPE while still writing, and `pipefail` reports the
//! producer's failure — so a large **matched** input makes the step conclude
//! "no match". cloudlands-fe PR #2709 shipped a relevance check built this
//! way that reported "not relevant" on a 98 KB matched diff
//! (<https://github.com/intent-hq/cloudlands-fe/pull/2709#discussion_r4057405412>).
//!
//! This test scans every `.github/workflows/*.yml` / `*.yaml` and fails,
//! naming `file:line: text` and the accepted rewrites, on any non-comment
//! line where a `|` (not `||`; `|&` counts) is followed by `grep` whose
//! flags include a quiet spelling — `-q`, a short-flag cluster containing
//! `q` (`-Eq`, `-qE`, `-Fxq`, …), `--quiet`, or `--silent`. The accepted
//! rewrites are:
//!
//! - variable input: drop the pipe — `grep -qE pattern <<<"$VAR"` (no
//!   producer process, so no SIGPIPE) or a bash pattern test;
//! - real producer: drain instead of quitting —
//!   `producer | grep -E pattern >/dev/null`;
//! - file input: `grep -q pattern file`.
//!
//! Limits: this is a bounded textual check (no shell parser). A quiet grep
//! reading a file or here-string on a line with no `|` is not a hit; a
//! pipeline whose `|` ends one line and whose `grep` starts the next is not
//! detected; a pipe inside a quoted string is treated like any other pipe.

use std::fs;
use std::path::{Path, PathBuf};

const WORKFLOWS_DIR: &str = ".github/workflows";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize workspace root")
}

fn workflow_files(root: &Path) -> Vec<PathBuf> {
    let dir = root.join(WORKFLOWS_DIR);
    let entries = fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "yml" || e == "yaml"))
        .collect();
    files.sort();
    files
}

/// Whether a single shell token is a grep quiet flag in any spelling.
fn is_quiet_flag(token: &str) -> bool {
    if token == "--quiet" || token == "--silent" {
        return true;
    }
    match token.strip_prefix('-') {
        Some(rest) if !rest.starts_with('-') && !rest.is_empty() => {
            rest.chars().all(|c| c.is_ascii_alphabetic()) && rest.contains('q')
        }
        _ => false,
    }
}

/// Whether `segment` (the text after a pipe) starts with a `grep` command
/// carrying a quiet flag before the segment's own command boundary.
fn segment_is_quiet_grep(segment: &str) -> bool {
    let segment = segment.trim_start_matches('&').trim_start();
    let Some(rest) = segment.strip_prefix("grep") else {
        return false;
    };
    if !rest.starts_with(char::is_whitespace) {
        return false;
    }
    let end = rest
        .find(['|', ';', '&', ')', '>', '<'])
        .unwrap_or(rest.len());
    rest[..end].split_whitespace().any(is_quiet_flag)
}

/// Whether one line of workflow text pipes into a quiet grep.
fn line_is_quiet_grep_pipeline(line: &str) -> bool {
    let trimmed = line.trim_start();
    if trimmed.starts_with('#') {
        return false;
    }
    let bytes = line.as_bytes();
    line.match_indices('|').any(|(i, _)| {
        let prev_is_pipe = i > 0 && bytes[i - 1] == b'|';
        let next_is_pipe = bytes.get(i + 1) == Some(&b'|');
        !prev_is_pipe && !next_is_pipe && segment_is_quiet_grep(&line[i + 1..])
    })
}

/// Every hit in `text`, as `<name>:<line>: <trimmed text>`.
fn hits_in(name: &str, text: &str) -> Vec<String> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| line_is_quiet_grep_pipeline(line))
        .map(|(i, line)| format!("{name}:{}: {}", i + 1, line.trim()))
        .collect()
}

fn scan(root: &Path) -> Vec<String> {
    let mut hits = Vec::new();
    for file in workflow_files(root) {
        let text = fs::read_to_string(&file).expect("read workflow");
        let rel = file.strip_prefix(root).unwrap_or(&file);
        hits.extend(hits_in(&rel.to_string_lossy(), &text));
    }
    hits
}

#[test]
fn workflows_do_not_pipe_into_quiet_grep() {
    let root = workspace_root();
    let files = workflow_files(&root);
    assert!(
        !files.is_empty(),
        "no workflow files found under {}/{WORKFLOWS_DIR} — discovery is broken",
        root.display()
    );
    let hits = scan(&root);
    assert!(
        hits.is_empty(),
        "\n\n`producer | grep -q` breaks under `bash -eo pipefail`: grep quits at the first \
         match, the producer takes SIGPIPE, and a large matched input reports failure \
         (cloudlands-fe#2709). {} site{}:\n  {}\n\nRewrite as one of:\n  \
         - variable input: `grep -qE pattern <<<\"$VAR\"` (here-string, no pipe) or a bash \
         pattern test\n  \
         - real producer: `producer | grep -E pattern >/dev/null` (drain, do not quit)\n  \
         - file input: `grep -q pattern file`\n",
        hits.len(),
        if hits.len() == 1 { "" } else { "s" },
        hits.join("\n  "),
    );
}

#[cfg(test)]
mod fixture {
    use super::{hits_in, line_is_quiet_grep_pipeline};

    #[test]
    fn quiet_flag_spellings_after_a_pipe_are_hits() {
        for line in [
            r"          printf x | grep -q x",
            r#"          echo "$OUTPUT" | grep -q "Cannot find package 'svelte'""#,
            r#"          printf '%s' "$BASE" | grep -Eq '^v?[0-9]+'"#,
            r#"          printf '%s' "$BASE" | grep -qE '^v?[0-9]+'"#,
            r"          cat f | grep -Fxq beta.json",
            r"          cat f | grep -i -q beta.json",
            r"          cat f | grep --quiet beta.json",
            r"          cat f | grep --silent beta.json",
            r"          cat f|grep -q beta.json",
            r"          cat f |grep -q beta.json",
            r"          cat f | grep -q beta.json && echo yes",
            r#"          if xcrun simctl list runtimes 2>/dev/null | grep -q "^iOS "; then"#,
            r#"          dpkg-deb --contents "$deb" | grep -q ' \./usr/bin/intentd$'"#,
            r"          cmd 2>&1 | grep -q err",
            r"          cmd |& grep -q err",
            r"            | grep -q err",
            r"          x=$(cat f | grep -q beta.json)",
        ] {
            assert!(line_is_quiet_grep_pipeline(line), "expected hit: {line}");
        }
    }

    #[test]
    fn non_pipeline_quiet_greps_and_drained_pipes_are_not_hits() {
        for line in [
            r"          grep -q pattern file",
            r#"          grep -Fxq "beta.json" "$tmpdir/assets.txt""#,
            r#"          grep -qE pattern <<<"$VAR""#,
            r#"          if ! grep -qE pattern <<<"$VAR"; then"#,
            r"          producer | grep -E pattern >/dev/null",
            r"          producer | grep pattern >/dev/null",
            r"          producer | grep -c pattern",
            r"          producer | grep -E pattern | tail -1",
            r"          test -f x || grep -q pattern file",
            r"          a || grep -q pattern file",
            r"          # cat f | grep -q beta.json",
            r"            # producer | grep --quiet x",
            r"        run: |",
            r"          producer | grepper -q x",
            r"          producer | egrep -q x",
            r"          producer | grep pat -1",
            r"          producer | grep -E pat >/dev/null || grep -q pat file",
        ] {
            assert!(!line_is_quiet_grep_pipeline(line), "unexpected hit: {line}");
        }
    }

    #[test]
    fn hits_are_reported_as_file_line_and_trimmed_text() {
        let text = "jobs:\n  a:\n    steps:\n      - run: |\n          ok=1\n          printf x | grep -q x\n          # printf x | grep -q x\n          echo \"$v\" | grep --quiet y\n";
        assert_eq!(
            hits_in(".github/workflows/x.yml", text),
            vec![
                ".github/workflows/x.yml:6: printf x | grep -q x".to_string(),
                ".github/workflows/x.yml:8: echo \"$v\" | grep --quiet y".to_string(),
            ]
        );
    }

    #[test]
    fn clean_text_has_no_hits() {
        let text = "      - run: |\n          producer | grep -E pat >/dev/null\n          grep -q pat file\n          grep -qE pat <<<\"$VAR\"\n";
        assert_eq!(hits_in("x.yml", text), Vec::<String>::new());
    }
}
