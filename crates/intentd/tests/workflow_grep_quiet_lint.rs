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
//! naming `file:line: text` and the accepted rewrites, wherever a `|` (not
//! `||`; `|&` counts) is followed by the command `grep` whose arguments
//! include a quiet spelling — `-q`, a short-flag cluster containing `q`
//! (`-Eq`, `-qE`, `-Fxq`, …), `--quiet`, or `--silent`. The accepted
//! rewrites are:
//!
//! - variable input: drop the pipe — `grep -qE pattern <<<"$VAR"` (no
//!   producer process, so no SIGPIPE) or a bash pattern test;
//! - real producer: drain instead of quitting —
//!   `producer | grep -E pattern >/dev/null`;
//! - file input: `grep -q pattern file`.
//!
//! The scan is a small quote-aware shell lexer, not a parser: single and
//! double quotes group words (so `-e 'x|y' -q` is a hit and `-E 'has -q
//! word'` is not), an unquoted `#` at a word start drops the comment tail,
//! redirections (`2>/dev/null`, `2>&1`) are ordinary non-option words, `--`
//! ends option scanning, the argument of an option that takes one is not a
//! flag (`-e '-q'`, `--regexp -q`, `-eq`, `-m 1`), and only an unquoted
//! `|`, `;`, `&`, `&&`, `||`, `(` or `)` ends grep's argument list. `$(…)`
//! opens a fresh command
//! context even inside double quotes. Physical lines ending in `\` are
//! joined with the next before lexing; a hit is reported on the physical
//! line holding the `grep` word.
//!
//! Limits: a quiet grep reading a file or here-string with no `|` is not a
//! hit; only the bare word `grep` is recognised (not `command grep`,
//! `/usr/bin/grep`, an env-prefixed `LC_ALL=C grep`, or `xargs grep`);
//! here-docs and backtick substitutions are lexed as ordinary text.

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

/// Short grep options that take an argument: in a cluster, whatever follows
/// one of these letters is that argument (`-eq` is pattern `q`, not quiet).
const SHORT_WITH_ARG: [char; 8] = ['e', 'f', 'm', 'A', 'B', 'C', 'd', 'D'];

/// Whether a single shell token is a grep quiet flag in any spelling.
fn is_quiet_flag(token: &str) -> bool {
    if token == "--quiet" || token == "--silent" {
        return true;
    }
    match token.strip_prefix('-') {
        Some(rest) if !rest.starts_with('-') && !rest.is_empty() => rest
            .chars()
            .take_while(|c| c.is_ascii_alphabetic() && !SHORT_WITH_ARG.contains(c))
            .any(|c| c == 'q'),
        _ => false,
    }
}

/// One shell logical line: physical lines joined at `\`-newline, with the
/// 0-based physical line index of every character.
struct Logical {
    chars: Vec<char>,
    lines: Vec<usize>,
}

/// A one-line YAML `run: "…"` / `run: '…'` scalar, unwrapped to its body so
/// the shell inside is lexed instead of being one quoted word.
fn unwrap_quoted_run(line: &str) -> &str {
    let t = line.trim_start();
    let t = t.strip_prefix("- ").unwrap_or(t);
    let Some(rest) = t.strip_prefix("run:") else {
        return line;
    };
    let rest = rest.trim();
    rest.strip_prefix('"')
        .and_then(|r| r.strip_suffix('"'))
        .or_else(|| rest.strip_prefix('\'').and_then(|r| r.strip_suffix('\'')))
        .unwrap_or(line)
}

fn logical_lines(text: &str) -> Vec<Logical> {
    let mut out = Vec::new();
    let mut cur: Option<Logical> = None;
    for (i, line) in text.lines().enumerate() {
        let line = unwrap_quoted_run(line);
        let trailing = line.chars().rev().take_while(|&c| c == '\\').count();
        let continued = trailing % 2 == 1;
        let body = if continued {
            &line[..line.len() - 1]
        } else {
            line
        };
        let l = cur.get_or_insert_with(|| Logical {
            chars: Vec::new(),
            lines: Vec::new(),
        });
        l.chars.extend(body.chars());
        l.lines.resize(l.chars.len(), i);
        if !continued {
            out.extend(cur.take());
        }
    }
    out.extend(cur);
    out
}

#[derive(Debug, PartialEq)]
enum Tok {
    /// A shell word with quotes removed, and the physical line it starts on.
    Word { text: String, line: usize },
    /// `|` or `|&`.
    Pipe,
    /// `;`, `&`, `&&`, `||`, `(`, `)`, `$(` — ends a command's argument list.
    Boundary,
}

#[derive(Clone, Copy, PartialEq)]
enum Ctx {
    DoubleQuote,
    Subshell,
}

struct Lexer<'a> {
    l: &'a Logical,
    i: usize,
    stack: Vec<Ctx>,
    word: String,
    word_line: Option<usize>,
    toks: Vec<Tok>,
}

impl Lexer<'_> {
    fn at(&self, off: usize) -> Option<char> {
        self.l.chars.get(self.i + off).copied()
    }

    fn push_char(&mut self, c: char) {
        self.word_line.get_or_insert(self.l.lines[self.i]);
        self.word.push(c);
    }

    fn flush(&mut self) {
        if let Some(line) = self.word_line.take() {
            let text = std::mem::take(&mut self.word);
            self.toks.push(Tok::Word { text, line });
        }
    }

    fn boundary(&mut self, width: usize) {
        self.flush();
        self.toks.push(Tok::Boundary);
        self.i += width;
    }

    fn run(mut self) -> Vec<Tok> {
        let n = self.l.chars.len();
        while self.i < n {
            let c = self.l.chars[self.i];
            if self.stack.last() == Some(&Ctx::DoubleQuote) {
                match c {
                    '"' => {
                        self.word_line.get_or_insert(self.l.lines[self.i]);
                        self.stack.pop();
                        self.i += 1;
                    }
                    '\\' if self.i + 1 < n => {
                        self.push_char(self.l.chars[self.i + 1]);
                        self.i += 2;
                    }
                    '$' if self.at(1) == Some('(') => {
                        self.boundary(2);
                        self.stack.push(Ctx::Subshell);
                    }
                    _ => {
                        self.push_char(c);
                        self.i += 1;
                    }
                }
                continue;
            }
            match c {
                ' ' | '\t' => {
                    self.flush();
                    self.i += 1;
                }
                '#' if self.word_line.is_none() => {
                    let line = self.l.lines[self.i];
                    while self.i < n && self.l.lines[self.i] == line {
                        self.i += 1;
                    }
                }
                '\'' => {
                    self.word_line.get_or_insert(self.l.lines[self.i]);
                    self.i += 1;
                    while self.i < n && self.l.chars[self.i] != '\'' {
                        self.word.push(self.l.chars[self.i]);
                        self.i += 1;
                    }
                    self.i += 1;
                }
                '"' => {
                    self.word_line.get_or_insert(self.l.lines[self.i]);
                    self.stack.push(Ctx::DoubleQuote);
                    self.i += 1;
                }
                '\\' if self.i + 1 < n => {
                    self.push_char(self.l.chars[self.i + 1]);
                    self.i += 2;
                }
                '$' if self.at(1) == Some('(') => {
                    self.boundary(2);
                    self.stack.push(Ctx::Subshell);
                }
                '>' | '<' => {
                    let fd_prefix = self.word.chars().all(|c| c.is_ascii_digit());
                    if !self.word.is_empty() && !fd_prefix {
                        self.flush();
                    }
                    self.push_char(c);
                    self.i += 1;
                }
                '(' | ';' => self.boundary(1),
                ')' => {
                    self.boundary(1);
                    if self.stack.last() == Some(&Ctx::Subshell) {
                        self.stack.pop();
                    }
                }
                '|' => {
                    self.flush();
                    match self.at(1) {
                        Some('|') => {
                            self.toks.push(Tok::Boundary);
                            self.i += 2;
                        }
                        Some('&') => {
                            self.toks.push(Tok::Pipe);
                            self.i += 2;
                        }
                        _ => {
                            self.toks.push(Tok::Pipe);
                            self.i += 1;
                        }
                    }
                }
                '&' => {
                    let redirection = self.word.ends_with(['>', '<']) || self.at(1) == Some('>');
                    if redirection {
                        self.push_char(c);
                        self.i += 1;
                    } else if self.at(1) == Some('&') {
                        self.boundary(2);
                    } else {
                        self.boundary(1);
                    }
                }
                _ => {
                    self.push_char(c);
                    self.i += 1;
                }
            }
        }
        self.flush();
        self.toks
    }
}

fn tokenize(l: &Logical) -> Vec<Tok> {
    Lexer {
        l,
        i: 0,
        stack: Vec::new(),
        word: String::new(),
        word_line: None,
        toks: Vec::new(),
    }
    .run()
}

/// Physical line indices (0-based) of every `grep` word that follows a pipe
/// and carries a quiet flag among its arguments.
fn quiet_grep_lines(toks: &[Tok]) -> Vec<usize> {
    let mut lines = Vec::new();
    for (i, tok) in toks.iter().enumerate() {
        if *tok != Tok::Pipe {
            continue;
        }
        let Some(Tok::Word { text, line }) = toks.get(i + 1) else {
            continue;
        };
        if text != "grep" {
            continue;
        }
        let args = toks[i + 2..]
            .iter()
            .map_while(|t| match t {
                Tok::Word { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .take_while(|arg| *arg != "--");
        let mut skip_next = false;
        for arg in args {
            if std::mem::take(&mut skip_next) {
                continue;
            }
            if is_quiet_flag(arg) {
                lines.push(*line);
                break;
            }
            skip_next = takes_separate_argument(arg);
        }
    }
    lines
}

/// Whether a grep option consumes the following word as its argument
/// (`-e PATTERN`, `-f FILE`, `-m NUM`, `--regexp PATTERN`, …), so that word
/// must not be read as a flag. A cluster whose argument letter is not last
/// (`-eq`) already carries its argument inline.
fn takes_separate_argument(arg: &str) -> bool {
    const LONG: [&str; 14] = [
        "--regexp",
        "--file",
        "--max-count",
        "--after-context",
        "--before-context",
        "--context",
        "--directories",
        "--devices",
        "--exclude",
        "--exclude-dir",
        "--exclude-from",
        "--include",
        "--label",
        "--group-separator",
    ];
    if LONG.contains(&arg) {
        return true;
    }
    match arg.strip_prefix('-') {
        Some(rest) if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_alphabetic()) => {
            rest.chars().position(|c| SHORT_WITH_ARG.contains(&c)) == Some(rest.len() - 1)
        }
        _ => false,
    }
}

/// Every hit in `text`, as `<name>:<line>: <trimmed text>`; the line is the
/// physical line holding the `grep` word.
fn hits_in(name: &str, text: &str) -> Vec<String> {
    let physical: Vec<&str> = text.lines().collect();
    logical_lines(text)
        .iter()
        .flat_map(|l| quiet_grep_lines(&tokenize(l)))
        .map(|line| format!("{name}:{}: {}", line + 1, physical[line].trim()))
        .collect()
}

/// Whether one line of workflow text pipes into a quiet grep.
fn line_is_quiet_grep_pipeline(line: &str) -> bool {
    !hits_in("", line).is_empty()
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
            r#"          x="$(cat f | grep -q beta.json)""#,
            r"          printf x | grep -e 'x|y' -q",
            r#"          printf x | grep -e "x|y" -q"#,
            r"          printf x | grep 2>/dev/null -q x",
            r"          printf x | grep >/dev/null -q x",
            r"          printf x | grep 2>&1 -q x",
            r"          printf x | grep -q x # comment",
            r"          printf x | grep -q x 2>/dev/null; echo done",
            r"          printf x | grep -q -- -x",
            r"          printf x | grep x -q>/dev/null",
            r"          printf x | grep x -q;",
            r"          printf x | grep x -q 2>&1",
            r"          printf x | grep x -q&& echo yes",
            r"          printf x | grep -e q -q",
            r"          printf x | grep -m 1 -q x",
            r"          printf x | grep --regexp -q --quiet",
            r"          printf x | grep -qe x",
            r"          (printf x | grep -q x)",
            r#"        run: "printf x | grep -q x""#,
            r"        run: 'printf x | grep -q x'",
        ] {
            assert!(line_is_quiet_grep_pipeline(line), "expected hit: {line}");
        }
    }

    #[test]
    fn backslash_continued_pipeline_is_a_hit_on_the_grep_line() {
        let text = "      - run: |\n          producer \\\n            | grep -q x\n          producer | \\\n            grep -q y\n          producer | grep -E z \\\n            >/dev/null\n";
        assert_eq!(
            hits_in("x.yml", text),
            vec![
                "x.yml:3: | grep -q x".to_string(),
                "x.yml:5: grep -q y".to_string(),
            ]
        );
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
            r"          printf x | grep -E 'has -q word' >/dev/null",
            r#"          printf x | grep -E "has -q word" >/dev/null"#,
            r"          printf x | grep -E has\ -q\ word >/dev/null",
            r"          printf x | grep x # no -q here",
            r"          printf x | grep x >/dev/null # was: | grep -q x",
            r"          printf x | grep x; grep -q y file",
            r"          printf x | grep x && grep -q y file",
            r"          printf x | grep x & grep -q y file",
            r"          printf x | grep x -- -q",
            r"          printf x | grep -E pat >/dev/null; x=$(grep -q y file)",
            r"          printf x | grep x >'/dev/null' -- -q",
            r"          printf x | grep x>/dev/null",
            r"          printf x | grep x 2>/dev/null",
            r"          printf '%s\n' -q | grep -e '-q' >/dev/null",
            r"          printf '%s\n' --quiet | grep -e --quiet >/dev/null",
            r"          printf x | grep --regexp -q >/dev/null",
            r"          printf x | grep -eq x >/dev/null",
            r"          printf x | grep -Eeq x >/dev/null",
            r"          printf x | grep -f -q >/dev/null",
            r"          printf x | grep -m 1 -e -q >/dev/null",
            r#"          echo "a | grep -q b""#,
            r"          echo 'a | grep -q b'",
            r"          # comment \",
            r"          printf '%s#%s' a b | grep -E pat >/dev/null",
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
