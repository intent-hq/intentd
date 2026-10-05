//! Running-turn status rule lint.
//!
//! "The agent is running a turn" means `AgentStatus::Pending | Active |
//! Processing`, and before intent-hq/intentd#2058 that rule had drifted into
//! four hand-written copies (two free `is_running_turn` fns in
//! intent-services, an inline match in the transfer export, and a literal
//! SQL `IN` list in intent-store). #2058 consolidated them onto
//! `AgentStatus::is_running_turn` in `crates/intent-core/src/model.rs`;
//! nothing else stops a fifth copy. This source-scanning test fails, naming
//! `file:line`, whenever an or-pattern anywhere under `crates/*/src/**/*.rs`
//! other than `model.rs` spells out exactly that variant set again (the Rust
//! rule), or a string literal under `crates/intent-store/src/**/*.rs` spells
//! the set as a SQL list again (the SQL rule).
//!
//! The pre-processing — blanking comments, string literals, and `#[cfg(test)]`
//! items, keeping every string literal's body with its line for the SQL rule,
//! cutting the text into statements (a block expression used as an operand —
//! its `}` followed by `.`, `?`, or `else` — chains with the text around it
//! while the block bodies stay their own statements), locating the opt-out
//! marker, and walking `crates/*/src/**/*.rs` minus `tests/` directories and
//! `tests.rs` files — is the shared `intentd_test_support::source_lint`
//! scaffolding; its module doc spells out those semantics. The rules
//! themselves are deliberately small:
//!
//! - Skipped: `crates/intent-core/src/model.rs` (the definition), on top of
//!   the test code the shared walker and `#[cfg(test)]` blanking leave out.
//! - An **or-group** is a maximal run of path alternatives (`A::B::C`)
//!   joined by single `|` inside one statement — `matches!(s, A | B | C)`,
//!   an `A | B | C =>` match arm, any order, across lines. An alternative
//!   may carry `&` / `&mut` prefixes and wrapping parentheses
//!   (`&A | &B | &C`, `(A) | (B) | (C)`): they are transparent, the group's
//!   variant set comes from the paths alone. `||` ends a group.
//! - A group is a **hit** when the set of variant names it spells as
//!   `AgentStatus::<V>` (any path prefix, e.g. `intent_core::AgentStatus::<V>`)
//!   or `Self::<V>` is exactly `{Pending, Active, Processing}`. Supersets
//!   (`… | Waiting`) and subsets (`Active | Processing`) are distinct rules
//!   and are not hits.
//! - Opt-out: `// running-turn: allow — <reason>` on the line immediately
//!   above the group's first line, or above the statement's first line (the
//!   `matches!(` line for a multi-line `matches!`), in the shared marker
//!   grammar (a standalone `//` line comment, the exact token, whitespace, an
//!   em dash or hyphen, and a nonempty reason). A malformed marker never
//!   suppresses the hit; the report says so.
//! - **SQL rule**: every string literal the lexer collected — cooked or raw,
//!   a `format!` template included — in non-test code under
//!   `crates/intent-store/src/**/*.rs` (same `tests/` / `tests.rs` /
//!   `#[cfg(test)]` skipping) is a hit when its body, as written, contains
//!   both `'active'` and `'Processing'` (case-sensitive: the lowercase /
//!   legacy-capitalized pair is the fingerprint of the running set as the
//!   persisted serde names). The hit is reported at the literal's line
//!   carrying the first of the two words; the opt-out marker goes on the
//!   line above the literal's opening line or above its statement's first
//!   line (a literal is never split across statements, so the statement is
//!   the text back to the previous `;` / `{` / `}`).
//!
//! Limits: bare imported variants (`use AgentStatus::*; Pending | Active |
//! Processing`) are not recognized, nor are variants nested in another
//! constructor (`Some(AgentStatus::Pending) | Some(…) | Some(…)` — each
//! `Some` is its own one-path group), a `Self::` triple on some other enum
//! with the same variant names is a false positive (opt out with a reason),
//! a test module file that is not named `tests.rs` and not under a `tests/`
//! directory is scanned like production code unless its item is
//! `#[cfg(test)]`, and a SQL list assembled from several literals (`"IN
//! ('active', "` + `"'Processing')"`) or one that omits `'active'` is not
//! recognized.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use intentd_test_support::source_lint::{
    blank_cfg_test_items, cfg_test_item_ranges, crate_src_files, lex, markers_by_line,
    split_statements, statement_line, word_at, workspace_root, Marker,
};

const RUNNING_TURN_VARIANTS: &[&str] = &["Pending", "Active", "Processing"];
const VARIANT_OWNERS: &[&str] = &["AgentStatus", "Self"];
const OPT_OUT_TAG: &str = "running-turn";
const EXEMPT_FILE: &[&str] = &["crates", "intent-core", "src", "model.rs"];
/// Both must appear in one literal for the SQL rule to fire.
const SQL_RUNNING_FINGERPRINT: &[&str] = &["'active'", "'Processing'"];
const SQL_LITERAL_DIR: &[&str] = &["crates", "intent-store", "src"];
const EXCERPT_CHARS: usize = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    excerpt: String,
    /// A line checked for the opt-out carried something that starts like the
    /// marker but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

/// The tokens an or-group is made of. Everything that is neither a path, a
/// `|`, nor one of the alternative wrappers (`&`, `(`, `)`) is `Other` and
/// ends any group in progress.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// `A::B::C` (whitespace allowed around `::`), as its segments.
    Path(Vec<String>),
    /// A single `|`; `||` is `Other`.
    Pipe,
    /// A single `&`; `&&` is `Other`.
    Amp,
    LParen,
    RParen,
    Other,
}

/// Tokens of one statement's text with the 1-based line each starts on
/// (`first_line` is the line of the statement's first non-whitespace
/// character, as `split_statements` records it).
fn tokenize(text: &str, first_line: usize) -> Vec<(Token, usize)> {
    let chars: Vec<char> = text.trim_start().chars().collect();
    let mut out = Vec::new();
    let mut line = first_line;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            line += 1;
            i += 1;
        } else if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start_line = line;
            let mut segments = Vec::new();
            loop {
                let (word, next) = word_at(&chars, i).expect("identifier start");
                segments.push(word);
                i = next;
                let mut j = i;
                let mut newlines = 0usize;
                while chars.get(j).is_some_and(|c| c.is_whitespace()) {
                    newlines += usize::from(chars[j] == '\n');
                    j += 1;
                }
                if chars.get(j) == Some(&':') && chars.get(j + 1) == Some(&':') {
                    j += 2;
                    let mut k = j;
                    while chars.get(k).is_some_and(|c| c.is_whitespace()) {
                        newlines += usize::from(chars[k] == '\n');
                        k += 1;
                    }
                    if chars
                        .get(k)
                        .is_some_and(|c| c.is_ascii_alphabetic() || *c == '_')
                    {
                        line += newlines;
                        i = k;
                        continue;
                    }
                }
                break;
            }
            out.push((Token::Path(segments), start_line));
        } else if c.is_ascii_digit() {
            while chars
                .get(i)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            {
                i += 1;
            }
            out.push((Token::Other, line));
        } else if c == '|' || c == '&' {
            if chars.get(i + 1) == Some(&c) {
                out.push((Token::Other, line));
                i += 2;
            } else {
                let token = if c == '|' { Token::Pipe } else { Token::Amp };
                out.push((token, line));
                i += 1;
            }
        } else {
            let token = match c {
                '(' => Token::LParen,
                ')' => Token::RParen,
                _ => Token::Other,
            };
            out.push((token, line));
            i += 1;
        }
    }
    out
}

/// A maximal run of path tokens joined by single `|` inside one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrGroup {
    line: usize,
    paths: Vec<Vec<String>>,
}

impl OrGroup {
    fn text(&self) -> String {
        self.paths
            .iter()
            .map(|p| p.join("::"))
            .collect::<Vec<_>>()
            .join(" | ")
    }

    /// Variant names the group spells as `<owner>::<Variant>` with an owner in
    /// `VARIANT_OWNERS`, whatever precedes the owner (`intent_core::AgentStatus::Active`).
    fn variants(&self) -> BTreeSet<&str> {
        self.paths
            .iter()
            .filter(|p| p.len() >= 2 && VARIANT_OWNERS.contains(&p[p.len() - 2].as_str()))
            .map(|p| p[p.len() - 1].as_str())
            .collect()
    }
}

/// One or-alternative starting at `tokens[i]`: a path, optionally preceded
/// by any mix of `&` / `&mut` and `(` and followed by exactly as many `)`
/// as `(` were opened. Returns the path, its line, and the index after the
/// alternative; `None` when the tokens at `i` do not form one (so a `(`
/// that wraps a whole group, `(A | B)`, is not an alternative wrapper).
fn alternative_at(tokens: &[(Token, usize)], i: usize) -> Option<(Vec<String>, usize, usize)> {
    let mut j = i;
    let mut parens = 0usize;
    let (path, line) = loop {
        match tokens.get(j)? {
            (Token::LParen, _) => {
                parens += 1;
                j += 1;
            }
            (Token::Amp, _) => {
                j += 1;
                if matches!(tokens.get(j), Some((Token::Path(p), _)) if p == &["mut"]) {
                    j += 1;
                }
            }
            (Token::Path(p), line) => break (p.clone(), *line),
            _ => return None,
        }
    };
    j += 1;
    for _ in 0..parens {
        if !matches!(tokens.get(j), Some((Token::RParen, _))) {
            return None;
        }
        j += 1;
    }
    Some((path, line, j))
}

fn or_groups(tokens: &[(Token, usize)]) -> Vec<OrGroup> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let Some((first, line, next)) = alternative_at(tokens, i) else {
            i += 1;
            continue;
        };
        let mut paths = vec![first];
        i = next;
        while tokens.get(i).is_some_and(|(t, _)| *t == Token::Pipe) {
            match alternative_at(tokens, i + 1) {
                Some((p, _, next)) => {
                    paths.push(p);
                    i = next;
                }
                None => break,
            }
        }
        if paths.len() > 1 {
            out.push(OrGroup { line, paths });
        }
    }
    out
}

/// Whether the group spells exactly the running-turn variant set.
fn is_running_turn_triple(group: &OrGroup) -> bool {
    let want: BTreeSet<&str> = RUNNING_TURN_VARIANTS.iter().copied().collect();
    group.variants() == want
}

fn excerpt(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(EXCERPT_CHARS).collect();
    if out.len() < collapsed.len() {
        out.push('…');
    }
    out
}

/// Scans one Rust source file's text and returns every or-group spelling the
/// running-turn triple that is not suppressed by a reasoned opt-out marker on
/// the line above the group or above its statement.
fn scan_source(src: &str) -> Vec<Hit> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let blanked = blank_cfg_test_items(&lexed.blanked);
    let marker_at = |line: usize| markers.get(line - 1).copied().unwrap_or(Marker::Absent);
    let mut hits = Vec::new();
    for statement in split_statements(&blanked) {
        for group in or_groups(&tokenize(&statement.text, statement.line)) {
            if !is_running_turn_triple(&group) {
                continue;
            }
            let states = [marker_at(group.line), marker_at(statement.line)];
            if states.contains(&Marker::WithReason) {
                continue;
            }
            hits.push(Hit {
                line: group.line,
                excerpt: excerpt(&group.text()),
                marker_malformed: states.contains(&Marker::Malformed),
            });
        }
    }
    hits
}

/// `(line offset within the literal, that line's text)` for the first
/// occurrence of any fingerprint word in a literal body.
fn sql_fingerprint_line(text: &str) -> (usize, &str) {
    let idx = SQL_RUNNING_FINGERPRINT
        .iter()
        .filter_map(|word| text.find(word))
        .min()
        .expect("caller checked the fingerprint");
    let offset = text[..idx].matches('\n').count();
    (offset, text.lines().nth(offset).unwrap_or(""))
}

/// Scans one Rust source file's text and returns every string literal in
/// non-test code whose body carries the SQL running-set fingerprint and is not
/// suppressed by a reasoned opt-out marker above the literal or its statement.
fn scan_sql_literals(src: &str) -> Vec<Hit> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let blanked: Vec<char> = lexed.blanked.chars().collect();
    let test_ranges = cfg_test_item_ranges(&blanked);
    let marker_at = |line: usize| markers.get(line - 1).copied().unwrap_or(Marker::Absent);
    let mut hits = Vec::new();
    for literal in &lexed.literals {
        if !SQL_RUNNING_FINGERPRINT
            .iter()
            .all(|word| literal.text.contains(word))
        {
            continue;
        }
        if test_ranges
            .iter()
            .any(|&(start, end)| (start..end).contains(&literal.offset))
        {
            continue;
        }
        let states = [
            marker_at(literal.line),
            marker_at(statement_line(&blanked, literal.offset)),
        ];
        if states.contains(&Marker::WithReason) {
            continue;
        }
        let (line_offset, line_text) = sql_fingerprint_line(&literal.text);
        hits.push(Hit {
            line: literal.line + line_offset,
            excerpt: excerpt(line_text),
            marker_malformed: states.contains(&Marker::Malformed),
        });
    }
    hits
}

fn is_exempt(rel: &Path) -> bool {
    let parts: Vec<_> = rel.components().map(Component::as_os_str).collect();
    parts.len() == EXEMPT_FILE.len()
        && parts
            .iter()
            .zip(EXEMPT_FILE)
            .all(|(have, want)| *have == *want)
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn running_turn_rule_is_only_spelled_in_agent_status() {
    let root = workspace_root();
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    let exempt_src = fs::read_to_string(root.join(&exempt)).unwrap_or_else(|e| {
        panic!(
            "{} moved ({e}); update EXEMPT_FILE so the exemption keeps pointing at AgentStatus",
            display_rel(&exempt)
        )
    });
    assert!(
        !scan_source(&exempt_src).is_empty(),
        "{} no longer spells the running-turn triple as an or-pattern; the exemption and \
         the rule's variant set need revisiting",
        display_rel(&exempt)
    );

    let files = crate_src_files(&root);
    assert!(
        !files.is_empty(),
        "no Rust sources found under {}",
        root.join("crates").display()
    );

    let mut report = Vec::new();
    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        if is_exempt(rel) {
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        for hit in scan_source(&src) {
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// running-turn: allow — <reason>`)"
            } else {
                ""
            };
            report.push(format!(
                "{}:{}: {}{note}",
                display_rel(rel),
                hit.line,
                hit.excerpt
            ));
        }
    }

    assert!(
        report.is_empty(),
        "the running-turn status rule is spelled out again outside AgentStatus:\n\n{}\n\n\
         Call `AgentStatus::is_running_turn()` (crates/intent-core/src/model.rs) instead of \
         matching `Pending | Active | Processing` by hand; that method is the single source \
         for \"the agent is running a turn\" (intent-hq/intentd#2058). A site that genuinely \
         needs the same variant set for a different rule may opt out with \
         `// running-turn: allow — <reason>` on the line immediately above the pattern or \
         its statement; the reason is required.",
        report.join("\n")
    );
}

/// Sorted non-test sources of the SQL rule's scope, `crates/intent-store/src`.
fn sql_literal_sources(root: &Path) -> Vec<PathBuf> {
    let dir: PathBuf = SQL_LITERAL_DIR.iter().collect();
    let files: Vec<PathBuf> = crate_src_files(root)
        .into_iter()
        .filter(|file| {
            file.strip_prefix(root)
                .is_ok_and(|rel| rel.starts_with(&dir))
        })
        .collect();
    assert!(
        !files.is_empty(),
        "no Rust sources found under {}; update SQL_LITERAL_DIR",
        display_rel(&dir)
    );
    files
}

#[test]
fn running_turn_sql_list_is_generated_not_spelled_in_store() {
    let root = workspace_root();
    let mut report = Vec::new();
    for file in sql_literal_sources(&root) {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        let src =
            fs::read_to_string(&file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        for hit in scan_sql_literals(&src) {
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// running-turn: allow — <reason>`)"
            } else {
                ""
            };
            report.push(format!(
                "{}:{}: {}{note}",
                display_rel(rel),
                hit.line,
                hit.excerpt
            ));
        }
    }

    assert!(
        report.is_empty(),
        "the running-turn status set is spelled out as a SQL literal in intent-store:\n\n{}\n\n\
         Generate the status list from `AgentStatus::ALL.iter().filter(|s| s.is_running_turn())` \
         as `delegated_counts_sql` (crates/intent-store/src/agent_repo.rs) does, so the store \
         cannot drift from `AgentStatus::is_running_turn`, the single source for \"the agent \
         is running a turn\" (intent-hq/intentd#2058). A literal that genuinely needs both \
         `'active'` and `'Processing'` for a different rule may opt out with \
         `// running-turn: allow — <reason>` on the line immediately above the literal or \
         its statement; the reason is required.",
        report.join("\n")
    );
}

// ---- scanner fixtures -------------------------------------------------------

/// 1-based line of the first line containing `needle`.
fn line_of(src: &str, needle: &str) -> usize {
    src.lines()
        .position(|l| l.contains(needle))
        .map_or_else(|| panic!("fixture lacks {needle:?}"), |i| i + 1)
}

fn hit_lines(src: &str) -> Vec<usize> {
    scan_source(src).into_iter().map(|h| h.line).collect()
}

/// The retire-guard copy in `crates/intent-services/src/agent_ops.rs` as it
/// stood before intentd commit e6703788 (#2058).
const AGENT_OPS_IS_RUNNING_TURN_PRE_2058: &str = r#"
/// "Running a turn" statuses for the retire guard (§5.5, confirmed
/// decision): a descendant in `pending`/`active`/`Processing` blocks
/// `ws.agent.retire`; idle/waiting/settled children are cascade-retired.
fn is_running_turn(status: AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
"#;

/// The lock-scope copy in `crates/intent-services/src/agent_locks.rs` before
/// #2058 (the second free fn).
const AGENT_LOCKS_IS_RUNNING_TURN_PRE_2058: &str = r"
/// Whether `status` means the session is mid-turn (parity with the retire
/// guard): `pending` / `active` / legacy `Processing`.
fn is_running_turn(status: AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
";

/// The transfer export warning in `crates/intent-services/src/transfer.rs`
/// before #2058: inline, inside a closure block, in a different order.
const TRANSFER_RUNNING_PRE_2058: &str = r"
        let running = sessions
            .iter()
            .filter(|s| {
                s.is_active
                    || matches!(
                        s.status,
                        AgentStatus::Active | AgentStatus::Pending | AgentStatus::Processing
                    )
            })
            .count();
";

#[test]
fn pre_2058_copies_are_flagged() {
    let src = AGENT_OPS_IS_RUNNING_TURN_PRE_2058;
    let hits = scan_source(src);
    assert_eq!(
        hits,
        vec![Hit {
            line: line_of(src, "AgentStatus::Pending | AgentStatus::Active"),
            excerpt: "AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing".into(),
            marker_malformed: false,
        }]
    );

    let src = AGENT_LOCKS_IS_RUNNING_TURN_PRE_2058;
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "AgentStatus::Pending | AgentStatus::Active")]
    );

    let src = TRANSFER_RUNNING_PRE_2058;
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "AgentStatus::Active | AgentStatus::Pending")]
    );
}

#[test]
fn text_after_an_operand_block_keeps_its_source_line() {
    let src = r"
fn f(s: AgentStatus) -> bool {
    matches!(
        {
            s
        }.clone(),
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
fn g(s: AgentStatus) -> bool {
    let s = if s.is_active() {
        s
    } else {
        s.parent()
    }.status();
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}
";
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "        AgentStatus::Pending | AgentStatus::Active"),
            line_of(src, "    matches!(s, AgentStatus::Pending"),
        ]
    );

    // A reasoned marker above the triple suppresses at that (correct) line.
    let src = r"
fn f(s: AgentStatus) -> bool {
    matches!(
        {
            s
        }.clone(),
        // running-turn: allow — fixture
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
";
    assert!(hit_lines(src).is_empty(), "{:?}", scan_source(src));
}

#[test]
fn match_arms_and_self_paths_are_recognized_across_lines_in_any_order() {
    let src = r"
impl Foo {
    fn running(self) -> bool {
        match self.status {
            AgentStatus::Processing
            | AgentStatus::Pending
            | AgentStatus::Active => true,
            _ => false,
        }
    }
    fn running_self(self) -> bool {
        matches!(self, Self::Pending | Self::Active | Self::Processing)
    }
    fn running_qualified(s: intent_core::AgentStatus) -> bool {
        matches!(s, intent_core::AgentStatus::Pending | intent_core::AgentStatus::Active | intent_core::AgentStatus::Processing)
    }
    fn nested(s: Option<AgentStatus>) -> bool {
        matches!(s, Some(AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing))
    }
}
";
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "            AgentStatus::Processing"),
            line_of(src, "Self::Pending | Self::Active"),
            line_of(src, "intent_core::AgentStatus::Pending |"),
            line_of(src, "Some(AgentStatus::Pending"),
        ]
    );
}

#[test]
fn supersets_subsets_and_other_enums_are_not_hits() {
    let src = r"
fn f(s: AgentStatus, t: Other) -> bool {
    let a = matches!(s, AgentStatus::Pending | AgentStatus::Active);
    let b = matches!(s, AgentStatus::Active | AgentStatus::Processing);
    let c = matches!(
        s,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
    );
    let d = matches!(t, Other::Pending | Other::Active | Other::Processing);
    let e = matches!(s, AgentStatus::Pending) || matches!(s, AgentStatus::Active) || matches!(s, AgentStatus::Processing);
    let f = s == AgentStatus::Pending || s == AgentStatus::Active || s == AgentStatus::Processing;
    let g = s.is_running_turn();
    a || b || c || d || e || f || g
}

// lib.rs / transfer_export.rs: the live-agent set.
fn is_live(s: AgentStatus) -> bool {
    matches!(
        s,
        AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
    )
}

// bindings/workspace.rs: the archive gate.
fn blocks_archive(s: AgentStatus) -> bool {
    matches!(
        s,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
    )
}

// agent_ops.rs: the wake-reason mapping.
fn agent_status_wire(status: AgentStatus) -> Option<&'static str> {
    match status {
        AgentStatus::Pending | AgentStatus::Waiting => Some(WAITING),
        AgentStatus::Active | AgentStatus::Processing => Some(RESPONDING),
        AgentStatus::RuntimeIdle | AgentStatus::Idle => Some(IDLE),
        AgentStatus::Completed => Some(COMPLETED),
        AgentStatus::Error => Some(FAILED),
        AgentStatus::Deleted => None,
    }
}

fn is_active(status: AgentStatus) -> bool {
    matches!(status, AgentStatus::Active)
}
";
    assert!(hit_lines(src).is_empty(), "{:?}", scan_source(src));
}

#[test]
fn comments_strings_and_test_code_are_skipped() {
    let src = r#"
//! Running means `AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing`.

/// See `AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing`.
fn doc(s: AgentStatus) -> &'static str {
    /* AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing */
    let msg = "AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing";
    let raw = r"AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing";
    if s.is_running_turn() { msg } else { raw }
}

#[cfg(test)]
fn in_test_fn(s: AgentStatus) -> bool {
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}

#[cfg(test)]
mod tests {
    fn helper(s: AgentStatus) -> bool {
        matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
    }
}

#[cfg(test)]
const RUNNING: fn(AgentStatus) -> bool =
    |s| matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);

#[cfg(not(test))]
fn scanned(s: AgentStatus) -> bool {
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}
"#;
    assert_eq!(hit_lines(src), vec![line_of(src, "fn scanned") + 1]);
}

#[test]
fn borrowed_and_parenthesized_alternatives_are_grouped() {
    let src = r"
fn by_ref(s: &AgentStatus) -> bool {
    matches!(s, &AgentStatus::Pending | &AgentStatus::Active | &AgentStatus::Processing)
}

fn by_ref_arm(s: &AgentStatus) -> bool {
    match s {
        &AgentStatus::Pending | &AgentStatus::Active | &AgentStatus::Processing => true,
        _ => false,
    }
}

fn by_mut_ref(s: &mut AgentStatus) -> bool {
    matches!(s, &mut AgentStatus::Pending | &mut AgentStatus::Active | &mut AgentStatus::Processing)
}

fn parenthesized(s: AgentStatus) -> bool {
    matches!(s, (AgentStatus::Pending) | (AgentStatus::Active) | (AgentStatus::Processing))
}

fn mixed(s: &AgentStatus) -> bool {
    matches!(s, (&AgentStatus::Pending) | &(AgentStatus::Active) | ((AgentStatus::Processing)))
}

fn whole_group_parenthesized(s: AgentStatus) -> bool {
    matches!(s, (AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing))
}

fn by_ref_superset(s: &AgentStatus) -> bool {
    matches!(
        s,
        &AgentStatus::Pending | &AgentStatus::Active | &AgentStatus::Processing | &AgentStatus::Waiting
    )
}

fn by_ref_subset(s: &AgentStatus) -> bool {
    matches!(s, &AgentStatus::Active | &AgentStatus::Processing)
}

fn nested_in_some(s: Option<AgentStatus>) -> bool {
    matches!(s, Some(AgentStatus::Pending) | Some(AgentStatus::Active) | Some(AgentStatus::Processing))
}
";
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "fn by_ref(") + 1,
            line_of(src, "fn by_ref_arm") + 2,
            line_of(src, "fn by_mut_ref") + 1,
            line_of(src, "fn parenthesized") + 1,
            line_of(src, "fn mixed") + 1,
            line_of(src, "fn whole_group_parenthesized") + 1,
        ]
    );
}

#[test]
fn reasoned_marker_suppresses_above_group_or_statement() {
    let src = r"
fn f(s: AgentStatus) -> bool {
    // running-turn: allow — fixture for the lint itself
    let a = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allow - the statement line is the `matches!(` line
    let b = matches!(
        s,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    );
    let c = matches!(
        s,
        // running-turn: allow — directly above the pattern
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    );
    let d = match s {
        // running-turn: allow — above the arm
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing => true,
        _ => false,
    };
    a || b || c || d
}
";
    assert!(hit_lines(src).is_empty(), "{:?}", scan_source(src));
}

#[test]
fn malformed_or_misplaced_markers_do_not_suppress() {
    let src = r"
fn f(s: AgentStatus) -> bool {
    // running-turn: allow
    let a = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allowance — longer token
    let b = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allow —
    let c = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    let d = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing); // running-turn: allow — trailing
    /* running-turn: allow — block comment */
    let e = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allow — two lines above

    let f = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    a || b || c || d || e || f
}
";
    let hits = scan_source(src);
    let expect: Vec<(usize, bool)> = ["let a", "let b", "let c", "let d", "let e", "let f"]
        .iter()
        .zip([true, true, true, false, false, false])
        .map(|(needle, malformed)| (line_of(src, needle), malformed))
        .collect();
    assert_eq!(
        hits.iter()
            .map(|h| (h.line, h.marker_malformed))
            .collect::<Vec<_>>(),
        expect
    );
}

// ---- SQL rule fixtures ------------------------------------------------------

fn sql_hit_lines(src: &str) -> Vec<usize> {
    scan_sql_literals(src).into_iter().map(|h| h.line).collect()
}

/// `delegated_counts_sql` in `crates/intent-store/src/agent_repo.rs` as it
/// stood before intentd commit e6703788 (#2058): the literal `IN` list.
const DELEGATED_COUNTS_SQL_PRE_2058: &str = r#"
/// grouped statement over the workspace's non-retired delegated rows (the
/// `delegated` predicate of [`scope_predicate`]) yielding `delegatedCounts`
/// (§5.5): one row per direct parent with the child count and the subset
/// whose persisted status is running. The running set is the daemon's
/// `is_running_turn` rule — `pending` / `active` / legacy capitalized
/// `Processing` (the serde names of `AgentStatus`, which is how the column is
/// written). Same `idx_agent_workspace` search as [`scope_counts_sql`], so
/// `SUM(total)` over the result always equals `scopeCounts.delegated`.
pub(crate) fn delegated_counts_sql() -> &'static str {
    "SELECT \
        parent_agent_id, \
        COUNT(*) AS total, \
        COALESCE(SUM(status IN ('pending', 'active', 'Processing')), 0) AS running \
     FROM agent_session \
     WHERE workspace_id = ? AND retired_at IS NULL AND parent_agent_id IS NOT NULL \
     GROUP BY parent_agent_id"
}
"#;

#[test]
fn pre_2058_delegated_counts_sql_literal_is_flagged() {
    let src = DELEGATED_COUNTS_SQL_PRE_2058;
    let hits = scan_sql_literals(src);
    assert_eq!(
        hits,
        vec![Hit {
            line: line_of(src, "COALESCE(SUM(status IN"),
            excerpt:
                "COALESCE(SUM(status IN ('pending', 'active', 'Processing')), 0) AS running \\"
                    .into(),
            marker_malformed: false,
        }]
    );
}

#[test]
fn sql_rule_sees_raw_strings_and_format_templates() {
    let src = r##"
fn a() -> String {
    format!(
        "SELECT COUNT(*) FROM agent_session \
         WHERE workspace_id = {ws} AND status IN ('pending', 'active', 'Processing')"
    )
}
fn b() -> &'static str {
    r#"SELECT 1 WHERE status IN ('active', 'Processing')"#
}
"##;
    assert_eq!(
        sql_hit_lines(src),
        vec![
            line_of(src, "WHERE workspace_id = {ws}"),
            line_of(src, "r#\"SELECT 1"),
        ]
    );
}

#[test]
fn sql_literals_with_one_fingerprint_word_are_not_hits() {
    let src = r#"
fn pr_monitor_queries() -> Vec<String> {
    vec![
        "UPDATE pr_monitor SET updated_at = ?1 WHERE monitor_id = ?2 AND state = 'active'".into(),
        format!("SELECT {COLUMNS} FROM pr_monitor WHERE state = 'active' ORDER BY created_at"),
        "SELECT 1 FROM pr_monitor WHERE state IN ('active', 'completed')".into(),
        "SELECT 1 FROM agent_session WHERE status = 'Processing'".into(),
        "SELECT 1 FROM agent_session WHERE status IN ('pending', 'Processing')".into(),
        "SELECT 1 FROM agent_session WHERE status IN ('Active', 'processing')".into(),
    ]
}
"#;
    assert!(
        sql_hit_lines(src).is_empty(),
        "{:?}",
        scan_sql_literals(src)
    );
}

#[test]
fn sql_literals_in_test_code_and_comments_are_not_hits() {
    let src = r#"
//! The store test pins `status IN ('pending', 'active', 'Processing')`.

/// Generated from `AgentStatus::ALL`; never spell `('pending', 'active', 'Processing')`.
pub(crate) fn delegated_counts_sql() -> String {
    /* status IN ('pending', 'active', 'Processing') */
    let running = running_status_list();
    format!("COALESCE(SUM(status IN ({running})), 0) AS running")
}

#[cfg(test)]
mod tests {
    #[test]
    fn delegated_counts_sql_running_set_matches_core_rule() {
        assert!(
            delegated_counts_sql().contains("status IN ('pending', 'active', 'Processing')"),
        );
    }
}

#[cfg(test)]
const EXPECTED: &str = "status IN ('pending', 'active', 'Processing')";

#[cfg(not(test))]
fn scanned() -> &'static str {
    "status IN ('pending', 'active', 'Processing')"
}
"#;
    assert_eq!(sql_hit_lines(src), vec![line_of(src, "fn scanned") + 1]);
}

#[test]
fn sql_rule_cfg_test_item_survives_semicolons_inside_brackets_and_parens() {
    let src = r#"
#[cfg(test)]
const EXPECTED: [&str; 1] = ["status IN ('pending', 'active', 'Processing')"];

fn after_const() -> &'static str {
    "status IN ('pending', 'active', 'Processing')"
}

#[cfg(test)]
fn fixture(_: [u8; 1]) -> &'static str {
    "status IN ('pending', 'active', 'Processing')"
}

fn after_fn() -> &'static str {
    "status IN ('pending', 'active', 'Processing')"
}
"#;
    assert_eq!(
        sql_hit_lines(src),
        vec![
            line_of(src, "fn after_const") + 1,
            line_of(src, "fn after_fn") + 1,
        ]
    );
}

#[test]
fn sql_rule_marker_above_literal_or_statement_suppresses_but_malformed_does_not() {
    let src = r#"
fn f() -> Vec<&'static str> {
    // running-turn: allow — fixture for the lint itself
    let a = "status IN ('pending', 'active', 'Processing')";
    // running-turn: allow - the statement line is the `let b` line
    let b = format!(
        "SELECT 1 WHERE \
         status IN ('pending', 'active', 'Processing')"
    );
    let c = format!(
        // running-turn: allow — directly above the literal
        "status IN ('pending', 'active', 'Processing')"
    );
    // running-turn: allow
    let d = "status IN ('pending', 'active', 'Processing')";
    let e = "status IN ('pending', 'active', 'Processing')"; // running-turn: allow — trailing
    // running-turn: allow — two lines above

    let f = "status IN ('pending', 'active', 'Processing')";
    vec![a, b, c, d, e, f]
}
"#;
    let hits = scan_sql_literals(src);
    let expect: Vec<(usize, bool)> = [("let d", true), ("let e", false), ("let f", false)]
        .iter()
        .map(|(needle, malformed)| (line_of(src, needle), *malformed))
        .collect();
    assert_eq!(
        hits.iter()
            .map(|h| (h.line, h.marker_malformed))
            .collect::<Vec<_>>(),
        expect
    );
}

#[test]
fn sql_rule_scope_is_intent_store_non_test_sources() {
    let root = workspace_root();
    let dir: PathBuf = SQL_LITERAL_DIR.iter().collect();
    let files = sql_literal_sources(&root);
    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        assert!(
            rel.starts_with(&dir),
            "{} is outside the SQL rule's scope",
            display_rel(rel)
        );
        assert!(
            rel.file_name().is_some_and(|n| n != "tests.rs")
                && !rel.components().any(|c| c.as_os_str() == "tests"),
            "{} is test code",
            display_rel(rel)
        );
    }
    assert!(
        files.iter().any(|f| f.ends_with("agent_repo.rs")),
        "agent_repo.rs (home of delegated_counts_sql) is not in scope"
    );
}
