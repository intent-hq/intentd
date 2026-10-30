//! Repo-slug fold lint.
//!
//! Forge owner/name slugs are case-insensitive, and every site that folded or
//! compared them by hand outside `intent_core::RepoRef` was a site where the
//! fold could be missed (intent-hq/intentd#1809 → #1815). This source-scanning
//! test fails, naming `file:line`, whenever slug identity is case-folded or
//! compared anywhere under `crates/*/src/**/*.rs` other than
//! `crates/intent-core/src/repo_ref.rs`.
//!
//! The pre-processing — blanking comments, string literals, and `#[cfg(test)]`
//! items, cutting the text into statements (a block expression used as an
//! operand — its `}` followed by `.`, `?`, or `else` — chains with the text
//! around it: `let repo = if c { a } else { b }.to_lowercase()` is one
//! statement while the block bodies stay their own), locating the opt-out
//! marker, and walking `crates/*/src/**/*.rs` minus `tests/` directories and
//! `tests.rs` files — is the shared `intentd_test_support::source_lint`
//! scaffolding; its module doc spells out those semantics. The rule itself is
//! deliberately small:
//!
//! - A statement is flagged when one of its fold calls (`to_lowercase`,
//!   `to_ascii_lowercase`, `eq_ignore_ascii_case`, `make_ascii_lowercase`)
//!   has a slug identifier among its operands: an identifier token with an
//!   underscore-delimited component equal to `owner`, `repo`, `repository`,
//!   or `slug`, or the bare token `name` when the same statement also carries
//!   an `owner` component.
//! - A fold's operands are its receiver chain and argument group, the
//!   receiver of the enclosing method call when the fold is passed as a value
//!   (`.map(str::to_lowercase)`) or runs on a closure parameter
//!   (`.map(|n| n.to_lowercase())`), and the left-hand side of an `=` whose
//!   whole right-hand side is the fold's chain (`let repo = x.to_lowercase()`).
//!   A slug identifier elsewhere in the statement does not count: a git
//!   handle named `repo` next to a fold of a commit id is not slug identity
//!   (intent-hq/intentd#2810). See `fold_operand_words` for the exact walk.
//! - Skipped: `crates/intent-core/src/repo_ref.rs`, on top of the test code
//!   the shared walker and `#[cfg(test)]` blanking leave out.
//! - Opt-out: `// repo-slug-fold: allow — <reason>` on the line immediately
//!   above the statement's first line, in the shared marker grammar (a
//!   standalone `//` line comment, the exact token, whitespace, an em dash or
//!   hyphen, and a nonempty reason). A malformed marker never suppresses the
//!   hit; the report says so.

use std::fs;
use std::path::{Component, Path, PathBuf};

use intentd_test_support::source_lint::{
    blank_cfg_test_items, crate_src_files, lex, markers_by_line, split_statements, workspace_root,
    Marker,
};

const FOLD_CALLS: &[&str] = &[
    "to_lowercase",
    "to_ascii_lowercase",
    "eq_ignore_ascii_case",
    "make_ascii_lowercase",
];
const SLUG_COMPONENTS: &[&str] = &["owner", "repo", "repository", "slug"];
const OPT_OUT_TAG: &str = "repo-slug-fold";
const EXEMPT_FILE: &[&str] = &["crates", "intent-core", "src", "repo_ref.rs"];
const EXCERPT_CHARS: usize = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    excerpt: String,
    /// The line above carried something that starts like the opt-out marker
    /// but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

/// One token of a blanked statement: a word (identifier, keyword, or number)
/// or a punctuation token (multi-character operators kept whole).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tok<'a> {
    Word(&'a str),
    Punct(&'a str),
}

/// Multi-character operators, longest first, so `==` never reads as an
/// assignment `=` and `::` never as two `:`.
const MULTI_PUNCT: &[&str] = &[
    "..=", "...", "<<=", ">>=", "::", "->", "=>", "==", "!=", "<=", ">=", "&&", "||", "+=", "-=",
    "*=", "/=", "%=", "^=", "|=", "&=", "<<", ">>", "..",
];
/// Punctuation the backward receiver walk steps over: method and field
/// access, paths, `?`, and the leftovers of type ascription / lifetimes /
/// attributes. Every other punctuation token ends the receiver.
const CHAIN_PUNCT: &[&str] = &[".", "::", "?", ":", "'", "#"];
/// Keywords that end the receiver walk (`for c in <receiver>`, `return x`).
const STOP_WORDS: &[&str] = &["in", "return", "break", "yield"];

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn tokens(text: &str) -> Vec<Tok<'_>> {
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(c) = rest.chars().next() {
        if c.is_whitespace() {
            rest = &rest[c.len_utf8()..];
            continue;
        }
        let word = is_word_char(c);
        let len = if word {
            rest.find(|ch: char| !is_word_char(ch))
                .unwrap_or(rest.len())
        } else {
            MULTI_PUNCT
                .iter()
                .find(|p| rest.starts_with(**p))
                .map_or(c.len_utf8(), |p| p.len())
        };
        let (tok, tail) = rest.split_at(len);
        out.push(if word {
            Tok::Word(tok)
        } else {
            Tok::Punct(tok)
        });
        rest = tail;
    }
    out
}

/// For every `(` / `[` / `)` / `]`, the index of its matching bracket; `None`
/// when unbalanced (a statement cut at a `{` can leave an opener unclosed).
fn partners(toks: &[Tok]) -> Vec<Option<usize>> {
    let mut out = vec![None; toks.len()];
    let mut stack: Vec<usize> = Vec::new();
    for (i, t) in toks.iter().enumerate() {
        match *t {
            Tok::Punct("(" | "[") => stack.push(i),
            Tok::Punct(close @ (")" | "]")) => {
                let want = if close == ")" { "(" } else { "[" };
                if let Some(&open) = stack.last() {
                    if toks[open] == Tok::Punct(want) {
                        stack.pop();
                        out[open] = Some(i);
                        out[i] = Some(open);
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Start of the expression chain that ends just before `end`: walks back
/// over words, chain punctuation, and balanced `(…)` / `[…]` groups until a
/// statement boundary, an unmatched opener, an operator, `,`, `|`, or a stop
/// keyword. Keywords left over from a chained operand block (`if c else`,
/// `match x`, `unsafe`) are stepped over like any other word.
fn chain_start(toks: &[Tok], partner: &[Option<usize>], end: usize) -> usize {
    let mut j = end;
    while j > 0 {
        match toks[j - 1] {
            Tok::Punct(")" | "]") => match partner[j - 1] {
                Some(open) => j = open,
                None => break,
            },
            Tok::Punct(p) if !CHAIN_PUNCT.contains(&p) => break,
            Tok::Word(w) if STOP_WORDS.contains(&w) => break,
            _ => j -= 1,
        }
    }
    j
}

/// Innermost `(` / `[` before `pos` whose group contains `pos`.
fn enclosing_open(toks: &[Tok], partner: &[Option<usize>], pos: usize) -> Option<usize> {
    let mut j = pos;
    while j > 0 {
        j -= 1;
        match toks[j] {
            Tok::Punct(")" | "]") => {
                if let Some(open) = partner[j] {
                    j = open;
                }
            }
            Tok::Punct("(" | "[") => return Some(j),
            _ => {}
        }
    }
    None
}

/// Index of the `.` when the `(` at `open` is a method call `.m(`.
fn method_dot(toks: &[Tok], open: usize) -> Option<usize> {
    (toks[open] == Tok::Punct("(")
        && open >= 2
        && matches!(toks[open - 1], Tok::Word(_))
        && toks[open - 2] == Tok::Punct("."))
    .then(|| open - 2)
}

/// Parameter words of the last closure argument (`|a, (b, c)|` right after
/// the `(`, a `,`, or `move`) that opens at the top level of the group at
/// `open` before `pos` — the closure whose body holds `pos`.
fn closure_params<'a>(
    toks: &[Tok<'a>],
    partner: &[Option<usize>],
    open: usize,
    pos: usize,
) -> Vec<&'a str> {
    let mut params = Vec::new();
    let mut k = open + 1;
    while k < pos {
        match toks[k] {
            Tok::Punct("(" | "[") => k = partner[k].map_or(pos, |c| c + 1),
            Tok::Punct("|")
                if k == open + 1 || matches!(toks[k - 1], Tok::Punct(",") | Tok::Word("move")) =>
            {
                let close = (k + 1..pos).find(|&m| toks[m] == Tok::Punct("|"));
                let Some(close) = close else { break };
                params = slug_candidates(&toks[k + 1..close]);
                k = close + 1;
            }
            _ => k += 1,
        }
    }
    params
}

/// Whether the tokens from `j` on are only a method/field chain and `?` — the
/// value of the expression ending at `j` is what the statement binds.
fn tail_is_chain(toks: &[Tok], partner: &[Option<usize>], mut j: usize) -> bool {
    while j < toks.len() {
        match toks[j] {
            Tok::Punct("?") => j += 1,
            Tok::Punct(".") => {
                if !matches!(toks.get(j + 1), Some(Tok::Word(_))) {
                    return false;
                }
                j += 2;
                if toks.get(j) == Some(&Tok::Punct("::")) {
                    match toks[j..].iter().position(|t| *t == Tok::Punct("(")) {
                        Some(k) => j += k,
                        None => return false,
                    }
                }
                if toks.get(j) == Some(&Tok::Punct("(")) {
                    match partner[j] {
                        Some(close) => j = close + 1,
                        None => return false,
                    }
                }
            }
            _ => return false,
        }
    }
    true
}

/// Identifier words (not numbers or punctuation) among `toks`.
fn slug_candidates<'a>(toks: &[Tok<'a>]) -> Vec<&'a str> {
    toks.iter()
        .filter_map(|t| match *t {
            Tok::Word(w) if w.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_') => Some(w),
            _ => None,
        })
        .collect()
}

/// Identifier words of the operands of the fold call at token `i`:
///
/// 1. its receiver chain (`<recv>.fold(…)`) and its own argument group
///    (`o.eq_ignore_ascii_case(owner)`, UFCS `str::to_lowercase(&owner)`);
/// 2. for a fold used as a value (`.map(str::to_lowercase)`), the receiver of
///    the enclosing method call (or the enclosing call's arguments);
/// 3. when an operand is a parameter of a closure passed to an enclosing
///    `.m(|n| …)`, the receiver of `.m` — repeated outward;
/// 4. when the fold's chain (as widened by 2/3, never across an enclosing
///    group that was not widened) is the whole right-hand side of an `=`
///    (only `.method()` / `?` follow), the assignment's left-hand side.
fn fold_operand_words<'a>(toks: &[Tok<'a>], partner: &[Option<usize>], i: usize) -> Vec<&'a str> {
    let mut words = Vec::new();
    let call_close = match toks.get(i + 1) {
        Some(Tok::Punct("(")) => partner[i + 1],
        _ => None,
    };
    if let Some(close) = call_close {
        words.extend(slug_candidates(&toks[i + 2..close]));
    }
    let (mut start, mut end) = if i > 0 && toks[i - 1] == Tok::Punct(".") {
        let s = chain_start(toks, partner, i - 1);
        words.extend(slug_candidates(&toks[s..i - 1]));
        (s, call_close.map_or(i + 1, |c| c + 1))
    } else if let Some(close) = call_close {
        (chain_start(toks, partner, i), close + 1)
    } else if let Some(open) = enclosing_open(toks, partner, i) {
        let inner_end = partner[open].unwrap_or(toks.len());
        let end = partner[open].map_or(toks.len(), |c| c + 1);
        if let Some(dot) = method_dot(toks, open) {
            let s = chain_start(toks, partner, dot);
            words.extend(slug_candidates(&toks[s..dot]));
            (s, end)
        } else {
            words.extend(slug_candidates(&toks[open + 1..inner_end]));
            (chain_start(toks, partner, open), end)
        }
    } else {
        (chain_start(toks, partner, i), i + 1)
    };

    let mut direct = true;
    let mut pos = start;
    while let Some(open) = enclosing_open(toks, partner, pos) {
        pos = open;
        let widened = method_dot(toks, open).filter(|_| {
            closure_params(toks, partner, open, start)
                .iter()
                .any(|p| words.contains(p))
        });
        match widened {
            Some(dot) => {
                let s = chain_start(toks, partner, dot);
                words.extend(slug_candidates(&toks[s..dot]));
                start = s;
                end = partner[open].map_or(toks.len(), |c| c + 1);
                pos = s;
            }
            None => direct = false,
        }
    }

    if direct
        && start > 0
        && toks[start - 1] == Tok::Punct("=")
        && tail_is_chain(toks, partner, end)
    {
        let eq = start - 1;
        let lhs = chain_start(toks, partner, eq);
        words.extend(slug_candidates(&toks[lhs..eq]));
    }
    words
}

fn has_component(ident: &str, component: &str) -> bool {
    ident.split('_').any(|part| part == component)
}

/// Whether one statement folds a slug identifier: some fold call has a slug
/// identifier among its operands ([`fold_operand_words`]). A slug identifier
/// elsewhere in the statement does not count.
fn is_flagged(statement: &str) -> bool {
    let toks = tokens(statement);
    let partner = partners(&toks);
    let has_owner = slug_candidates(&toks)
        .iter()
        .any(|id| has_component(id, "owner"));
    toks.iter()
        .enumerate()
        .filter(|(_, t)| matches!(t, Tok::Word(w) if FOLD_CALLS.contains(w)))
        .any(|(i, _)| {
            fold_operand_words(&toks, &partner, i).iter().any(|id| {
                SLUG_COMPONENTS.iter().any(|c| has_component(id, c)) || (*id == "name" && has_owner)
            })
        })
}

fn excerpt(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(EXCERPT_CHARS).collect();
    if out.len() < collapsed.len() {
        out.push('…');
    }
    out
}

/// Scans one Rust source file's text and returns every flagged statement
/// that is not suppressed by a reasoned opt-out marker.
fn scan_source(src: &str) -> Vec<Hit> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let blanked = blank_cfg_test_items(&lexed.blanked);
    split_statements(&blanked)
        .into_iter()
        .filter(|s| is_flagged(&s.text))
        .filter_map(|s| {
            let marker = markers.get(s.line - 1).copied().unwrap_or(Marker::Absent);
            match marker {
                Marker::WithReason => None,
                Marker::Malformed | Marker::Absent => Some(Hit {
                    line: s.line,
                    excerpt: excerpt(&s.text),
                    marker_malformed: marker == Marker::Malformed,
                }),
            }
        })
        .collect()
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
fn slug_identity_is_only_folded_inside_repo_ref() {
    let root = workspace_root();
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(
        root.join(&exempt).is_file(),
        "{} moved; update EXEMPT_FILE so the exemption keeps pointing at RepoRef",
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
                "  (opt-out marker is malformed: expected `// repo-slug-fold: allow — <reason>`)"
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
        "owner/name slug identity is case-folded or compared outside intent_core::RepoRef:\n\n{}\n\n\
         Compare slugs as `RepoRef` values (its PartialEq/Hash fold ASCII case) or take the \
         folded form from `RepoRef::identity_parts()` / `identity_key()`. A site that is not \
         slug identity (e.g. a folder-name slugifier) may opt out with \
         `// repo-slug-fold: allow — <reason>` on the line immediately above the statement; \
         the reason is required.",
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

/// `lookup_known_repo_local_path` as it stood before intentd commit 31dc6cc7
/// (`crates/intent-acp/src/mcp_server/bindings/app/workspaces.rs`).
const LOOKUP_KNOWN_REPO_LOCAL_PATH_PRE_31DC6CC7: &str = r#"
async fn lookup_known_repo_local_path(
    api: &Arc<dyn WorkspaceApi>,
    github_url: &str,
) -> Option<String> {
    let (owner, repo) = parse_github_owner_repo(github_url)?;
    let workspaces = api.list_workspaces(intent_core::WorkspaceListFilter::WITH_ARCHIVED).await.ok()?;

    let mut strict = Vec::new();
    let mut name_only = Vec::new();
    let mut basename_only = Vec::new();
    for ws in &workspaces {
        // Skip deleted workspaces: their repositoryPath may no longer exist
        // on disk (the FE lookup consults user-visible checkouts only).
        if ws.status == WorkspaceStatus::Deleted {
            continue;
        }
        let Some(path) = ws.repository_path.as_deref().filter(|p| !p.is_empty()) else {
            continue;
        };
        if path.contains("/.clones/") || path.contains("\\.clones\\") {
            continue;
        }
        let entry_owner = ws
            .repository_owner
            .as_deref()
            .filter(|o| !o.is_empty())
            .map(str::to_lowercase);
        let entry_name = ws
            .repository_name
            .as_deref()
            .filter(|n| !n.is_empty())
            .map(|n| strip_git_suffix(&n.to_lowercase()).to_string());
        let entry_basename = path
            .rsplit(['/', '\\'])
            .next()
            .map(|b| strip_git_suffix(&b.to_lowercase()).to_string());

        if entry_name.as_deref() == Some(repo.as_str()) {
            if entry_owner.as_deref() == Some(owner.as_str()) {
                strict.push(path.to_string());
            } else if entry_owner.is_none() {
                name_only.push(path.to_string());
            }
        } else if entry_basename.as_deref() == Some(repo.as_str()) && entry_owner.is_none() {
            basename_only.push(path.to_string());
        }
    }
    None
}
"#;

/// `origin_is_github_slot` as it stood on `main` before intentd#1815
/// (`crates/intent-git/src/repo_cache.rs`).
const ORIGIN_IS_GITHUB_SLOT_PRE_1815: &str = r#"
fn origin_is_github_slot(url: &str, owner: &str, repo: &str) -> bool {
    let trimmed = url.trim().trim_end_matches('/');
    let (rest, scp_like) = match trimmed.split_once("://") {
        Some((scheme, rest)) => {
            let known = ["https", "http", "ssh", "git"]
                .iter()
                .any(|s| scheme.eq_ignore_ascii_case(s));
            if !known {
                return false;
            }
            (rest, false)
        }
        // No scheme: only the scp-like `user@host:owner/repo` form qualifies.
        None => (trimmed, true),
    };
    let rest = rest.rsplit_once('@').map_or(rest, |(_, r)| r);
    let (authority, path) = if scp_like {
        match rest.split_once(':') {
            Some(pair) => pair,
            None => return false,
        }
    } else {
        match rest.split_once('/') {
            Some(pair) => pair,
            None => return false,
        }
    };
    let host = authority.split(':').next().unwrap_or(authority);
    if !host.eq_ignore_ascii_case("github.com") {
        return false;
    }
    let mut segments = path.split('/').filter(|s| !s.is_empty());
    let (Some(o), Some(r)) = (segments.next(), segments.next()) else {
        return false;
    };
    if segments.next().is_some() {
        return false;
    }
    let r = r.strip_suffix(".git").unwrap_or(r);
    o.eq_ignore_ascii_case(owner) && r.eq_ignore_ascii_case(repo)
}
"#;

/// `parse_github_owner_repo` as it stood on `main` before intentd#1815
/// (`crates/intent-acp/src/mcp_server/bindings/app/workspaces.rs`).
const PARSE_GITHUB_OWNER_REPO_PRE_1815: &str = r#"
/// Parse a GitHub URL (https, www, or git@ form) into lowercase
/// `(owner, repo)` (TS `parseGithubOwnerRepo`).
fn parse_github_owner_repo(github_url: &str) -> Option<(String, String)> {
    let t = github_url.trim();
    let stripped = strip_prefix_ci(t, "https://www.github.com/")
        .or_else(|| strip_prefix_ci(t, "http://www.github.com/"))
        .or_else(|| strip_prefix_ci(t, "https://github.com/"))
        .or_else(|| strip_prefix_ci(t, "http://github.com/"))
        .or_else(|| strip_prefix_ci(t, "git@github.com:"))
        .unwrap_or(t);
    let stripped = strip_git_suffix(stripped);
    let mut segs = stripped.split('/').filter(|s| !s.is_empty());
    let owner = segs.next()?;
    let repo = segs.next()?;
    Some((owner.to_lowercase(), repo.to_lowercase()))
}
"#;

/// `head_reached_by` as it stood before intentd commit 8ce3c3dd
/// (`crates/intent-git/src/local_changes.rs`, intent-hq/intentd#2810): the
/// fold is on a commit id, and `repo` is a git handle in another operand.
const HEAD_REACHED_BY_PRE_8CE3C3DD: &str = r"
pub fn head_reached_by(worktree_path: &Path, heads: &[String]) -> Result<bool> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let Some(head) = repo
        .head()
        .ok()
        .and_then(|h| h.peel_to_commit().ok())
        .map(|c| c.id())
    else {
        return Ok(false);
    };
    for candidate in heads {
        let Ok(oid) = Oid::from_str(candidate.trim()) else {
            continue;
        };
        // `Oid::from_str` zero-pads a short prefix; only an exact id of a
        // commit present locally can vouch for `HEAD`.
        if oid.to_string() != candidate.trim().to_ascii_lowercase()
            || repo.find_commit(oid).is_err()
        {
            continue;
        }
        if oid == head || repo.graph_descendant_of(oid, head).map_err(map_git_err)? {
            return Ok(true);
        }
    }
    Ok(false)
}
";

#[test]
fn flags_pre_31dc6cc7_lookup_known_repo_local_path() {
    let src = LOOKUP_KNOWN_REPO_LOCAL_PATH_PRE_31DC6CC7;
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "let entry_owner = ws"),
            line_of(src, "let entry_name = ws"),
        ]
    );
    let hits = scan_source(src);
    assert!(hits[0].excerpt.contains("repository_owner"), "{hits:?}");
    assert!(hits.iter().all(|h| !h.marker_malformed), "{hits:?}");
}

#[test]
fn flags_pre_1815_origin_is_github_slot_but_not_its_scheme_and_host_checks() {
    let src = ORIGIN_IS_GITHUB_SLOT_PRE_1815;
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "o.eq_ignore_ascii_case(owner)")]
    );
}

#[test]
fn flags_pre_1815_parse_github_owner_repo() {
    let src = PARSE_GITHUB_OWNER_REPO_PRE_1815;
    assert_eq!(
        hit_lines(src),
        vec![line_of(
            src,
            "Some((owner.to_lowercase(), repo.to_lowercase()))"
        )]
    );
}

#[test]
fn ignores_folds_that_are_not_slug_identity() {
    let src = r#"
fn not_identity(host: &str, scheme: &str, name: &str, specialist_name: &str) -> bool {
    if !host.eq_ignore_ascii_case("github.com") {
        return false;
    }
    let known = ["https", "http"].iter().any(|s| scheme.eq_ignore_ascii_case(s));
    let auth = name.eq_ignore_ascii_case("authorization");
    let implementor = specialist_name.eq_ignore_ascii_case("implementor");
    known && auth && implementor
}

fn ordered(left: &str, right: &str) -> std::cmp::Ordering {
    left.to_lowercase().cmp(&right.to_lowercase())
}

fn header_name(name: &str) -> String {
    name.to_ascii_lowercase()
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn bare_name_counts_only_next_to_an_owner() {
    let src = r"
fn key(owner: &str, name: &str) -> (String, String) {
    (owner.to_lowercase(), name.to_lowercase())
}

fn lone(name: &str) -> String {
    name.to_lowercase()
}
";
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "(owner.to_lowercase(), name")]
    );
}

#[test]
fn slug_identity_hides_in_strings_and_comments_is_ignored() {
    let src = r#"
fn describe(url: &str) -> String {
    // owner and repo are folded elsewhere: see repo.to_lowercase()
    /* owner.to_lowercase() */
    let label = "owner.to_lowercase()";
    format!("{label}: {}", url.to_lowercase())
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn c_string_literals_are_stripped_like_other_strings() {
    // Raw C string: a fold inside its body is not a hit.
    let raw_false_positive =
        "fn f(owner: &str) {\n    let _ = cr#\"\" owner.to_lowercase() \"\"#;\n}\n";
    assert_eq!(hit_lines(raw_false_positive), Vec::<usize>::new());

    // Raw C string ending in a quote: quote scanning must stay in sync so the
    // live fold after it is still seen.
    let raw_hidden_fold =
        "fn f(owner: &str) {\n    let _ = cr#\"ends with a quote \"\"#;\n    owner.to_lowercase();\n}\n";
    assert_eq!(hit_lines(raw_hidden_fold), vec![3]);

    // Unhashed raw C string, and cooked C / byte strings.
    let cooked = "fn f(owner: &str) {\n    let _ = cr\"owner.to_lowercase()\";\n    let _ = c\"owner.to_lowercase() \\\" \";\n    let _ = b\"owner.to_lowercase()\";\n    owner.to_lowercase();\n}\n";
    assert_eq!(hit_lines(cooked), vec![5]);
}

#[test]
fn opt_out_marker_with_a_reason_suppresses_the_statement() {
    let src = r"
pub(crate) fn worktree_folder_slug(repo_name: &str) -> String {
    let mut slug = String::new();
    // repo-slug-fold: allow — folder-name slugifier, not repo identity
    for c in repo_name.chars().flat_map(char::to_lowercase) {
        slug.push(c);
    }
    slug
}
";
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn opt_out_marker_without_a_reason_still_fails() {
    for marker in [
        "// repo-slug-fold: allow",
        "// repo-slug-fold: allow —",
        "// repo-slug-fold: allow -",
        "// repo-slug-fold: allow —   ",
        "// repo-slug-fold: allow reason without a dash",
        "// repo-slug-fold: allow—no space before the dash",
    ] {
        let src = format!(
            "fn slugify(repo_name: &str) -> String {{\n    {marker}\n    repo_name.to_lowercase()\n}}\n"
        );
        let hits = scan_source(&src);
        assert_eq!(hits.len(), 1, "{marker:?}: {hits:?}");
        assert_eq!(hits[0].line, 3, "{marker:?}");
        assert!(hits[0].marker_malformed, "{marker:?}");
    }
}

#[test]
fn opt_out_marker_must_sit_immediately_above_the_statement() {
    let src = "fn slugify(repo_name: &str) -> String {\n    // repo-slug-fold: allow — reason\n\n    repo_name.to_lowercase()\n}\n";
    let hits = scan_source(src);
    assert_eq!(hit_lines(src), vec![4]);
    assert!(!hits[0].marker_malformed);
}

#[test]
fn cfg_test_items_are_skipped_but_code_after_them_is_not() {
    let src = r"
#[cfg(test)]
mod tests {
    fn folds(owner: &str, repo: &str) -> bool {
        owner.to_lowercase() == repo.to_lowercase()
    }
}

#[cfg(test)]
mod more_tests;

#[cfg(test)]
fn helper(owner: &str) -> String {
    owner.to_ascii_lowercase()
}

fn real(owner: &str) -> String {
    owner.to_lowercase()
}
";
    assert_eq!(hit_lines(src), vec![line_of(src, "fn real(owner") + 1]);
}

#[test]
fn operand_blocks_chain_with_the_surrounding_statement() {
    let src = r"
fn f(c: bool, a: &str, b: &str, owner_ref: Kind, x: &str) {
    let repo = if c { a } else { b }.to_lowercase();
    let n = match owner_ref {
        Kind::A => x,
        Kind::B => a,
    }.to_lowercase();
    let s = unsafe { raw(x) }.eq_ignore_ascii_case(slug);
    let t = { Some(x) }?.to_lowercase();
}
";
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "let repo = if c"),
            line_of(src, "let n = match owner_ref"),
            line_of(src, "let s = unsafe"),
        ]
    );

    // A fold inside the block body is still its own statement.
    let src = r"
fn f(c: bool, owner: &str, b: &str) -> String {
    let h = if c { owner.to_lowercase() } else { b.to_string() }.trim().to_string();
    h
}
";
    assert_eq!(hit_lines(src), vec![line_of(src, "let h = if c")]);

    // No slug identifier anywhere: the chained statement is not flagged.
    let src = r"
fn f(c: bool, x: &str, y: &str) -> String {
    let h = if c { x } else { y }.to_lowercase();
    h
}
";
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn only_repo_ref_is_exempt() {
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(is_exempt(&exempt));
    let sibling: PathBuf = ["crates", "intent-core", "src", "slug.rs"].iter().collect();
    assert!(!is_exempt(&sibling));
    let elsewhere: PathBuf = ["crates", "intent-git", "src", "repo_ref.rs"]
        .iter()
        .collect();
    assert!(!is_exempt(&elsewhere));
    assert_eq!(display_rel(&exempt), "crates/intent-core/src/repo_ref.rs");
}

#[test]
fn ignores_pre_8ce3c3dd_head_reached_by_commit_id_fold() {
    assert_eq!(hit_lines(HEAD_REACHED_BY_PRE_8CE3C3DD), Vec::<usize>::new());
}

/// Hit count of `statement` scanned as the only statement of a function body.
fn statement_hits(statement: &str) -> usize {
    scan_source(&format!("fn f() {{\n    {statement};\n}}\n")).len()
}

#[test]
fn folds_of_a_slug_operand_are_flagged() {
    for statement in [
        "repo.to_lowercase()",
        "owner.to_lowercase()",
        "self.repo_slug.make_ascii_lowercase()",
        "o.eq_ignore_ascii_case(owner) || repo.find_commit(x).is_err()",
        "repo.find_commit(x).is_err() || o.eq_ignore_ascii_case(&owner)",
        "let k = str::to_lowercase(&owner)",
        "let owner = segs.next()?.to_lowercase()",
        "let owner: String = segs.next()?.to_lowercase().trim().to_string()",
        "let v: Vec<String> = repo_slugs.iter().map(|r| r.to_lowercase()).collect()",
        "let v = repo_names.iter().map(str::to_lowercase).collect::<Vec<_>>()",
        "let v = repo_list.iter().flat_map(|g| g.iter().map(|r| r.to_lowercase())).count()",
        "let v = repo_names.iter().map_or(0, |r| r.to_lowercase().len())",
    ] {
        assert_eq!(statement_hits(statement), 1, "{statement:?}");
    }
}

#[test]
fn folds_of_other_values_next_to_a_slug_identifier_pass() {
    for statement in [
        "repo.find_commit(oid.to_string().to_lowercase())",
        "repo.find_branch(&branch.to_lowercase(), BranchType::Local)",
        "let repo = Repository::open(path.to_lowercase())?",
        "let ok = repo.is_bare() && kind.eq_ignore_ascii_case(\"bare\")",
        "let lowered = names.iter().map(|n| n.to_lowercase()).collect::<Vec<_>>()",
        "let repo_dir = base.join(tag.to_lowercase())",
        "if oid.to_string() != candidate.trim().to_ascii_lowercase() || repo.find_commit(oid).is_err() {}",
    ] {
        assert_eq!(statement_hits(statement), 0, "{statement:?}");
    }
}
